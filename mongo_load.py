"""
mongo_load.py -- Load the generated e-commerce dataset into MongoDB.

Design decisions (explain these in the report):
  * products     : category + subcategory EMBEDDED (one read gets the full product).
  * transactions : line items EMBEDDED; each item is enriched with category_id and
                   profit_margin so revenue/profit by category needs no $lookup.
  * users        : purchase_summary + engagement EMBEDDED (pre-computed at load time).
  * sessions     : page_views EMBEDDED; cart_contents converted from a {product_id: ...}
                   dict into an ARRAY (dynamic keys can't be indexed/queried cleanly).
  * All ISO date strings are converted to real BSON dates.

Usage:  python mongo_load.py [--data-dir data] [--uri mongodb://localhost:27017] [--db ecommerce]
"""
import argparse, glob, json, os, time
from collections import defaultdict
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING

ap = argparse.ArgumentParser()
ap.add_argument("--data-dir", default="data")
ap.add_argument("--uri", default="mongodb://localhost:27017")
ap.add_argument("--db", default="ecommerce")
ap.add_argument("--batch", type=int, default=5000)
args = ap.parse_args()

def dt(s):
    """ISO string -> datetime (None-safe)."""
    return datetime.fromisoformat(s) if s else None

def load_json(name):
    with open(os.path.join(args.data_dir, name), "r") as f:
        return json.load(f)

def insert_batches(coll, docs_iter, batch=args.batch):
    """Insert documents in batches to keep memory low."""
    buf, total = [], 0
    for d in docs_iter:
        buf.append(d)
        if len(buf) >= batch:
            coll.insert_many(buf, ordered=False); total += len(buf); buf = []
    if buf:
        coll.insert_many(buf, ordered=False); total += len(buf)
    return total

client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
client.admin.command("ping")                      # fail fast if server is down
db = client[args.db]
for name in ("categories", "products", "users", "transactions", "sessions"):
    db[name].drop()                               # idempotent re-runs
t0 = time.time()

# ---------------- categories (reference collection) ----------------
categories = load_json("categories.json")
cat_name = {c["category_id"]: c["name"] for c in categories}
sub_info = {s["subcategory_id"]: s for c in categories for s in c["subcategories"]}
db.categories.insert_many([{"_id": c["category_id"], **c} for c in categories])
print(f"categories   : {len(categories)}")

# ---------------- products (category/subcategory embedded) ----------------
products = load_json("products.json")
prod_cat, prod_margin = {}, {}
def product_docs():
    for p in products:
        sub = sub_info.get(p.get("subcategory_id"), {})
        prod_cat[p["product_id"]] = (p["category_id"], p.get("subcategory_id"))
        prod_margin[p["product_id"]] = sub.get("profit_margin", 0.0)
        yield {
            "_id": p["product_id"], "name": p["name"],
            "category": {"category_id": p["category_id"], "name": cat_name.get(p["category_id"])},
            "subcategory": {"subcategory_id": p.get("subcategory_id"),
                            "name": sub.get("name"), "profit_margin": sub.get("profit_margin")},
            "base_price": p["base_price"], "current_stock": p["current_stock"],
            "is_active": p["is_active"], "creation_date": dt(p["creation_date"]),
            "price_history": [{"price": h["price"], "date": dt(h["date"])} for h in p["price_history"]],
        }
n = insert_batches(db.products, product_docs())
print(f"products     : {n}")

# ---------------- sessions (streamed; also accumulate per-user engagement) ----------------
eng = defaultdict(lambda: {"session_count": 0, "total_session_seconds": 0, "converted_sessions": 0})
def session_docs(s_list):
    for s in s_list:
        e = eng[s["user_id"]]
        e["session_count"] += 1
        e["total_session_seconds"] += s["duration_seconds"]
        e["converted_sessions"] += s["conversion_status"] == "converted"
        yield {
            "_id": s["session_id"], "user_id": s["user_id"],
            "start_time": dt(s["start_time"]), "end_time": dt(s["end_time"]),
            "duration_seconds": s["duration_seconds"], "geo_data": s["geo_data"],
            "device_profile": s["device_profile"], "viewed_products": s["viewed_products"],
            "page_views": [{**pv, "timestamp": dt(pv["timestamp"])} for pv in s["page_views"]],
            "page_view_count": len(s["page_views"]),
            "cart_items": [{"product_id": k, "quantity": v["quantity"], "price": v["price"]}
                           for k, v in s["cart_contents"].items()],
            "conversion_status": s["conversion_status"], "referrer": s["referrer"],
        }
n = 0
for path in sorted(glob.glob(os.path.join(args.data_dir, "sessions_*.json"))):
    with open(path) as f:
        n += insert_batches(db.sessions, session_docs(json.load(f)))   # one file in memory at a time
    print(f"  loaded {os.path.basename(path)}")
print(f"sessions     : {n}")

# ---------------- transactions (items embedded + enriched) ----------------
txns = load_json("transactions.json")
summ = defaultdict(lambda: {"total_orders": 0, "total_spent": 0.0, "first": None, "last": None,
                            "cat_spend": defaultdict(float)})
def txn_docs():
    for t in txns:
        ts = dt(t["timestamp"])
        items = []
        for it in t["items"]:
            cid, sid = prod_cat.get(it["product_id"], (None, None))
            items.append({**it, "category_id": cid, "subcategory_id": sid,
                          "profit_margin": prod_margin.get(it["product_id"], 0.0)})
            summ[t["user_id"]]["cat_spend"][cid] += it["subtotal"]
        u = summ[t["user_id"]]
        u["total_orders"] += 1; u["total_spent"] += t["total"]
        u["first"] = ts if u["first"] is None or ts < u["first"] else u["first"]
        u["last"] = ts if u["last"] is None or ts > u["last"] else u["last"]
        yield {"_id": t["transaction_id"], "session_id": t["session_id"], "user_id": t["user_id"],
               "timestamp": ts, "items": items, "item_count": len(items),
               "subtotal": t["subtotal"], "discount": t["discount"], "total": t["total"],
               "payment_method": t["payment_method"], "status": t["status"]}
n = insert_batches(db.transactions, txn_docs())
print(f"transactions : {n}")

# ---------------- users (purchase_summary + engagement embedded) ----------------
users = load_json("users.json")
def user_docs():
    for u in users:
        s, e = summ.get(u["user_id"]), eng.get(u["user_id"])
        ps = {"total_orders": 0, "total_spent": 0.0, "avg_order_value": 0.0,
              "first_purchase": None, "last_purchase": None, "top_category_id": None}
        if s:
            top = max(s["cat_spend"].items(), key=lambda kv: kv[1])[0]
            ps = {"total_orders": s["total_orders"], "total_spent": round(s["total_spent"], 2),
                  "avg_order_value": round(s["total_spent"] / s["total_orders"], 2),
                  "first_purchase": s["first"], "last_purchase": s["last"], "top_category_id": top}
        en = {"session_count": 0, "total_session_seconds": 0, "avg_session_seconds": 0.0,
              "converted_sessions": 0}
        if e:
            en = {**e, "avg_session_seconds": round(e["total_session_seconds"] / e["session_count"], 1)}
        yield {"_id": u["user_id"], "geo_data": u["geo_data"],
               "registration_date": dt(u["registration_date"]), "last_active": dt(u["last_active"]),
               "purchase_summary": ps, "engagement": en}
n = insert_batches(db.users, user_docs())
print(f"users        : {n}")

# ---------------- indexes (match the query patterns) ----------------
db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
db.transactions.create_index("timestamp")
db.transactions.create_index("items.product_id")
db.sessions.create_index([("user_id", ASCENDING), ("start_time", DESCENDING)])
db.sessions.create_index("viewed_products")
db.products.create_index("category.category_id")
db.users.create_index("purchase_summary.total_orders")
print("indexes created")
print(f"\nDone in {time.time()-t0:.1f}s -> database '{args.db}'")
