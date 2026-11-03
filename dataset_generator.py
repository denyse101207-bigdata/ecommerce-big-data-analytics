# dataset_generator.py  -- runnable, low-memory version (8 GB RAM friendly)
# Usage:  python dataset_generator.py                (defaults below)
#         python dataset_generator.py --sessions 50000 --transactions 12000
import argparse, json, os, random, datetime, uuid
import numpy as np
from faker import Faker

# ---------------- Configuration ----------------
ap = argparse.ArgumentParser()
ap.add_argument("--users", type=int, default=5000)
ap.add_argument("--products", type=int, default=2000)
ap.add_argument("--categories", type=int, default=25)
ap.add_argument("--sessions", type=int, default=100000)
ap.add_argument("--transactions", type=int, default=25000)
ap.add_argument("--days", type=int, default=90)
ap.add_argument("--out", type=str, default="data")
args = ap.parse_args()

NUM_USERS, NUM_PRODUCTS, NUM_CATEGORIES = args.users, args.products, args.categories
NUM_SESSIONS, NUM_TRANSACTIONS, TIMESPAN_DAYS = args.sessions, args.transactions, args.days
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2
CHUNK_SIZE = 20000                       # sessions per file (flushed to disk)
os.makedirs(args.out, exist_ok=True)

fake = Faker()
np.random.seed(42); random.seed(42); Faker.seed(42)
print("Initializing dataset generation...")

def generate_session_id():     return f"sess_{uuid.uuid4().hex[:10]}"
def generate_transaction_id(): return f"txn_{uuid.uuid4().hex[:12]}"

class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
    def update_stock(self, pid, qty):
        p = self.products.get(pid)
        if p and p["current_stock"] >= qty:
            p["current_stock"] -= qty
            return True
        return False
    def get_product(self, pid):
        return self.products.get(pid)

TRANSITIONS = {
    "home": (["category_listing", "search", "product_detail"], [0.5, 0.3, 0.2]),
    "category_listing": (["product_detail", "category_listing", "search", "home"], [0.7, 0.1, 0.1, 0.1]),
    "search": (["product_detail", "search", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
    "product_detail": (["product_detail", "cart", "category_listing", "search", "home"], [0.3, 0.3, 0.2, 0.1, 0.1]),
    "cart": (["checkout", "product_detail", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
    "checkout": (["confirmation", "cart", "home"], [0.8, 0.1, 0.1]),
    "confirmation": (["home", "product_detail", "category_listing"], [0.6, 0.2, 0.2]),
}
def determine_page_type(position, previous_pages):
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    if not previous_pages:
        return "home"
    opts, w = TRANSITIONS.get(previous_pages[-1]["page_type"], (["home"], [1]))
    return random.choices(opts, weights=w)[0]

def get_page_content(page_type, products_list, cat_by_id):
    if page_type == "product_detail":
        for _ in range(10):
            p = random.choice(products_list)
            if p["is_active"] and p["current_stock"] > 0:
                return p, cat_by_id.get(p["category_id"])
        p = random.choice(products_list)
        return p, cat_by_id.get(p["category_id"])
    if page_type == "category_listing":
        return None, random.choice(list(cat_by_id.values()))
    return None, None

# ---------------- Categories ----------------
categories = []
for cat_id in range(NUM_CATEGORIES):
    cat = {"category_id": f"cat_{cat_id:03d}", "name": fake.company(), "subcategories": []}
    for sub_id in range(random.randint(3, 5)):
        cat["subcategories"].append({
            "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
            "name": fake.bs(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2)})
    categories.append(cat)
cat_by_id = {c["category_id"]: c for c in categories}
cat_list = list(cat_by_id.values())
print(f"Generated {len(categories)} categories")

# ---------------- Products ----------------
products = []
start0 = datetime.datetime.now() - datetime.timedelta(days=TIMESPAN_DAYS * 2)
for i in range(NUM_PRODUCTS):
    cat = random.choice(cat_list)
    sub = random.choice(cat["subcategories"])          # added: matches the project PDF sample
    base_price = round(random.uniform(5, 500), 2)
    initial = fake.date_time_between(start_date=start0, end_date=start0 + datetime.timedelta(days=TIMESPAN_DAYS // 3))
    ph = [{"price": base_price, "date": initial.isoformat()}]
    for _ in range(random.randint(0, 2)):
        d = fake.date_time_between(start_date=initial, end_date="now")
        ph.append({"price": round(base_price * random.uniform(0.8, 1.2), 2), "date": d.isoformat()})
        initial = d
    ph.sort(key=lambda x: x["date"])
    products.append({
        "product_id": f"prod_{i:05d}", "name": fake.catch_phrase().title(),
        "category_id": cat["category_id"], "subcategory_id": sub["subcategory_id"],
        "base_price": ph[-1]["price"], "current_stock": random.randint(10, 1000),
        "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
        "price_history": ph, "creation_date": ph[0]["date"]})
print(f"Generated {len(products)} products")

# ---------------- Users ----------------
users = []
for i in range(NUM_USERS):
    reg = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS*3}d", end_date=f"-{TIMESPAN_DAYS}d")
    users.append({
        "user_id": f"user_{i:06d}",
        "geo_data": {"city": fake.city(), "state": fake.state_abbr(), "country": fake.country_code()},
        "registration_date": reg.isoformat(),
        "last_active": fake.date_time_between(start_date=reg, end_date="now").isoformat()})
print(f"Generated {len(users)} users")

# ---------------- Sessions & Transactions ----------------
inventory = InventoryManager(products)
transactions, buffer = [], []
session_counter = transaction_counter = file_index = iteration = 0

def flush_sessions():
    global buffer, file_index
    if buffer:
        with open(os.path.join(args.out, f"sessions_{file_index}.json"), "w") as f:
            json.dump(buffer, f)
        file_index += 1
        buffer = []

print("Generating sessions and transactions...")
while (session_counter < NUM_SESSIONS or transaction_counter < NUM_TRANSACTIONS) and iteration < MAX_ITERATIONS:
    iteration += 1
    if session_counter < NUM_SESSIONS:
        user = random.choice(users)
        sid = generate_session_id()
        s_start = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now")
        dur = random.randint(30, 3600)
        page_views, viewed, cart = [], set(), {}
        slots = sorted([0] + [random.randint(1, dur - 1) for _ in range(random.randint(3, 15))] + [dur])
        for i in range(len(slots) - 1):
            vd = slots[i + 1] - slots[i]
            ptype = determine_page_type(i, page_views)
            prod, cat = get_page_content(ptype, products, cat_by_id)
            if ptype == "product_detail" and prod:
                pid = prod["product_id"]; viewed.add(pid)
                if random.random() < 0.3:
                    cart.setdefault(pid, {"quantity": 0, "price": prod["base_price"]})
                    room = min(3, inventory.get_product(pid)["current_stock"] - cart[pid]["quantity"])
                    if room > 0:
                        cart[pid]["quantity"] += random.randint(1, room)
            page_views.append({
                "timestamp": (s_start + datetime.timedelta(seconds=slots[i])).isoformat(),
                "page_type": ptype,
                "product_id": prod["product_id"] if prod else None,
                "category_id": cat["category_id"] if cat else None,
                "view_duration": vd})
        converted = False
        if cart and any(p["page_type"] in ("checkout", "confirmation") for p in page_views):
            converted = random.random() < 0.7
        geo = user["geo_data"].copy(); geo["ip_address"] = fake.ipv4()
        clean_cart = {k: v for k, v in cart.items() if v["quantity"] > 0}
        buffer.append({
            "session_id": sid, "user_id": user["user_id"],
            "start_time": s_start.isoformat(),
            "end_time": (s_start + datetime.timedelta(seconds=dur)).isoformat(),
            "duration_seconds": dur, "geo_data": geo,
            "device_profile": {"type": random.choice(["mobile", "desktop", "tablet"]),
                               "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                               "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"])},
            "viewed_products": list(viewed), "page_views": page_views,
            "cart_contents": clean_cart,
            "conversion_status": "converted" if converted else "abandoned" if cart else "browsed",
            "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"])})
        session_counter += 1
        if len(buffer) >= CHUNK_SIZE:
            flush_sessions()

        if converted and transaction_counter < NUM_TRANSACTIONS:
            items, valid = [], True
            for pid, d in cart.items():
                q = d["quantity"]
                if q > 0:
                    if inventory.update_stock(pid, q):
                        items.append({"product_id": pid, "quantity": q, "unit_price": d["price"],
                                      "subtotal": round(q * d["price"], 2)})
                    else:
                        valid = False; break
            if valid and items:
                sub = sum(x["subtotal"] for x in items)
                disc = round(sub * random.choice([0.05, 0.1, 0.15, 0.2]), 2) if random.random() < 0.2 else 0
                transactions.append({
                    "transaction_id": generate_transaction_id(), "session_id": sid,
                    "user_id": user["user_id"],
                    "timestamp": (s_start + datetime.timedelta(seconds=dur)).isoformat(),
                    "items": items, "subtotal": sub, "discount": disc, "total": round(sub - disc, 2),
                    "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "crypto"]),
                    "status": "completed"})
                transaction_counter += 1

    if transaction_counter < NUM_TRANSACTIONS and random.random() < 0.2:
        user = random.choice(users)
        items = []
        for p in random.sample(products, k=min(3, len(products))):
            if p["is_active"]:
                q = random.randint(1, 3)
                if inventory.update_stock(p["product_id"], q):
                    items.append({"product_id": p["product_id"], "quantity": q,
                                  "unit_price": p["base_price"], "subtotal": round(q * p["base_price"], 2)})
        if items:
            sub = sum(x["subtotal"] for x in items)
            disc = round(sub * random.choice([0.05, 0.1, 0.15, 0.2]), 2) if random.random() < 0.2 else 0
            transactions.append({
                "transaction_id": generate_transaction_id(), "session_id": None,
                "user_id": user["user_id"],
                "timestamp": fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now").isoformat(),
                "items": items, "subtotal": sub, "discount": disc, "total": round(sub - disc, 2),
                "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "gift_card"]),
                "status": random.choice(["completed", "processing", "shipped", "delivered"])})
            transaction_counter += 1

    if iteration % 10000 == 0:
        print(f"Progress: {session_counter:,}/{NUM_SESSIONS:,} sessions, "
              f"{transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions")

flush_sessions()

# ---------------- Export ----------------
print("Saving datasets...")
def dump(name, obj):
    with open(os.path.join(args.out, name), "w") as f:
        json.dump(obj, f)
dump("users.json", users)
dump("products.json", list(inventory.products.values()))
dump("categories.json", categories)
dump("transactions.json", transactions)
print(f"\nDone! Output folder: {args.out}/\n- Sessions: {session_counter:,} in {file_index} files\n"
      f"- Transactions: {transaction_counter:,}\n- Users: {len(users):,}\n- Products: {len(products):,}")
