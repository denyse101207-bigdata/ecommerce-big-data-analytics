"""
mongo_queries.py -- Aggregation pipelines for Part 1 (MongoDB).
Results are printed AND saved to results/*.csv (used later for the visualizations).

Usage:  python mongo_queries.py [--uri mongodb://localhost:27017] [--db ecommerce]
"""
import argparse, csv, os
from pymongo import MongoClient

ap = argparse.ArgumentParser()
ap.add_argument("--uri", default="mongodb://localhost:27017")
ap.add_argument("--db", default="ecommerce")
args = ap.parse_args()
db = MongoClient(args.uri)[args.db]
os.makedirs("results", exist_ok=True)

def show(title, rows, fname):
    print(f"\n=== {title} ===")
    for r in rows[:10]:
        print(r)
    if rows:
        with open(os.path.join("results", fname), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
        print(f"(saved results/{fname}, {len(rows)} rows)")

# 1) PRODUCT POPULARITY: top-selling products by revenue (unwind embedded items)
p1 = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.product_id", "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"}, "orders": {"$sum": 1}}},
    {"$sort": {"revenue": -1}}, {"$limit": 20},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$project": {"_id": 0, "product_id": "$_id", "name": "$p.name",
                  "category": "$p.category.name", "units_sold": 1, "orders": 1,
                  "revenue": {"$round": ["$revenue", 2]}}},
]
show("Top 20 products by revenue", list(db.transactions.aggregate(p1)), "top_products_revenue.csv")

# 2) REVENUE ANALYTICS: revenue + estimated profit by category (margin embedded in items)
p2 = [
    {"$unwind": "$items"},
    {"$group": {"_id": "$items.category_id", "revenue": {"$sum": "$items.subtotal"},
                "est_profit": {"$sum": {"$multiply": ["$items.subtotal", "$items.profit_margin"]}},
                "units_sold": {"$sum": "$items.quantity"}}},
    {"$lookup": {"from": "categories", "localField": "_id", "foreignField": "_id", "as": "c"}},
    {"$unwind": "$c"},
    {"$project": {"_id": 0, "category_id": "$_id", "category": "$c.name", "units_sold": 1,
                  "revenue": {"$round": ["$revenue", 2]}, "est_profit": {"$round": ["$est_profit", 2]},
                  "margin_pct": {"$round": [{"$multiply": [{"$divide": ["$est_profit", "$revenue"]}, 100]}, 1]}}},
    {"$sort": {"revenue": -1}},
]
show("Revenue & estimated profit by category", list(db.transactions.aggregate(p2)), "revenue_by_category.csv")

# 3) REVENUE OVER TIME: daily revenue and order count
p3 = [
    {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                "revenue": {"$sum": "$total"}, "orders": {"$sum": 1}}},
    {"$sort": {"_id": 1}},
    {"$project": {"_id": 0, "date": "$_id", "orders": 1, "revenue": {"$round": ["$revenue", 2]}}},
]
show("Daily revenue", list(db.transactions.aggregate(p3)), "daily_revenue.csv")

# 4) USER SEGMENTATION by purchasing frequency (uses embedded purchase_summary, no join)
p4 = [
    {"$bucket": {"groupBy": "$purchase_summary.total_orders",
                 "boundaries": [0, 1, 2, 4, 7, 1000000], "default": "other",
                 "output": {"users": {"$sum": 1},
                            "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                            "avg_sessions": {"$avg": "$engagement.session_count"},
                            "total_revenue": {"$sum": "$purchase_summary.total_spent"}}}},
]
labels = {0: "0 orders (browsers)", 1: "1 order (one-time)", 2: "2-3 orders (occasional)",
          4: "4-6 orders (regular)", 7: "7+ orders (loyal)"}
rows = [{"segment": labels.get(r["_id"], str(r["_id"])), "users": r["users"],
         "avg_spent": round(r["avg_spent"], 2), "avg_sessions": round(r["avg_sessions"], 1),
         "total_revenue": round(r["total_revenue"], 2)} for r in db.users.aggregate(p4)]
show("User segments by purchase frequency", rows, "user_segments.csv")

# 5) PRODUCT POPULARITY: most viewed products (unwind viewed_products in sessions)
p5 = [
    {"$unwind": "$viewed_products"},
    {"$group": {"_id": "$viewed_products", "views": {"$sum": 1}}},
    {"$sort": {"views": -1}}, {"$limit": 20},
    {"$lookup": {"from": "products", "localField": "_id", "foreignField": "_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$project": {"_id": 0, "product_id": "$_id", "name": "$p.name", "views": 1}},
]
show("Top 20 most-viewed products", list(db.sessions.aggregate(p5, allowDiskUse=True)), "top_products_views.csv")