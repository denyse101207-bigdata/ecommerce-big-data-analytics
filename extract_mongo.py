"""
extract_mongo.py -- Part 3, step 1: export the MongoDB data needed for the CLV analysis.
  users        -> data_staging/mongo_users.jsonl        (profile + embedded purchase_summary)
  transactions -> data_staging/mongo_transactions.jsonl (total + estimated profit from the margins
                                                         embedded in each order item)
Run:  python extract_mongo.py
"""
import argparse, json, os
from pymongo import MongoClient

ap = argparse.ArgumentParser()
ap.add_argument("--uri", default="mongodb://localhost:27017")
ap.add_argument("--db", default="ecommerce")
args = ap.parse_args()

db = MongoClient(args.uri, serverSelectionTimeoutMS=5000)[args.db]
os.makedirs("data_staging", exist_ok=True)

n = 0
with open("data_staging/mongo_users.jsonl", "w") as f:
    for u in db.users.find({}, {"registration_date": 1, "geo_data.state": 1,
                                "geo_data.country": 1, "purchase_summary": 1}):
        ps = u["purchase_summary"]
        f.write(json.dumps({
            "user_id": u["_id"],
            "registration_date": u["registration_date"].isoformat(),
            "state": u["geo_data"]["state"], "country": u["geo_data"]["country"],
            "mongo_total_orders": ps["total_orders"], "mongo_total_spent": ps["total_spent"]}) + "\n")
        n += 1
print(f"users exported        : {n:,}")

n = 0
with open("data_staging/mongo_transactions.jsonl", "w") as f:
    for t in db.transactions.find({}, {"user_id": 1, "timestamp": 1, "total": 1, "status": 1,
                                       "session_id": 1, "items": 1}):
        est_profit = sum(it["subtotal"] * it.get("profit_margin", 0.0) for it in t["items"])
        f.write(json.dumps({
            "transaction_id": t["_id"], "user_id": t["user_id"],
            "txn_ts": t["timestamp"].isoformat(), "total": t["total"],
            "est_profit": round(est_profit, 2), "session_id": t["session_id"],
            "status": t["status"]}) + "\n")
        n += 1
print(f"transactions exported : {n:,}")
