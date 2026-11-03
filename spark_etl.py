"""
spark_etl.py -- Part 2, batch job 1: clean & normalize the raw dataset + data-quality report.

Run:  python spark_etl.py
Out:  results/spark_data_quality.csv  (+ printed schemas and samples for the report)
"""
import time
from pyspark.sql import functions as F
from spark_common import get_spark, load_raw, clean_all, save_csv

t0 = time.time()
spark = get_spark("etl-clean-normalize")
raw = load_raw(spark)
clean = clean_all(spark, raw)

print("\n=== Raw sessions schema (explicit; note cart_contents is a MAP) ===")
raw["sessions"].printSchema()
print("=== Cleaned users: sample ===")
clean["users"].select("user_id", "city", "state", "registration_date", "last_active_raw",
                      "last_active", "n_sessions").show(5, truncate=False)

# ---------------- data-quality report ----------------
rows = []
def add(check, value):
    rows.append((check, int(value)))
    print(f"{check:<70} {int(value):>10,}")

print("\n=== Data-quality report ===")
for k in ("users", "products", "sessions", "transactions"):
    r, c = raw[k].count(), clean[k].count()
    add(f"{k}: raw rows", r); add(f"{k}: rows after cleaning", c); add(f"{k}: rows removed", r - c)
add("transaction line items (exploded)", clean["items"].count())

s, t, u, p, items = (clean[k] for k in ("sessions", "transactions", "users", "products", "items"))

add("RULE: transactions where total != subtotal - discount",
    t.filter(~F.col("total_consistent")).count())
add("RULE: product_detail page views missing product_id",
    s.select(F.explode("page_views").alias("pv")).filter(
        (F.col("pv.page_type") == "product_detail") & F.col("pv.product_id").isNull()).count())
add("INTEGRITY: sessions whose user_id is not in users",
    s.join(u.select("user_id"), "user_id", "left_anti").count())
add("INTEGRITY: transactions whose user_id is not in users",
    t.join(u.select("user_id"), "user_id", "left_anti").count())
add("INTEGRITY: transaction items whose product_id is not in products",
    items.join(p.select("product_id"), "product_id", "left_anti").count())
add("INTEGRITY: transactions with a session_id not found in sessions",
    t.filter(F.col("session_id").isNotNull())
     .join(s.select("session_id"), "session_id", "left_anti").count())
add("INFO: transactions with no session (session_id is null)",
    t.filter(F.col("session_id").isNull()).count())
add("INFO: sessions marked 'converted' with no transaction",
    s.filter(F.col("conversion_status") == "converted")
     .join(t.select("session_id"), "session_id", "left_anti").count())
add("FIXED: users whose last_active was earlier than real activity",
    u.filter(F.col("last_active") > F.col("last_active_raw")).count())
add("INFO: products inactive", p.filter(~F.col("is_active")).count())
add("INFO: products with zero stock", p.filter(F.col("current_stock") == 0).count())

from pyspark.sql import Row
save_csv(spark.createDataFrame([Row(check=a, value=b) for a, b in rows]), "spark_data_quality.csv")
print(f"\nDone in {time.time()-t0:.0f}s")
spark.stop()
