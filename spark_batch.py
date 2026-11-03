"""
spark_batch.py -- Part 2, batch job 2: recommendation indicators + cohort analysis.

  A) "Users who bought X also bought Y"       (co-purchase, from transactions)
  B) "Products frequently viewed together"    (co-view, from sessions)
     metrics per product pair: pair_count, support, confidence, lift
  C) Cohort analysis: users grouped by registration month; buyers & revenue by months since registration

Run:  python spark_batch.py
Out:  results/spark_copurchase.csv, spark_coview.csv, spark_cohort.csv
"""
import time
from pyspark.sql import functions as F
from spark_common import get_spark, load_raw, clean_all, register_views, save_csv

t0 = time.time()
spark = get_spark("batch-recommendations-cohorts")
clean = clean_all(spark, load_raw(spark))
register_views(clean)
names = clean["products"].select("product_id", "name")


def pair_metrics(baskets, total, min_count, names):
    """baskets: DataFrame(basket_id, product_id), distinct. Returns pair metrics (association rules)."""
    counts = baskets.groupBy("product_id").agg(F.count("*").alias("n"))
    a = baskets.select(F.col("basket_id").alias("bid_a"), F.col("product_id").alias("prod_a"))
    b = baskets.select(F.col("basket_id").alias("bid_b"), F.col("product_id").alias("prod_b"))
    pairs = (a.join(b, (F.col("bid_a") == F.col("bid_b")) & (F.col("prod_a") < F.col("prod_b")))
             .groupBy("prod_a", "prod_b").agg(F.count("*").alias("pair_count"))
             .filter(F.col("pair_count") >= min_count))
    ca = counts.select(F.col("product_id").alias("prod_a"), F.col("n").alias("n_a"))
    cb = counts.select(F.col("product_id").alias("prod_b"), F.col("n").alias("n_b"))
    na = names.select(F.col("product_id").alias("prod_a"), F.col("name").alias("name_a"))
    nb = names.select(F.col("product_id").alias("prod_b"), F.col("name").alias("name_b"))
    tot = F.lit(float(total))
    return (pairs.join(ca, "prod_a").join(cb, "prod_b")
            .join(F.broadcast(na), "prod_a").join(F.broadcast(nb), "prod_b")
            .withColumn("support", F.round(F.col("pair_count") / tot, 5))
            .withColumn("confidence_a_to_b", F.round(F.col("pair_count") / F.col("n_a"), 3))
            .withColumn("confidence_b_to_a", F.round(F.col("pair_count") / F.col("n_b"), 3))
            .withColumn("lift", F.round(F.col("pair_count") * tot / (F.col("n_a") * F.col("n_b")), 2))
            .select("prod_a", "name_a", "prod_b", "name_b", "pair_count", "support",
                    "confidence_a_to_b", "confidence_b_to_a", "lift"))

# ---------------- A) co-purchase ----------------
n_txn = clean["transactions"].count()
buy = clean["items"].select(F.col("transaction_id").alias("basket_id"), "product_id").distinct()
copurchase = (pair_metrics(buy, n_txn, 2, names)
              .orderBy(F.desc("pair_count"), F.desc("lift")).limit(25))
print(f"\n=== A) Products bought together (basket = transaction, {n_txn:,} transactions) ===")
copurchase.select("name_a", "name_b", "pair_count", "confidence_a_to_b", "lift").show(10, truncate=40)
save_csv(copurchase, "spark_copurchase.csv")

# ---------------- B) co-view ----------------
sess = clean["sessions"].filter(F.col("n_products_viewed").between(2, 30))
n_sess = sess.count()
view = sess.select(F.col("session_id").alias("basket_id"),
                   F.explode(F.array_distinct("viewed_products")).alias("product_id"))
coview = (pair_metrics(view, n_sess, 3, names)
          .orderBy(F.desc("pair_count"), F.desc("lift")).limit(25))
print(f"=== B) Products viewed together (basket = session, {n_sess:,} sessions with 2-30 views) ===")
coview.select("name_a", "name_b", "pair_count", "confidence_a_to_b", "lift").show(10, truncate=40)
save_csv(coview, "spark_coview.csv")

# ---------------- C) cohort analysis (Spark SQL) ----------------
COHORT_SQL = """
WITH cohort AS (
    SELECT user_id, date_trunc('month', registration_date) AS cohort_month
    FROM users
),
sizes AS (
    SELECT cohort_month, COUNT(*) AS cohort_size FROM cohort GROUP BY cohort_month
),
activity AS (
    SELECT c.cohort_month,
           CAST(months_between(date_trunc('month', t.txn_ts), c.cohort_month) AS INT) AS month_offset,
           t.user_id, t.total
    FROM transactions t JOIN cohort c ON t.user_id = c.user_id
)
SELECT date_format(a.cohort_month, 'yyyy-MM') AS cohort,
       a.month_offset,
       s.cohort_size,
       COUNT(DISTINCT a.user_id)                            AS active_buyers,
       ROUND(COUNT(DISTINCT a.user_id) / s.cohort_size, 3)  AS buyer_rate,
       ROUND(SUM(a.total), 2)                               AS revenue,
       ROUND(SUM(a.total) / s.cohort_size, 2)               AS revenue_per_user
FROM activity a JOIN sizes s ON a.cohort_month = s.cohort_month
WHERE a.month_offset >= 0
GROUP BY a.cohort_month, a.month_offset, s.cohort_size
ORDER BY a.cohort_month, a.month_offset
"""
cohort = spark.sql(COHORT_SQL)
print("=== C) Cohort analysis: registration-month cohorts ===")
cohort.show(30, truncate=False)
save_csv(cohort, "spark_cohort.csv")

print(f"\nDone in {time.time()-t0:.0f}s")
spark.stop()
