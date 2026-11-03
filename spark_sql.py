"""
spark_sql.py -- Part 2: Spark SQL analytics on the cleaned DataFrames (registered as views).

The views stand in for data that could notionally live in MongoDB (users, products, transactions)
and HBase (sessions). Q5 joins "MongoDB-side" and "HBase-side" data in one SQL statement; Part 3
replaces these views with data really read from MongoDB and HBase.

Run:  python spark_sql.py
Out:  results/sql_q1..q6.csv
"""
import time
from spark_common import get_spark, load_raw, clean_all, register_views, save_csv

t0 = time.time()
spark = get_spark("spark-sql-analytics")
register_views(clean_all(spark, load_raw(spark)))

QUERIES = {}

# Q1: window function -- top 3 categories by revenue in each month
QUERIES["sql_q1_top_categories_per_month.csv"] = """
WITH m AS (
    SELECT date_format(i.txn_ts, 'yyyy-MM') AS month, p.category_name,
           ROUND(SUM(i.line_total), 2) AS revenue
    FROM items i JOIN products p ON i.product_id = p.product_id
    GROUP BY date_format(i.txn_ts, 'yyyy-MM'), p.category_name
),
ranked AS (
    SELECT month, category_name, revenue,
           RANK() OVER (PARTITION BY month ORDER BY revenue DESC) AS rnk
    FROM m
)
SELECT month, rnk, category_name, revenue FROM ranked WHERE rnk <= 3 ORDER BY month, rnk
"""

# Q2: NTILE -- customer value quartiles
QUERIES["sql_q2_customer_quartiles.csv"] = """
WITH spend AS (
    SELECT u.user_id, COALESCE(SUM(t.total), 0) AS total_spent, COUNT(t.transaction_id) AS orders
    FROM users u LEFT JOIN transactions t ON u.user_id = t.user_id
    GROUP BY u.user_id
),
q AS (SELECT *, NTILE(4) OVER (ORDER BY total_spent DESC) AS spend_quartile FROM spend)
SELECT spend_quartile, COUNT(*) AS users, ROUND(AVG(total_spent), 2) AS avg_spent,
       ROUND(AVG(orders), 1) AS avg_orders,
       ROUND(MIN(total_spent), 2) AS min_spent, ROUND(MAX(total_spent), 2) AS max_spent
FROM q GROUP BY spend_quartile ORDER BY spend_quartile
"""

# Q3/Q4: conversion funnel by device and by referrer
FUNNEL = """
SELECT {dim} AS {dim}, COUNT(*) AS sessions,
       SUM(CASE WHEN n_cart_items > 0 THEN 1 ELSE 0 END) AS with_cart,
       SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS converted,
       ROUND(100 * SUM(CASE WHEN n_cart_items > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS cart_rate_pct,
       ROUND(100 * SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) / COUNT(*), 1) AS conversion_rate_pct,
       ROUND(AVG(duration_seconds), 0) AS avg_duration_s
FROM sessions GROUP BY {dim} ORDER BY sessions DESC
"""
QUERIES["sql_q3_funnel_by_device.csv"] = FUNNEL.format(dim="device_type")
QUERIES["sql_q4_funnel_by_referrer.csv"] = FUNNEL.format(dim="referrer")

# Q5: moving average window (7 days)
QUERIES["sql_q5_daily_revenue_7d_avg.csv"] = """
WITH daily AS (
    SELECT txn_date AS day, COUNT(*) AS orders, SUM(total) AS revenue
    FROM transactions GROUP BY txn_date
)
SELECT day, orders, ROUND(revenue, 2) AS revenue,
       ROUND(AVG(revenue) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_7d_avg
FROM daily ORDER BY day
"""

# Q6: cross-source join -- profile (MongoDB-like) + sessions (HBase-like) + transactions (MongoDB-like)
QUERIES["sql_q6_user360_cross_source.csv"] = """
SELECT u.user_id, u.state, s.n_sessions, ROUND(s.avg_session_s, 0) AS avg_session_s,
       COALESCE(t.orders, 0) AS orders, ROUND(COALESCE(t.revenue, 0), 2) AS revenue,
       ROUND(COALESCE(t.revenue, 0) / s.n_sessions, 2) AS revenue_per_session
FROM users u
JOIN (SELECT user_id, COUNT(*) AS n_sessions, AVG(duration_seconds) AS avg_session_s
      FROM sessions GROUP BY user_id) s ON u.user_id = s.user_id
LEFT JOIN (SELECT user_id, COUNT(*) AS orders, SUM(total) AS revenue
           FROM transactions GROUP BY user_id) t ON u.user_id = t.user_id
ORDER BY revenue DESC LIMIT 20
"""

for fname, sql in QUERIES.items():
    print(f"\n=== {fname} ===")
    df = spark.sql(sql)
    df.show(10, truncate=40)
    save_csv(df, fname)

print(f"\nDone in {time.time()-t0:.0f}s")
spark.stop()
