# Distributed Multi-Model Analytics for E-commerce Data

AUCA Big Data Analytics – Final Project  ·Names: **MUTONI DENYSE** ID Number: 101207 · November 2026

A multi-system analytics pipeline for a synthetic e-commerce dataset (5,000 users, 2,000 products,
100,000 sessions, 25,000 transactions, 90 days). It uses **MongoDB** (document model), **HBase**
(wide-column model) and **Apache Spark** (distributed processing), and combines them in one
integrated analysis (Customer Lifetime Value).

## Architecture

```
dataset_generator.py ──► data/*.json
                           │
        ┌──────────────────┼───────────────────────┐
        ▼                  ▼                       ▼
   MongoDB            HBase (Thrift)           Spark (local[2])
   users, products,   user_sessions,           cleaning, recommendations,
   transactions,      product_metrics          cohorts, Spark SQL
   sessions
        │                  │                       │
        └──── extract_*.py ┴──► data_staging/ ──► integrated_clv.py (Spark join)
                                                   │
                                          results/*.csv ──► visualize.py ──► figures/*.png
```

| System | Used for | Why |
|---|---|---|
| MongoDB | Product catalog (category embedded), transactions (items embedded), user profiles (embedded purchase summary), sessions | Nested documents, aggregation pipelines, no joins for common reads |
| HBase | Time-series sessions (`user_id#reverse_timestamp`) and product metrics (`product_id#yyyymmdd`) | Sorted row keys = fast "latest sessions of a user" and time-range scans; sparse wide rows |
| Spark | Cleaning, data-quality report, co-purchase / co-view indicators, cohort analysis, Spark SQL, cross-source join | Distributed batch processing and SQL over all sessions |

## Environment used
Windows 11 · 8 GB RAM · Python 3.14 · Java 17 (Temurin) · PySpark 4.3 · MongoDB 8.0 (Windows service) ·
HBase 2.6.6 standalone inside WSL2 (Ubuntu) with the Thrift server on port 9090.

## Files

| Part | File | Purpose |
|---|---|---|
| Data | `dataset_generator.py` | Generates the dataset (`--sessions`, `--transactions` options) |
| 1 – MongoDB | `mongo_load.py` | Loads data with the embedded document design + indexes |
| | `mongo_queries.py` | 5 aggregation pipelines (top products, revenue/profit by category, daily revenue, user segments, most viewed) |
| 1 – HBase | `hbase_shell_commands.txt` | Table creation + shell queries |
| | `hbase_load.py` | Loads sessions (row key `user_id#reverse_ts`) and daily product metrics |
| | `hbase_queries.py` | Last-N sessions, time-range scan, full session, product funnel, row counts |
| 2 – Spark | `spark_common.py` | Spark session, explicit schemas, JSON→JSON Lines, cleaning functions |
| | `spark_etl.py` | Cleaning + data-quality report |
| | `spark_batch.py` | Co-purchase / co-view indicators, cohort analysis |
| | `spark_sql.py` | 6 Spark SQL queries (window functions, NTILE, funnels, moving average, cross-source join) |
| 3 – Integration | `extract_mongo.py`, `extract_hbase.py` | Export the needed data from each store |
| | `integrated_clv.py` | Spark join of MongoDB + HBase data → Customer Lifetime Value tiers |
| 4 – Visuals | `visualize.py` | 7 charts from `results/*.csv` |
| Output | `results/`, `figures/`, `screenshots/` | Result tables, charts, screenshots |
| Report | `report/Technical_Report_MUTONI_DENYSE.pdf` | Technical report (7 pages) |

## How to reproduce

```powershell
python -m venv venv
venv\Scripts\activate
pip install faker numpy pandas pymongo happybase pyspark matplotlib

# 1. data
python dataset_generator.py                      # creates data/

# 2. MongoDB (service running on localhost:27017)
python mongo_load.py
python mongo_queries.py

# 3. HBase (WSL2: start-hbase.sh, then hbase-daemon.sh start thrift; create tables from hbase_shell_commands.txt)
python hbase_load.py --host <WSL address from `hostname -I`>
python hbase_queries.py --host <WSL address>

# 4. Spark (stop HBase first on an 8 GB machine; needs JAVA_HOME = Java 17)
python spark_etl.py
python spark_batch.py
python spark_sql.py

# 5. Integration: export, then join in Spark
python extract_mongo.py
python extract_hbase.py --host <WSL address>
python integrated_clv.py

# 6. Charts
python visualize.py
```

## Key findings
* The top 25 % of customers generate about 43 % of revenue; predicted 12-month profit per Platinum customer is about 4.7× a Bronze customer.
* The `converted` label and the transactions disagree: 20.4 % of sessions are labelled converted, but only 12.7 % match a real transaction. The gap is probably caused by the generator's 25,000-order cap, so true conversion lies between 12.7 % and 20.4 %.
* 98 % of users had a `last_active` date earlier than their real activity; Spark recomputed it from sessions and transactions.
* Browsing volume is only weakly correlated with customer value (r ≈ 0.14).

## Limitations
* The data is synthetic and randomly generated: device, referrer and product-pair effects are weak by construction.
* No age/demographic field exists, so segmentation uses purchase frequency and spend instead of age groups.
* HBase holds a 30,000-session sample; daily product metrics use all sessions.
* Spark connectors for MongoDB/HBase were not used; data is exported with the native Python clients and joined in Spark (reliable on an 8 GB machine, but not a live connector).
* CLV assumes each customer keeps their current order rate (no churn model) over a 91-day observation window.
