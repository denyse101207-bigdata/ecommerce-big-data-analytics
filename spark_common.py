"""
spark_common.py -- shared helpers for all Spark jobs (session, schemas, loading, cleaning).

Pipeline:  data/*.json  --(ensure_jsonl)-->  data_jsonl/*.jsonl  --(load_raw, explicit schemas)-->
           raw DataFrames  --(clean_all)-->  clean DataFrames  --> analytics jobs

Optimization choices (for the report):
  * JSON Lines instead of one big JSON array  -> files become splittable, lower memory per task
  * Explicit schemas (no inference pass; cart_contents as MAP, otherwise Spark would create
    one column per product_id)
  * Broadcast joins for small dimension tables (products, categories)
  * local[2] + small shuffle partition count (8) because the machine has only 8 GB RAM
  * Only built-in Spark functions (no Python UDFs) -> stays in the JVM, avoids Python-worker overhead
"""
import csv, json, os
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

RAW, JSONL, RESULTS = Path("data"), Path("data_jsonl"), Path("results")


def get_spark(app="ecommerce-analytics"):
    mem = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    spark = (SparkSession.builder.master("local[2]").appName(app)
             .config("spark.driver.memory", mem)
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "UTC")     # same as MongoDB (UTC)
             .config("spark.sql.ansi.enabled", "false")       # lenient: bad values -> NULL, not crash
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def ensure_jsonl():
    """Convert each JSON-array file to JSON Lines (one record per line). Skips up-to-date files."""
    JSONL.mkdir(exist_ok=True)
    for src in sorted(RAW.glob("*.json")):
        dst = JSONL / (src.stem + ".jsonl")
        if dst.exists() and dst.stat().st_mtime >= src.stat().st_mtime:
            continue
        with open(src, "r") as f:
            data = json.load(f)
        with open(dst, "w") as out:
            for rec in data:
                out.write(json.dumps(rec) + "\n")
        print(f"converted {src.name} -> {dst.name} ({len(data):,} records)")


# ---------------- explicit schemas ----------------
S = T.StringType()
GEO = T.StructType([T.StructField(n, S) for n in ("city", "state", "country")])
SCHEMAS = {
    "users": T.StructType([
        T.StructField("user_id", S), T.StructField("geo_data", GEO),
        T.StructField("registration_date", S), T.StructField("last_active", S)]),
    "categories": T.StructType([
        T.StructField("category_id", S), T.StructField("name", S),
        T.StructField("subcategories", T.ArrayType(T.StructType([
            T.StructField("subcategory_id", S), T.StructField("name", S),
            T.StructField("profit_margin", T.DoubleType())])))]),
    "products": T.StructType([
        T.StructField("product_id", S), T.StructField("name", S),
        T.StructField("category_id", S), T.StructField("subcategory_id", S),
        T.StructField("base_price", T.DoubleType()), T.StructField("current_stock", T.LongType()),
        T.StructField("is_active", T.BooleanType()),
        T.StructField("price_history", T.ArrayType(T.StructType([
            T.StructField("price", T.DoubleType()), T.StructField("date", S)]))),
        T.StructField("creation_date", S)]),
    "sessions": T.StructType([
        T.StructField("session_id", S), T.StructField("user_id", S),
        T.StructField("start_time", S), T.StructField("end_time", S),
        T.StructField("duration_seconds", T.LongType()),
        T.StructField("geo_data", T.StructType([T.StructField(n, S) for n in ("city", "state", "country", "ip_address")])),
        T.StructField("device_profile", T.StructType([T.StructField(n, S) for n in ("type", "os", "browser")])),
        T.StructField("viewed_products", T.ArrayType(S)),
        T.StructField("page_views", T.ArrayType(T.StructType([
            T.StructField("timestamp", S), T.StructField("page_type", S),
            T.StructField("product_id", S), T.StructField("category_id", S),
            T.StructField("view_duration", T.LongType())]))),
        T.StructField("cart_contents", T.MapType(S, T.StructType([
            T.StructField("quantity", T.LongType()), T.StructField("price", T.DoubleType())]))),
        T.StructField("conversion_status", S), T.StructField("referrer", S)]),
    "transactions": T.StructType([
        T.StructField("transaction_id", S), T.StructField("session_id", S), T.StructField("user_id", S),
        T.StructField("timestamp", S),
        T.StructField("items", T.ArrayType(T.StructType([
            T.StructField("product_id", S), T.StructField("quantity", T.LongType()),
            T.StructField("unit_price", T.DoubleType()), T.StructField("subtotal", T.DoubleType())]))),
        T.StructField("subtotal", T.DoubleType()), T.StructField("discount", T.DoubleType()),
        T.StructField("total", T.DoubleType()),
        T.StructField("payment_method", S), T.StructField("status", S)]),
}


def load_raw(spark):
    ensure_jsonl()
    base = JSONL.resolve().as_uri()
    pattern = {"users": "users.jsonl", "categories": "categories.jsonl", "products": "products.jsonl",
               "transactions": "transactions.jsonl", "sessions": "sessions_*.jsonl"}
    return {k: spark.read.schema(SCHEMAS[k]).json(f"{base}/{v}") for k, v in pattern.items()}


def clean_all(spark, raw):
    """Clean + normalize every dataset. Returns dict of DataFrames."""
    # ---- sessions ----
    s = (raw["sessions"]
         .filter(F.col("session_id").isNotNull() & F.col("user_id").isNotNull())
         .dropDuplicates(["session_id"])
         .withColumn("start_time", F.col("start_time").cast("timestamp"))
         .withColumn("end_time", F.col("end_time").cast("timestamp"))
         .filter(F.col("end_time") >= F.col("start_time"))
         .filter(F.col("duration_seconds") > 0)
         .withColumn("device_type", F.lower(F.coalesce(F.col("device_profile.type"), F.lit("unknown"))))
         .withColumn("os", F.coalesce(F.col("device_profile.os"), F.lit("unknown")))
         .withColumn("browser", F.coalesce(F.col("device_profile.browser"), F.lit("unknown")))
         .withColumn("referrer", F.lower(F.coalesce(F.col("referrer"), F.lit("unknown"))))
         .withColumn("conversion_status", F.lower(F.coalesce(F.col("conversion_status"), F.lit("browsed"))))
         .withColumn("n_page_views", F.size("page_views"))
         .withColumn("n_products_viewed", F.size("viewed_products"))
         .withColumn("n_cart_items", F.size("cart_contents"))
         .withColumn("session_date", F.to_date("start_time"))
         .drop("device_profile"))

    # ---- transactions ----
    t = (raw["transactions"]
         .filter(F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull())
         .dropDuplicates(["transaction_id"])
         .withColumn("txn_ts", F.col("timestamp").cast("timestamp"))
         .drop("timestamp")
         .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)))
         .withColumn("status", F.lower(F.coalesce(F.col("status"), F.lit("unknown"))))
         .withColumn("payment_method", F.lower(F.coalesce(F.col("payment_method"), F.lit("unknown"))))
         .filter((F.col("total") >= 0) & (F.size("items") > 0))
         .withColumn("total_consistent",
                     F.abs(F.col("total") - (F.col("subtotal") - F.col("discount"))) <= 0.02)
         .withColumn("txn_date", F.to_date("txn_ts"))
         .withColumn("txn_month", F.date_format("txn_ts", "yyyy-MM")))

    items = (t.select("transaction_id", "user_id", "txn_ts", F.explode("items").alias("it"))
             .select("transaction_id", "user_id", "txn_ts",
                     F.col("it.product_id").alias("product_id"),
                     F.col("it.quantity").alias("quantity"),
                     F.col("it.unit_price").alias("unit_price"),
                     F.col("it.subtotal").alias("line_total"))
             .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0)))

    # ---- categories -> flat (category, subcategory) dimension, joined onto products ----
    subs = (raw["categories"]
            .select(F.col("category_id"), F.col("name").alias("category_name"),
                    F.explode("subcategories").alias("sc"))
            .select("category_id", "category_name",
                    F.col("sc.subcategory_id").alias("subcategory_id"),
                    F.col("sc.name").alias("subcategory_name"),
                    F.col("sc.profit_margin").alias("profit_margin")))
    p = (raw["products"]
         .filter(F.col("product_id").isNotNull())
         .dropDuplicates(["product_id"])
         .withColumn("creation_date", F.col("creation_date").cast("timestamp"))
         .withColumn("current_stock", F.greatest(F.col("current_stock"), F.lit(0).cast("long")))
         .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(True)))
         .withColumn("n_price_changes", F.size("price_history") - 1)
         .drop("price_history")
         .join(F.broadcast(subs), ["category_id", "subcategory_id"], "left"))

    # ---- users: normalize + reconcile last_active with real activity ----
    last_sess = s.groupBy("user_id").agg(F.max("start_time").alias("last_session"),
                                         F.count("*").alias("n_sessions"))
    last_txn = t.groupBy("user_id").agg(F.max("txn_ts").alias("last_txn"))
    u = (raw["users"]
         .filter(F.col("user_id").isNotNull())
         .dropDuplicates(["user_id"])
         .withColumn("registration_date", F.col("registration_date").cast("timestamp"))
         .withColumn("last_active_raw", F.col("last_active").cast("timestamp"))
         .withColumn("city", F.coalesce(F.initcap(F.trim(F.col("geo_data.city"))), F.lit("Unknown")))
         .withColumn("state", F.coalesce(F.upper(F.trim(F.col("geo_data.state"))), F.lit("NA")))
         .withColumn("country", F.coalesce(F.upper(F.trim(F.col("geo_data.country"))), F.lit("NA")))
         .drop("geo_data", "last_active")
         .join(last_sess, "user_id", "left")
         .join(last_txn, "user_id", "left")
         .withColumn("n_sessions", F.coalesce(F.col("n_sessions"), F.lit(0)))
         .withColumn("last_active", F.greatest("last_active_raw", "last_session", "last_txn")))

    return {"users": u, "products": p, "sessions": s, "transactions": t, "items": items}


def register_views(clean):
    """Expose cleaned DataFrames as Spark SQL temp views."""
    for name, df in clean.items():
        df.createOrReplaceTempView(name)


def save_csv(df, filename):
    """Collect a (small) result and write CSV with plain Python: no Hadoop/winutils needed."""
    RESULTS.mkdir(exist_ok=True)
    rows = df.collect()
    with open(RESULTS / filename, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        for r in rows:
            w.writerow(list(r))
    print(f"(saved results/{filename}, {len(rows)} rows)")
    return len(rows)
