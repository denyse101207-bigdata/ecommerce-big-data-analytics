"""
hbase_load.py -- Load session data and product metrics into HBase (via Thrift/happybase).

TABLE 1: user_sessions   (one row per session)
  Row key : <user_id>#<reverse_timestamp>   reverse_ts = 9999999999999 - start_time_ms (13 digits)
            -> all sessions of a user are adjacent, NEWEST FIRST: "last N sessions" = one short scan.
  Families: s    session metadata (device, referrer, duration, conversion ...)
            pv   page views, one SPARSE column per page view  (pv:00, pv:01 ...) value = type|product|category|seconds|time
            cart cart contents, one column per product        (cart:<product_id>)  value = qty|price

TABLE 2: product_metrics (one row per product per day)
  Row key : <product_id>#<yyyymmdd>   -> a time-range scan on one product = its performance over a period
  Family  : m    views, cart_adds, purchases, units_sold, revenue

Usage:  python hbase_load.py [--limit 30000] [--host localhost] [--port 9090] [--data-dir data]
        --limit 0 loads ALL sessions (slower). product_metrics always uses ALL sessions + transactions.
"""
import argparse, glob, json, os, time
from collections import defaultdict
from datetime import datetime, timezone
import happybase

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="localhost")
ap.add_argument("--port", type=int, default=9090)
ap.add_argument("--data-dir", default="data")
ap.add_argument("--limit", type=int, default=30000, help="max sessions to load (0 = all)")
args = ap.parse_args()

MAX_TS = 9999999999999
def epoch_ms(iso):
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp() * 1000)
def session_key(user_id, start_iso):
    return f"{user_id}#{MAX_TS - epoch_ms(start_iso):013d}".encode()
def s(x):
    return "" if x is None else str(x)

conn = happybase.Connection(args.host, port=args.port, timeout=120000)
existing = {t.decode() for t in conn.tables()}
for need in ("user_sessions", "product_metrics"):
    if need not in existing:
        raise SystemExit(f"Table '{need}' not found. Create it in the HBase shell first.")
sess_tbl, met_tbl = conn.table("user_sessions"), conn.table("product_metrics")

metrics = defaultdict(lambda: defaultdict(float))     # (product_id, yyyymmdd) -> counters
loaded, seen, t0 = 0, 0, time.time()

with sess_tbl.batch(batch_size=500) as b:
    for path in sorted(glob.glob(os.path.join(args.data_dir, "sessions_*.json"))):
        with open(path) as f:
            sessions = json.load(f)
        for ss in sessions:
            seen += 1
            day = ss["start_time"][:10].replace("-", "")
            for pv in ss["page_views"]:                                  # metrics from ALL sessions
                if pv["page_type"] == "product_detail" and pv["product_id"]:
                    metrics[(pv["product_id"], day)]["views"] += 1
            for pid in ss["cart_contents"]:
                metrics[(pid, day)]["cart_adds"] += 1

            if args.limit == 0 or loaded < args.limit:                   # sessions: representative subset
                row = {
                    b"s:session_id": ss["session_id"].encode(),
                    b"s:start_time": ss["start_time"].encode(),
                    b"s:end_time": ss["end_time"].encode(),
                    b"s:duration": s(ss["duration_seconds"]).encode(),
                    b"s:device_type": ss["device_profile"]["type"].encode(),
                    b"s:os": ss["device_profile"]["os"].encode(),
                    b"s:browser": ss["device_profile"]["browser"].encode(),
                    b"s:referrer": ss["referrer"].encode(),
                    b"s:conversion": ss["conversion_status"].encode(),
                    b"s:city": s(ss["geo_data"].get("city")).encode(),
                    b"s:state": s(ss["geo_data"].get("state")).encode(),
                    b"s:country": s(ss["geo_data"].get("country")).encode(),
                    b"s:page_view_count": s(len(ss["page_views"])).encode(),
                }
                for i, pv in enumerate(ss["page_views"]):                # sparse: one column per page view
                    row[f"pv:{i:02d}".encode()] = "|".join(
                        [pv["page_type"], s(pv["product_id"]), s(pv["category_id"]),
                         s(pv["view_duration"]), pv["timestamp"]]).encode()
                for pid, v in ss["cart_contents"].items():
                    row[f"cart:{pid}".encode()] = f"{v['quantity']}|{v['price']}".encode()
                b.put(session_key(ss["user_id"], ss["start_time"]), row)
                loaded += 1
        print(f"  {os.path.basename(path)} done  (loaded {loaded:,} session rows, {time.time()-t0:.0f}s)")

with open(os.path.join(args.data_dir, "transactions.json")) as f:        # purchases per product/day
    for t in json.load(f):
        day = t["timestamp"][:10].replace("-", "")
        for it in t["items"]:
            m = metrics[(it["product_id"], day)]
            m["purchases"] += 1; m["units_sold"] += it["quantity"]; m["revenue"] += it["subtotal"]

with met_tbl.batch(batch_size=1000) as b:
    for (pid, day), m in metrics.items():
        b.put(f"{pid}#{day}".encode(), {
            b"m:views": s(int(m["views"])).encode(), b"m:cart_adds": s(int(m["cart_adds"])).encode(),
            b"m:purchases": s(int(m["purchases"])).encode(), b"m:units_sold": s(int(m["units_sold"])).encode(),
            b"m:revenue": f"{m['revenue']:.2f}".encode()})

print(f"\nDone in {time.time()-t0:.0f}s: {loaded:,} rows in user_sessions "
      f"(from {seen:,} sessions seen), {len(metrics):,} rows in product_metrics")
