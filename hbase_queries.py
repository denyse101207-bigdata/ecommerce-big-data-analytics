"""
hbase_queries.py -- Example HBase queries (via happybase). Each has an HBase-shell equivalent
in hbase_shell_commands.txt.

Usage: python hbase_queries.py [--user user_000004] [--product prod_01434]
"""
import argparse
from datetime import datetime, timezone
import happybase

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="localhost"); ap.add_argument("--port", type=int, default=9090)
ap.add_argument("--user", default=None); ap.add_argument("--product", default="prod_01434")
args = ap.parse_args()

MAX_TS = 9999999999999
def epoch_ms(iso):
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp() * 1000)
def rev(iso):
    return f"{MAX_TS - epoch_ms(iso):013d}"
def d(b):
    return b.decode() if b else ""

conn = happybase.Connection(args.host, port=args.port, timeout=120000)
sess, met = conn.table("user_sessions"), conn.table("product_metrics")

user = args.user
if user is None or not list(sess.scan(row_prefix=f"{user}#".encode(), limit=1)):
    first_key, _ = next(sess.scan(limit=1))
    user = first_key.decode().split("#")[0]
    print(f"(using user {user}, which has loaded sessions)")

# Q1: the user's 5 MOST RECENT sessions (newest-first because of the reverse-timestamp key)
print(f"\n=== Q1: last 5 sessions of {user} ===")
for key, data in sess.scan(row_prefix=f"{user}#".encode(), limit=5,
                           columns=[b"s:start_time", b"s:device_type", b"s:referrer", b"s:duration", b"s:conversion"]):
    print(key.decode(), {k.decode(): d(v) for k, v in data.items()})

# Q2: the user's sessions inside a TIME RANGE (Sep 1 - Oct 31 2026): range scan on the key
print(f"\n=== Q2: sessions of {user} between 2026-09-01 and 2026-10-31 ===")
start_key = f"{user}#{rev('2026-10-31T23:59:59')}".encode()        # newer date = smaller number
stop_key = f"{user}#{rev('2026-09-01T00:00:00')}".encode()
rows = list(sess.scan(row_start=start_key, row_stop=stop_key, columns=[b"s:start_time", b"s:conversion"]))
for key, data in rows:
    print(d(data[b"s:start_time"]), d(data[b"s:conversion"]))
print(f"-> {len(rows)} sessions")

# Q3: full journey of ONE session (sparse page-view columns + cart columns)
print(f"\n=== Q3: full journey of the most recent session of {user} ===")
key, row = next(sess.scan(row_prefix=f"{user}#".encode(), limit=1))
print("row key:", key.decode())
for k in sorted(row):
    if k.startswith((b"pv:", b"cart:")):
        print(f"  {k.decode():<18} {d(row[k])}")
print("  conversion:", d(row[b"s:conversion"]), "| referrer:", d(row[b"s:referrer"]))

# Q4: product performance over a period (time-range scan on product_id#yyyymmdd) + conversion rates
print(f"\n=== Q4: {args.product} daily metrics, 2026-08-01..2026-10-31 ===")
tot = {"views": 0, "cart_adds": 0, "purchases": 0}
n = 0
for key, data in met.scan(row_start=f"{args.product}#20260801".encode(), row_stop=f"{args.product}#20261101".encode()):
    n += 1
    for k in tot:
        tot[k] += int(d(data.get(f"m:{k}".encode())) or 0)
print(f"days with activity: {n} | totals: {tot}")
if tot["views"]:
    print(f"view->cart rate: {tot['cart_adds']/tot['views']:.1%} | view->purchase rate: {tot['purchases']/tot['views']:.1%}")

# Q5: how many session rows are stored (keys only = cheap)
print("\n=== Q5: row counts ===")
print("user_sessions rows   :", sum(1 for _ in sess.scan(filter=b"KeyOnlyFilter()")))
print("product_metrics rows :", sum(1 for _ in met.scan(filter=b"KeyOnlyFilter()")))
