"""
extract_hbase.py -- Part 3, step 2: export browsing sessions from HBase (table user_sessions).
  -> data_staging/hbase_sessions.jsonl   (one line per session row)
The user_id is the first part of the row key (user_id#reverse_timestamp).

Run:  python extract_hbase.py --host <WSL address from `hostname -I`>
"""
import argparse, json, os
import happybase

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="localhost")
ap.add_argument("--port", type=int, default=9090)
args = ap.parse_args()

conn = happybase.Connection(args.host, port=args.port, timeout=300000)
table = conn.table("user_sessions")
cols = [b"s:start_time", b"s:duration", b"s:conversion", b"s:device_type", b"s:referrer"]
dec = lambda b: b.decode() if b else None

os.makedirs("data_staging", exist_ok=True)
n = 0
with open("data_staging/hbase_sessions.jsonl", "w") as f:
    for key, d in table.scan(columns=cols, batch_size=2000):
        f.write(json.dumps({
            "user_id": key.decode().split("#")[0],
            "start_time": dec(d.get(b"s:start_time")),
            "duration": int(dec(d.get(b"s:duration")) or 0),
            "conversion": dec(d.get(b"s:conversion")),
            "device_type": dec(d.get(b"s:device_type")),
            "referrer": dec(d.get(b"s:referrer"))}) + "\n")
        n += 1
print(f"HBase session rows exported: {n:,}")
