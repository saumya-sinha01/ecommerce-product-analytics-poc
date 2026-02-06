from __future__ import annotations
import pandas as pd

from etl.config import load_config
from etl.io_s3 import make_s3_client, join_key, s3_read_csv, s3_write_parquet


def main():
    cfg = load_config()
    s3 = cfg.s3
    client = make_s3_client(s3.endpoint_url, s3.region, s3.access_key, s3.secret_key)

    raw_key = join_key(s3.raw_prefix, cfg.paths["raw"]["sessions"])
    out_key = join_key(s3.processed_prefix, cfg.paths["processed"]["staged"]["sessions"])

    df = s3_read_csv(client, s3.raw_bucket, raw_key)

    smap = cfg.schema.get("sessions", {})
    session_id = smap.get("session_id", "session_id")
    user_id = smap.get("user_id", "user_id")
    start_ts = smap.get("session_start_ts", "session_start_ts")
    end_ts = smap.get("session_end_ts", "session_end_ts")

    if session_id in df.columns:
        df[session_id] = pd.to_numeric(df[session_id], errors="coerce").astype("Int64")
    if user_id in df.columns:
        df[user_id] = pd.to_numeric(df[user_id], errors="coerce").astype("Int64")

    if start_ts in df.columns:
        df[start_ts] = pd.to_datetime(df[start_ts], errors="coerce", utc=False)
    if end_ts in df.columns:
        df[end_ts] = pd.to_datetime(df[end_ts], errors="coerce", utc=False)

    # If both timestamps exist, compute duration secs
    if start_ts in df.columns and end_ts in df.columns:
        df["session_duration_seconds"] = (df[end_ts] - df[start_ts]).dt.total_seconds()

    # Keep integrity
    if session_id in df.columns:
        df = df.dropna(subset=[session_id])

    s3_write_parquet(client, df, s3.processed_bucket, out_key)
    print(f"✅ staged sessions -> s3://{s3.processed_bucket}/{out_key} rows={len(df)} cols={len(df.columns)}")


if __name__ == "__main__":
    main()
