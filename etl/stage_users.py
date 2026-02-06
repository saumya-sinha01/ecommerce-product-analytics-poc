from __future__ import annotations
import pandas as pd

from etl.config import load_config
from etl.io_s3 import make_s3_client, join_key, s3_read_csv, s3_write_parquet


def main():
    cfg = load_config()
    s3 = cfg.s3
    client = make_s3_client(s3.endpoint_url, s3.region, s3.access_key, s3.secret_key)

    raw_key = join_key(s3.raw_prefix, cfg.paths["raw"]["users"])
    out_key = join_key(s3.processed_prefix, cfg.paths["processed"]["staged"]["users"])

    df = s3_read_csv(client, s3.raw_bucket, raw_key)

    # schema mapping (optional)
    umap = cfg.schema.get("users", {})
    user_id = umap.get("user_id", "user_id")
    created_ts = umap.get("created_ts", "created_ts")

    if user_id in df.columns:
        df[user_id] = pd.to_numeric(df[user_id], errors="coerce").astype("Int64")

    if created_ts in df.columns:
        df[created_ts] = pd.to_datetime(df[created_ts], errors="coerce", utc=False)

    # Drop rows missing user_id (dimension integrity)
    if user_id in df.columns:
        df = df.dropna(subset=[user_id])

    s3_write_parquet(client, df, s3.processed_bucket, out_key)
    print(f"✅ staged users -> s3://{s3.processed_bucket}/{out_key} rows={len(df)} cols={len(df.columns)}")


if __name__ == "__main__":
    main()
