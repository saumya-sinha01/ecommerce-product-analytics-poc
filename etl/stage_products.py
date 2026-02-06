# This script is the Product Catalog Stager. Its primary role is to take the raw product listing 
# (usually a CSV) and convert it into a clean, typed, and efficient Parquet format. By ensuring product IDs are unique and prices are valid numbers, it prevents "broken links" in your data when you eventually try to
# calculate total revenue.
from __future__ import annotations
import pandas as pd

# Import project utilities for configuration and cloud storage access
from etl.config import load_config
from etl.io_s3 import make_s3_client, join_key, s3_read_csv, s3_write_parquet


def main():
    # --- 1. INITIALIZATION ---
    cfg = load_config()
    s3 = cfg.s3
    client = make_s3_client(s3.endpoint_url, s3.region, s3.access_key, s3.secret_key)

    # Construct file paths for the raw input and the staged output
    raw_key = join_key(s3.raw_prefix, cfg.paths["raw"]["products"])
    out_key = join_key(s3.processed_prefix, cfg.paths["processed"]["staged"]["products"])

    # Fetch the raw product CSV from S3
    df = s3_read_csv(client, s3.raw_bucket, raw_key)

    # --- 2. DYNAMIC COLUMN MAPPING ---
    # We pull column names from the config (e.g., in case 'price' is called 'MSRP' in the raw file)
    pmap = cfg.schema.get("products", {})
    product_id = pmap.get("product_id", "product_id")
    price = pmap.get("price", "price")

    # --- 3. DATA CLEANING & TYPE CASTING ---
    # Validate Product IDs
    if product_id in df.columns:
        # Convert to numeric, turn errors to NaN, and use the 'Int64' type (allows Nulls)
        df[product_id] = pd.to_numeric(df[product_id], errors="coerce").astype("Int64")
        # Critical: A product record is useless without a valid ID
        df = df.dropna(subset=[product_id])

    # Validate Prices
    if price in df.columns:
        # Ensure prices are floating-point numbers so we can do math on them later
        df[price] = pd.to_numeric(df[price], errors="coerce")

    # --- 4. EXPORT ---
    # Save as Parquet, which is significantly smaller and faster to read for the "Marts" step
    s3_write_parquet(client, df, s3.processed_bucket, out_key)
    
    print(f"✅ staged products -> s3://{s3.processed_bucket}/{out_key} rows={len(df)} cols={len(df.columns)}")


if __name__ == "__main__":
    main()