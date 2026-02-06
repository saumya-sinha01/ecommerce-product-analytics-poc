# This script is the Experiment Assignment Stager. Its job is to take the raw "who is in which group" 
# data (often generated in CSV format), clean it up,and convert it into a high-performance Parquet file.

# This is a critical step in A/B testing: ensuring that every user is mapped to exactly 
# one variant (e.g., "control" or "test") before you start calculating metrics

from __future__ import annotations
import pandas as pd

# Importing our custom configuration and S3 utility tools
from etl.config import load_config
from etl.io_s3 import make_s3_client, join_key, s3_read_csv, s3_write_parquet


def main():
    # --- 1. SETUP ---
    # Load configuration and initialize the S3 client
    cfg = load_config()
    s3 = cfg.s3
    client = make_s3_client(s3.endpoint_url, s3.region, s3.access_key, s3.secret_key)

    # Define paths: Reading from 'raw' and writing to 'processed/staged'
    raw_key = join_key(s3.raw_prefix, cfg.paths["raw"]["experiment_assignments"])
    out_key = join_key(s3.processed_prefix, cfg.paths["processed"]["staged"]["experiment_assignments"])

    # Load the raw assignment data from S3
    df = s3_read_csv(client, s3.raw_bucket, raw_key)

    # --- 2. DYNAMIC COLUMN MAPPING ---
    # We look up what the columns are named in the config (schema.yaml) 
    # so the code doesn't break if someone renames "variant" to "group" in the CSV
    amap = cfg.schema.get("experiment_assignments", {})
    user_id = amap.get("user_id", "user_id")
    variant = amap.get("variant", "variant")
    experiment_id = amap.get("experiment_id", "experiment_id")

    # Validation: Ensure the CSV actually contains the essential data columns
    if user_id not in df.columns or variant not in df.columns:
        raise ValueError(f"Assignments must include {user_id} and {variant}. Found: {list(df.columns)}")

    # --- 3. DATA CLEANING & TYPE CASTING ---
    # Convert User IDs to a large integer type (Int64)
    # Errors="coerce" turns non-numeric junk into NaN (Not a Number)
    df[user_id] = pd.to_numeric(df[user_id], errors="coerce").astype("Int64")
    
    # Clean whitespace from the variant names (e.g., " control " -> "control")
    df[variant] = df[variant].astype(str).str.strip()

    # If the CSV doesn't specify an Experiment ID, apply a default from our config
    if experiment_id not in df.columns:
        df[experiment_id] = cfg.experiment.get("default_experiment_id", "pdp_redesign_experiment")
    else:
        df[experiment_id] = df[experiment_id].astype(str).str.strip()

    # --- 4. INTEGRITY CHECKS ---
    # Drop rows without IDs and ensure each user is only assigned to ONE variant.
    # Duplicates can skew A/B test results significantly.
    df = df.dropna(subset=[user_id]).drop_duplicates(subset=[user_id], keep="first")

    # --- 5. EXPORT ---
    # Save the cleaned table as a Parquet file for efficient downstream processing
    s3_write_parquet(client, df, s3.processed_bucket, out_key)
    
    # Final logging to verify the distribution of the test groups
    print(f"✅ staged experiment_assignments -> s3://{s3.processed_bucket}/{out_key} rows={len(df)} cols={len(df.columns)}")
    print("variant split:")
    print(df[variant].value_counts(dropna=False))


if __name__ == "__main__":
    main()