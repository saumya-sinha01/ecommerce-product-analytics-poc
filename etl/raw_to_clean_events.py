# This script is the Cleaning & Normalization Layer (the "Transform" in ETL).
# It takes messy, raw CSV files from the S3 "raw" bucket, standardizes the column names 
# and data types, joins experiment assignments (who is in the Test vs. Control group), 
# and saves the results as optimized, partitioned Parquet file

from __future__ import annotations

import uuid
import pandas as pd

from etl.config import load_config
from etl.io_s3 import make_s3_client, join_key, s3_read_csv, s3_write_parquet


def normalize_event_name(name: str, aliases: dict[str, str]) -> str:
    """
    Standardizes event strings (e.g., 'Purchase ', 'PURCHASE', and 'buy_now' 
    all become 'purchase') based on a mapping defined in the config.
    """
    if name is None:
        return ""
    x = str(name).strip().lower()
    x = x.replace(" ", "_")
    return aliases.get(x, x)


def main():
    # --- 1. INITIALIZATION ---
    cfg = load_config()
    s3 = cfg.s3
    client = make_s3_client(s3.endpoint_url, s3.region, s3.access_key, s3.secret_key)

    # Define source (S3 Raw) and destination (S3 Processed) paths
    raw_events_key = join_key(s3.raw_prefix, cfg.paths["raw"]["events"])
    raw_assign_key = join_key(s3.raw_prefix, cfg.paths["raw"]["experiment_assignments"])
    out_prefix = join_key(s3.processed_prefix, cfg.paths["processed"]["clean_events_prefix"])

    # --- 2. SCHEMA MAPPING ---
    # We pull column names from the YAML config so the code doesn't break if 
    # the raw CSV column headers change (e.g., if 'user_id' becomes 'UID')
    emap = cfg.schema.get("events", {})
    amap = cfg.schema.get("experiment_assignments", {})

    # Define 'Canonical' names: The standard names our pipeline expects downstream
    canonical_required = {
        emap.get("event_ts", "event_ts"): "event_ts",
        emap.get("user_id", "user_id"): "user_id",
        emap.get("session_id", "session_id"): "session_id",
        emap.get("event_name", "event_name"): "event_name",
    }

    # --- 3. LOAD & CLEAN EXPERIMENT ASSIGNMENTS ---
    assign_df = s3_read_csv(client, s3.raw_bucket, raw_assign_key)
    
    # Ensure IDs are numeric and strings are clean of whitespace
    col_a_user = amap.get("user_id", "user_id")
    col_a_var = amap.get("variant", "variant")
    col_a_exp = amap.get("experiment_id", "experiment_id")

    assign_df[col_a_user] = pd.to_numeric(assign_df[col_a_user], errors="coerce").astype("Int64")
    
    # Remove duplicates: A user should only be assigned to one experiment variant
    assign_df = (
        assign_df.dropna(subset=[col_a_user])
                 .drop_duplicates(subset=[col_a_user], keep="first")
                 [[col_a_user, col_a_var, col_a_exp]]
    )

    # --- 4. LOAD & CLEAN EVENTS ---
    events_df = s3_read_csv(client, s3.raw_bucket, raw_events_key)

    # Rename raw columns to our standard internal names
    events_df = events_df.rename(columns=canonical_required)
    
    # Convert timestamps and IDs to the correct technical types
    events_df["event_ts"] = pd.to_datetime(events_df["event_ts"], errors="coerce")
    events_df["user_id"] = pd.to_numeric(events_df["user_id"], errors="coerce").astype("Int64")
    
    # Drop "trash" data: rows missing a timestamp, user, or session are useless
    events_df = events_df.dropna(subset=["event_ts", "user_id", "session_id"])

    # Normalize names (e.g., fixing typos in the raw logs)
    aliases = cfg.etl.get("event_aliases", {})
    events_df["event_name"] = events_df["event_name"].apply(lambda x: normalize_event_name(x, aliases))

    # --- 5. FINANCIAL CALCULATIONS ---
    # Ensure revenue columns exist and fill missing values with 0
    events_df["price_paid"] = pd.to_numeric(events_df["price_paid"], errors="coerce").fillna(0.0)
    events_df["quantity"] = pd.to_numeric(events_df["quantity"], errors="coerce").fillna(1).astype(int)
    
    # --- 6. JOINING & DERIVING COLS ---
    # Attach experiment info (Variant A or B) to every event row
    clean_df = events_df.merge(assign_df, how="left", left_on="user_id", right_on=col_a_user)

    # Calculate net revenue (only for purchase events)
    clean_df["dt"] = clean_df["event_ts"].dt.date.astype(str)
    clean_df["net_revenue"] = (clean_df["price_paid"] * clean_df["quantity"]) - clean_df.get("discount_amount", 0.0)
    clean_df.loc[clean_df["event_name"] != "purchase", "net_revenue"] = 0.0

    # --- 7. PARTITIONED WRITE ---
    # Instead of one giant file, we save data into folders grouped by date (dt=YYYY-MM-DD)
    # This makes later analysis significantly faster
    
    for dt_val, part_df in clean_df.groupby("dt", dropna=True):
        part_id = uuid.uuid4().hex[:12] # Generate a unique filename
        out_key = join_key(out_prefix, f"dt={dt_val}", f"part-{part_id}.parquet")
        s3_write_parquet(client, part_df, s3.processed_bucket, out_key)

    print(f"✅ Wrote clean events to S3 partitioned by date.")


if __name__ == "__main__":
    main()