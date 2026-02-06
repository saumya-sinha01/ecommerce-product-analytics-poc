from __future__ import annotations

import pandas as pd

# Internal modules for configuration and S3 interaction
from etl.config import load_config
from etl.io_s3 import (
    make_s3_client,
    join_key,
    s3_list_objects,
    s3_read_parquet,
    s3_write_parquet,
)


def load_clean_events(cfg, client) -> pd.DataFrame:
    """
    Scans S3 for all partitioned Parquet files in the 'clean events' folder,
    reads them, and merges them into a single DataFrame.
    """
    s3 = cfg.s3
    # Construct the S3 path (prefix)
    prefix = join_key(s3.processed_prefix, cfg.paths["processed"]["clean_events_prefix"])
    
    # List all files and filter for .parquet files
    keys = [k for k in s3_list_objects(client, s3.processed_bucket, prefix) if k.endswith(".parquet")]
    
    if not keys:
        raise ValueError(f"No clean events parquet found under s3://{s3.processed_bucket}/{prefix}/")

    # Read all Parquet files from S3 into a list of DataFrames
    dfs = [s3_read_parquet(client, s3.processed_bucket, k) for k in keys]
    # Combine everything into one table
    ev = pd.concat(dfs, ignore_index=True)

    # Standardize the timestamp column format
    if "event_ts" in ev.columns:
        ev["event_ts"] = pd.to_datetime(ev["event_ts"], errors="coerce", utc=False)

    return ev


def main():
    # --- 1. SETUP ---
    cfg = load_config()
    s3 = cfg.s3
    client = make_s3_client(s3.endpoint_url, s3.region, s3.access_key, s3.secret_key)

    # Read processed session data (used later for duration/bounce metrics)
    sessions_key = join_key(s3.processed_prefix, cfg.paths["processed"]["staged"]["sessions"])
    sessions = s3_read_parquet(client, s3.processed_bucket, sessions_key)

    # Read the master event log
    ev = load_clean_events(cfg, client)

    # Integrity check: Ensure required columns exist for the analysis
    required = ["user_id", "session_id", "event_ts", "event_name", "variant"]
    missing = [c for c in required if c not in ev.columns]
    if missing:
        raise ValueError(f"Clean events missing {missing}. Found columns: {list(ev.columns)}")

    # --- 2. CONFIGURATION ---
    # Load A/B testing parameters (e.g., how many days to track a user)
    mart_cfg = cfg.mart or {}
    outcome_days = int(mart_cfg.get("outcome_window_days", 7))
    names = mart_cfg.get("event_names", {}) or {}
    
    # Map friendly names to the actual event strings found in the data
    exposure_event = names.get("exposure_event", "pdp_view")
    add_to_cart_event = names.get("add_to_cart", "add_to_cart")
    begin_checkout_event = names.get("begin_checkout", "begin_checkout")
    purchase_event = names.get("purchase", "purchase")

    # --- 3. MART 1: USER EXPOSURE ---
    # Define 'Exposure' as the first time a user sees the experiment (e.g., views a Product Detail Page)
    exposure = (
        ev.loc[ev["event_name"] == exposure_event, ["user_id", "variant", "event_ts", "session_id"]]
        .dropna(subset=["user_id", "event_ts"])
        .sort_values(["user_id", "event_ts"])
        .groupby("user_id", as_index=False)
        .first() # Get only the earliest timestamp per user
        .rename(columns={"event_ts": "exposure_ts", "session_id": "exposure_session_id"})
    )

    # Assign an experiment ID (from data if available, otherwise from config)
    if "experiment_id" in ev.columns:
        exp_map = ev[["user_id", "experiment_id"]].dropna().drop_duplicates("user_id")
        exposure = exposure.merge(exp_map, on="user_id", how="left")
    else:
        exposure["experiment_id"] = cfg.experiment.get("default_experiment_id", "pdp_redesign_experiment")

    if exposure.empty:
        raise ValueError(f"No exposure rows found for '{exposure_event}'.")

    print(f"✅ mart_user_exposure rows={len(exposure)} (eligible users)")

    # Define the deadline for measuring outcomes (e.g., exposure + 7 days)
    exposure["window_end_ts"] = exposure["exposure_ts"] + pd.to_timedelta(outcome_days, unit="D")

    # --- 4. FILTERING & FLAG GENERATION ---
    # Only look at events that happened AFTER exposure and BEFORE the window deadline
    evw = ev.merge(exposure[["user_id", "exposure_ts", "window_end_ts", "exposure_session_id"]], on="user_id", how="inner")
    evw = evw[(evw["event_ts"] >= evw["exposure_ts"]) & (evw["event_ts"] < evw["window_end_ts"])]

    # Create binary flags (1 or 0) for key business actions
    evw["is_add_to_cart"] = (evw["event_name"] == add_to_cart_event)
    evw["is_begin_checkout"] = (evw["event_name"] == begin_checkout_event)
    evw["is_purchase"] = (evw["event_name"] == purchase_event)

    if "net_revenue" not in evw.columns:
        evw["net_revenue"] = 0.0

    # --- 5. AGGREGATING OUTCOMES ---
    # Pivot from many events per user to ONE row per user with summary stats
    outcomes = evw.groupby("user_id", as_index=False).agg(
        add_to_cart=("is_add_to_cart", "max"),        # Did they ever add to cart? (1/0)
        begin_checkout=("is_begin_checkout", "max"),  # Did they ever start checkout? (1/0)
        purchased=("is_purchase", "max"),             # Did they ever buy? (1/0)
        revenue=("net_revenue", "sum"),               # Total revenue in 7 days
        events_in_window=("event_name", "count"),     # Total engagement
    )

    # Calculate Bounce Proxy: Was the exposure session very short (only 1 event)?
    exp_sess_counts = (
        ev.merge(exposure[["user_id", "exposure_session_id"]], on="user_id", how="inner")
          .query("session_id == exposure_session_id")
          .groupby("user_id", as_index=False)
          .agg(events_in_exposure_session=("event_name", "count"))
    )
    outcomes = outcomes.merge(exp_sess_counts, on="user_id", how="left")
    outcomes["bounce"] = (outcomes["events_in_exposure_session"].fillna(0) <= 1).astype(int)

    # Calculate Average Session Duration within the 7-day window
    if "session_start_ts" in sessions.columns and "session_duration_seconds" in sessions.columns:
        sessions["session_start_ts"] = pd.to_datetime(sessions["session_start_ts"], errors="coerce", utc=False)
        sessw = sessions.merge(exposure[["user_id", "exposure_ts", "window_end_ts"]], on="user_id", how="inner")
        sessw = sessw[(sessw["session_start_ts"] >= sessw["exposure_ts"]) & (sessw["session_start_ts"] < sessw["window_end_ts"])]
        dur = sessw.groupby("user_id", as_index=False)["session_duration_seconds"].mean().rename(
            columns={"session_duration_seconds": "avg_session_duration_seconds"}
        )
        outcomes = outcomes.merge(dur, on="user_id", how="left")
    else:
        outcomes["avg_session_duration_seconds"] = None

    # Calculate 7-Day Retention: Did the user come back exactly 1 week later?
    evr = ev.merge(exposure[["user_id", "exposure_ts"]], on="user_id", how="inner")
    ret_start = evr["exposure_ts"] + pd.to_timedelta(7, unit="D")
    ret_end = evr["exposure_ts"] + pd.to_timedelta(8, unit="D")
    evr = evr[(evr["event_ts"] >= ret_start) & (evr["event_ts"] < ret_end)]
    retained = evr.groupby("user_id").size().rename("retained_7d").reset_index()
    retained["retained_7d"] = 1

    # Merge retention back and fill missing with 0 (meaning they didn't return)
    outcomes = outcomes.merge(retained, on="user_id", how="left")
    outcomes["retained_7d"] = outcomes["retained_7d"].fillna(0).astype(int)

    # --- 6. FINAL TABLE & EXPORT ---
    # Combine exposure info with all calculated metrics
    mart_user_outcomes = exposure[["experiment_id", "user_id", "variant", "exposure_ts"]].merge(
        outcomes, on="user_id", how="left"
    ).fillna({"add_to_cart": 0, "begin_checkout": 0, "purchased": 0, "revenue": 0.0, "bounce": 0, "retained_7d": 0})

    # Save both tables back to S3 as Parquet files
    out_exposure_key = join_key(s3.processed_prefix, cfg.paths["processed"]["marts"]["user_exposure"])
    out_outcomes_key = join_key(s3.processed_prefix, cfg.paths["processed"]["marts"]["user_outcomes"])

    s3_write_parquet(client, exposure.drop(columns=["window_end_ts"]), s3.processed_bucket, out_exposure_key)
    s3_write_parquet(client, mart_user_outcomes, s3.processed_bucket, out_outcomes_key)

    print(f"✅ wrote mart_user_outcomes -> s3://{s3.processed_bucket}/{out_outcomes_key}")


if __name__ == "__main__":
    main()