# This script represents the final stage of your ETL (Extract, Transform, Load)
# pipeline: Loading into the Data Warehouse. It takes the high-value
# "Data Marts" from S3 cloud storage and moves them into a PostgreSQL database, 
# making them ready for Business Intelligence
# (BI) tools like Tableau or Looker.

from __future__ import annotations

import os
import pandas as pd
from sqlalchemy import create_engine, text

from etl.config import load_config
from etl.io_s3 import make_s3_client, join_key, s3_read_parquet


def get_pg_url() -> str:
    """
    Constructs the database connection string (URL) using environment variables.
    Provides defaults for a standard Docker-based Postgres setup.
    """
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "postgres")
    # Format: postgresql+psycopg2://user:password@host:port/database
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def ensure_tables(engine) -> None:
    """
    Sets up the Database Schema. 
    It deletes existing tables and recreates them to ensure the structure 
    matches our data exactly (Idempotent operation).
    """
    ddl = """
    DROP TABLE IF EXISTS mart_user_exposure;
    DROP TABLE IF EXISTS mart_user_outcomes;

    CREATE TABLE mart_user_exposure (
      experiment_id TEXT,
      user_id BIGINT PRIMARY KEY,
      variant TEXT,
      exposure_ts TIMESTAMP,
      exposure_session_id BIGINT
    );

    CREATE TABLE mart_user_outcomes (
      experiment_id TEXT,
      user_id BIGINT PRIMARY KEY,
      variant TEXT,
      exposure_ts TIMESTAMP,
      add_to_cart INT,
      begin_checkout INT,
      purchased INT,
      revenue DOUBLE PRECISION,
      events_in_window BIGINT,
      events_in_exposure_session BIGINT,
      bounce INT,
      avg_session_duration_seconds DOUBLE PRECISION,
      retained_7d INT
    );
    """
    # Execute the SQL commands inside a single transaction
    with engine.begin() as conn:
        conn.execute(text(ddl))


def coerce_types(exposure: pd.DataFrame, outcomes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Data Cleaning: Ensures Pandas data types match Postgres expectations.
    Prevents errors like trying to put a string 'NULL' into an Integer column.
    """
    # Standardize User IDs and Timestamps for the exposure table
    if "user_id" in exposure.columns:
        exposure["user_id"] = pd.to_numeric(exposure["user_id"], errors="coerce").astype("Int64")
    if "exposure_session_id" in exposure.columns:
        exposure["exposure_session_id"] = pd.to_numeric(exposure["exposure_session_id"], errors="coerce").astype("Int64")
    if "exposure_ts" in exposure.columns:
        exposure["exposure_ts"] = pd.to_datetime(exposure["exposure_ts"], errors="coerce")

    # Convert binary flags (True/False) to Integers (1/0) for SQL metrics
    for c in ["add_to_cart", "begin_checkout", "purchased", "bounce", "retained_7d"]:
        if c in outcomes.columns:
            outcomes[c] = pd.to_numeric(outcomes[c], errors="coerce").fillna(0).astype(int)

    # Ensure financial/duration metrics are Floats
    for c in ["revenue", "avg_session_duration_seconds"]:
        if c in outcomes.columns:
            outcomes[c] = pd.to_numeric(outcomes[c], errors="coerce").fillna(0.0)

    # Remove any corrupted rows where the Primary Key (user_id) is missing
    exposure = exposure.dropna(subset=["user_id"]).copy()
    outcomes = outcomes.dropna(subset=["user_id"]).copy()

    return exposure, outcomes


def main():
    # 1. LOAD SETTINGS
    cfg = load_config()
    s3 = cfg.s3
    client = make_s3_client(s3.endpoint_url, s3.region, s3.access_key, s3.secret_key)

    # 2. EXTRACT FROM S3
    exposure_key = join_key(s3.processed_prefix, cfg.paths["processed"]["marts"]["user_exposure"])
    outcomes_key = join_key(s3.processed_prefix, cfg.paths["processed"]["marts"]["user_outcomes"])

    exposure = s3_read_parquet(client, s3.processed_bucket, exposure_key)
    outcomes = s3_read_parquet(client, s3.processed_bucket, outcomes_key)

    # 3. TRANSFORM TYPES
    exposure, outcomes = coerce_types(exposure, outcomes)

    # 4. LOAD INTO POSTGRES
    pg_url = get_pg_url()
    engine = create_engine(pg_url)

    # Initialize SQL tables
    ensure_tables(engine)

    # Batch upload data using 'multi' for high performance
    # chunksize prevents the script from overwhelming the database memory
    exposure.to_sql("mart_user_exposure", engine, if_exists="append", index=False, method="multi", chunksize=2000)
    outcomes.to_sql("mart_user_outcomes", engine, if_exists="append", index=False, method="multi", chunksize=2000)

    # 5. VERIFICATION (SANITY CHECKS)
    with engine.begin() as conn:
        exp_n = conn.execute(text("SELECT COUNT(*) FROM mart_user_exposure")).scalar_one()
        out_n = conn.execute(text("SELECT COUNT(*) FROM mart_user_outcomes")).scalar_one()
        print(f"✅ Postgres mart_user_exposure rows={exp_n}")
        print(f"✅ Postgres mart_user_outcomes rows={out_n}")

        # Quickly check the experiment split (Control vs Test)
        rows = conn.execute(text("""
          SELECT variant, COUNT(*) AS n
          FROM mart_user_outcomes
          GROUP BY 1
          ORDER BY 1
        """)).fetchall()
        print("variant split in mart_user_outcomes:", rows)

    print("✅ Done.")


if __name__ == "__main__":
    main()