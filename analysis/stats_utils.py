import os
import pandas as pd
from sqlalchemy import create_engine


def get_pg_engine():
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "postgres")
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD", "postgres")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url)


def load_mart_user_outcomes():
    engine = get_pg_engine()
    query = "SELECT * FROM mart_user_outcomes"
    return pd.read_sql(query, engine)
