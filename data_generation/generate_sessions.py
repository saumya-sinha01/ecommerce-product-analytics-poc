import os
import yaml
import numpy as np
import pandas as pd

CONFIG_PATH = "config/base.yaml"
USERS_PATH = "data/raw/users.csv"
OUTPUT_PATH = "data/raw/sessions.csv"


def load_base_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    cfg = load_base_config(CONFIG_PATH)
    seed = int(cfg["random_seed"])
    np.random.seed(seed + 3)

    start_date = pd.to_datetime(cfg["data"]["start_date"])
    end_date = pd.to_datetime(cfg["data"]["end_date"])

    users = pd.read_csv(USERS_PATH)
    users["signup_ts"] = pd.to_datetime(users["signup_ts"])

    session_rows = []
    session_id = 1

    for _, row in users.iterrows():
        user_id = int(row["user_id"])
        device_type = row["device_type"]
        signup_ts = row["signup_ts"]

        # sessions per user: 1-5
        n_sessions = np.random.randint(1, 6)

        for _ in range(n_sessions):
            # start day between signup and end_date
            if signup_ts > end_date:
                continue

            start_day = np.random.choice(pd.date_range(signup_ts, end_date, freq="D"))
            # random time within day
            start_time = pd.to_timedelta(np.random.randint(0, 24 * 60), unit="m")
            session_start = pd.to_datetime(start_day) + start_time

            # duration 1-30 minutes
            duration_min = np.random.randint(1, 31)
            session_end = session_start + pd.to_timedelta(duration_min, unit="m")

            session_rows.append({
                "session_id": session_id,
                "user_id": user_id,
                "session_start_ts": session_start,
                "session_end_ts": session_end,
                "device_type": device_type,
            })
            session_id += 1

    df = pd.DataFrame(session_rows)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df.head())


if __name__ == "__main__":
    main()
