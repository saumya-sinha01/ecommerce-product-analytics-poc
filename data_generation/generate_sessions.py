import os
import yaml
import numpy as np
import pandas as pd

# Global Paths: Linking configuration, input user data, and the new session output
CONFIG_PATH = "config/base.yaml"
USERS_PATH = "data/raw/users.csv"
OUTPUT_PATH = "data/raw/sessions.csv"


def load_base_config(path: str) -> dict:
    """Reads the YAML file to get global settings like date ranges and random seeds."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    # 1. Initialization
    cfg = load_base_config(CONFIG_PATH)
    seed = int(cfg["random_seed"])
    # Using seed + 3 ensures this script generates different randomness than products/users
    np.random.seed(seed + 3)

    # Convert config strings into Python Datetime objects
    start_date = pd.to_datetime(cfg["data"]["start_date"])
    end_date = pd.to_datetime(cfg["data"]["end_date"])

    # 2. Data Loading
    # Load the existing user database so we can generate sessions for real user IDs
    users = pd.read_csv(USERS_PATH)
    users["signup_ts"] = pd.to_datetime(users["signup_ts"])

    session_rows = []
    session_id = 1

    # 3. Session Generation Logic
    # We iterate through every user in the CSV to give them activity
    for _, row in users.iterrows():
        user_id = int(row["user_id"])
        device_type = row["device_type"]
        signup_ts = row["signup_ts"]

        # Randomly decide how many times this specific user logged in (1 to 5 times)
        n_sessions = np.random.randint(1, 6)

        for _ in range(n_sessions):
            # Guardrail: A user cannot have a session if they signed up after the data cutoff
            if signup_ts > end_date:
                continue

            # Pick a random day between their signup date and the simulation end date
            start_day = np.random.choice(pd.date_range(signup_ts, end_date, freq="D"))
            
            # Pick a random minute in the day (0 to 1439) to set the exact start time
            start_time = pd.to_timedelta(np.random.randint(0, 24 * 60), unit="m")
            session_start = pd.to_datetime(start_day) + start_time

            # Randomly decide how long the session lasted (1 to 30 minutes)
            duration_min = np.random.randint(1, 31)
            session_end = session_start + pd.to_timedelta(duration_min, unit="m")

            # 4. Storage
            # Append the record to our temporary list
            session_rows.append({
                "session_id": session_id,
                "user_id": user_id,
                "session_start_ts": session_start,
                "session_end_ts": session_end,
                "device_type": device_type, # Inherited from the user's primary device
            })
            session_id += 1

    # 5. Save and Export
    df = pd.DataFrame(session_rows)

    # Create the directory if it's missing (e.g., 'data/raw/')
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df.head())


if __name__ == "__main__":
    main()