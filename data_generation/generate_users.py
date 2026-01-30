import os
import yaml
import numpy as np
import pandas as pd

#Path to the project’s main configuration file, outpy saved files
#Number of synthetic users to generate.
CONFIG_PATH = "config/base.yaml"
OUTPUT_PATH = "data/raw/users.csv"
N_USERS = 1000

#Allowed country values for users, allowed device types and probability distribution for devices
COUNTRIES = ["US", "IN", "UK", "DE", "CA"]
DEVICE_TYPES = ["mobile", "desktop"]
DEVICE_PROBS = [0.65, 0.35]

#function to load YAML config
#input is a string, output is a dictionary
#Opens the config file in read mode
#Parses YAML into a Python dictionary and safe_load avoids executing unsafe YAML code.
def load_base_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def main():
    #Reads base.yaml into cfg
    cfg = load_base_config(CONFIG_PATH)

    # TODO 1: read start_date/end_date and random_seed from cfg
    start_date = pd.to_datetime(cfg["data"]["start_date"])
    end_date = pd.to_datetime(cfg["data"]["end_date"])
    

    # TODO 2: set np.random.seed(...)
    seed = int(cfg["random_seed"])
    np.random.seed(seed)
    #Fixes NumPy’s random generator so results are repeatable.

    # TODO 3: generate user_id from 1..N_USERS
    # TODO 4: generate signup_ts uniformly across date range
    # TODO 5: generate country + device_type (weighted)
    # Generate signup timestamps (uniform across days)

    #Creates a list of every day between start and end dates.
    all_days = pd.date_range(start_date, end_date, freq="D")

    #Randomly selects N_USERS dates from all_days.
    # replace=True allows multiple users to sign up on the same day.
    # Result: realistic signup distribution.
    signup_ts = pd.to_datetime(
        np.random.choice(all_days, size=N_USERS, replace=True)
    )

    # Generate user fields 1->1000
    user_id = np.arange(1, N_USERS + 1)
    country = np.random.choice(COUNTRIES, size=N_USERS, replace=True)
    device_type = np.random.choice(DEVICE_TYPES, size=N_USERS, p=DEVICE_PROBS)

    # TODO 6: define is_new_user rule (simple rule is fine)
    #Marks users who signed up within first 30 days as True
    is_new_user = signup_ts <= (start_date + pd.Timedelta(days=30))
    df = pd.DataFrame({
        # TODO: fill columns
        "user_id": user_id,
        "signup_ts": signup_ts,
        "country": country,
        "device_type": device_type,
        "is_new_user": is_new_user.astype(bool),
    })

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    # print("N_USERS =", N_USERS)
    # print("len(user_id) =", len(user_id))
    # print("len(signup_ts) =", len(signup_ts))
    # print("len(country) =", len(country))
    # print("len(device_type) =", len(device_type))


    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df.head())

if __name__ == "__main__":
    main()
