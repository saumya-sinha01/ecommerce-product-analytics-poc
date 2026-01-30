import os
import yaml
import numpy as np
import pandas as pd

CONFIG_PATH = "config/base.yaml"
USERS_PATH = "data/raw/users.csv"
OUTPUT_PATH = "data/raw/experiment_assignments.csv"

EXPERIMENT_NAME = "pdp_redesign_experiment"
TREATMENT_PROB = 0.5  # 50/50


def load_base_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    cfg = load_base_config(CONFIG_PATH)
    seed = int(cfg["random_seed"])
    np.random.seed(seed + 2)  # different stream than users/products

    users = pd.read_csv(USERS_PATH)
    users["signup_ts"] = pd.to_datetime(users["signup_ts"])

    user_ids = users["user_id"].values

    # Randomly assign variants
    variant = np.random.choice(
        ["control", "treatment"],
        size=len(user_ids),
        p=[1 - TREATMENT_PROB, TREATMENT_PROB]
    )

    # Assignment timestamp: signup_ts + random delay of 0-7 days
    delay_days = np.random.randint(0, 8, size=len(user_ids))
    assignment_ts = users["signup_ts"] + pd.to_timedelta(delay_days, unit="D")

    df = pd.DataFrame({
        "experiment_name": EXPERIMENT_NAME,
        "user_id": user_ids,
        "variant": variant,
        "assignment_ts": assignment_ts,
    })

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df["variant"].value_counts())
    print(df.head())


if __name__ == "__main__":
    main()
