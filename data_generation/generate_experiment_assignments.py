# ============================================================
# Synthetic Experiment Assignment Generator
#
# This script creates a user-level experiment assignment table
# for an A/B test (control vs treatment).
#
# Output:
#   data/raw/experiment_assignments.csv
#
# Each user receives exactly one assignment, which ensures:
# - clean randomization
# - stable cohort membership across sessions/events
# - correct experiment analysis downstream
# ============================================================

import os
import yaml
import numpy as np
import pandas as pd

# ------------------------------------------------------------
# File paths
# ------------------------------------------------------------
CONFIG_PATH = "config/base.yaml"                 # config source of randomness seed
USERS_PATH = "data/raw/users.csv"               # users generated earlier
OUTPUT_PATH = "data/raw/experiment_assignments.csv"  # output assignment file

# ------------------------------------------------------------
# Experiment settings (A/B test definition)
# ------------------------------------------------------------
EXPERIMENT_NAME = "pdp_redesign_experiment"

# Probability that a user is assigned to treatment.
# With 0.5 → 50/50 split.
TREATMENT_PROB = 0.5


def load_base_config(path: str) -> dict:
    """
    Load YAML configuration.
    We use this mainly to retrieve random_seed so results
    are reproducible across runs.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    # --------------------------------------------------------
    # Deterministic randomness
    # --------------------------------------------------------
    cfg = load_base_config(CONFIG_PATH)
    seed = int(cfg["random_seed"])

    # Offset seed so assignments use a different random stream
    # than other generators (users/products/sessions/events).
    np.random.seed(seed + 2)

    # --------------------------------------------------------
    # Load user population
    # --------------------------------------------------------
    users = pd.read_csv(USERS_PATH)

    # Ensure signup timestamp is parsed correctly.
    # We use this to generate a realistic assignment timestamp.
    users["signup_ts"] = pd.to_datetime(users["signup_ts"])

    user_ids = users["user_id"].values

    # --------------------------------------------------------
    # Randomization (A/B split)
    #
    # For each user:
    # - assign either "control" or "treatment"
    # - distribution controlled by TREATMENT_PROB
    # --------------------------------------------------------
    variant = np.random.choice(
        ["control", "treatment"],
        size=len(user_ids),
        p=[1 - TREATMENT_PROB, TREATMENT_PROB]
    )

    # --------------------------------------------------------
    # Assignment timestamp generation
    #
    # For realism, we do not assign every user at the same time.
    # We simulate assignment occurring sometime after signup.
    #
    # assignment_ts = signup_ts + random delay between 0 and 7 days
    # --------------------------------------------------------
    delay_days = np.random.randint(0, 8, size=len(user_ids))
    assignment_ts = users["signup_ts"] + pd.to_timedelta(delay_days, unit="D")

    # --------------------------------------------------------
    # Build assignment dataframe
    #
    # Output schema:
    # - experiment_name: string constant
    # - user_id: user identifier (randomization unit)
    # - variant: control vs treatment
    # - assignment_ts: when assignment became active
    # --------------------------------------------------------
    df = pd.DataFrame({
        "experiment_name": EXPERIMENT_NAME,
        "user_id": user_ids,
        "variant": variant,
        "assignment_ts": assignment_ts,
    })

    # --------------------------------------------------------
    # Write output
    # --------------------------------------------------------
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    # --------------------------------------------------------
    # Basic sanity logging
    # --------------------------------------------------------
    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df["variant"].value_counts())
    print(df.head())


if __name__ == "__main__":
    main()
