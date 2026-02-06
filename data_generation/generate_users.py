import os
import yaml
import numpy as np
import pandas as pd

# Global Configuration: Define file locations and volume of data to generate
CONFIG_PATH = "config/base.yaml"
OUTPUT_PATH = "data/raw/users.csv"
N_USERS = 1000

# Metadata Options: Categorical values that will be assigned to users
COUNTRIES = ["US", "IN", "UK", "DE", "CA"]
DEVICE_TYPES = ["mobile", "desktop"]
# Probability weights: 65% of users will be mobile, 35% desktop
DEVICE_PROBS = [0.65, 0.35]

def load_base_config(path: str) -> dict:
    """
    Utility function to safely read the project's YAML configuration.
    Returns a dictionary containing dates and random seeds.
    """
    with open(path, "r") as f:
        return yaml.safe_load(f)

def main():
    # 1. Setup and Config Loading
    cfg = load_base_config(CONFIG_PATH)

    # Extract timeframe from the config file to define the "world" of our data
    start_date = pd.to_datetime(cfg["data"]["start_date"])
    end_date = pd.to_datetime(cfg["data"]["end_date"])

    # Set the random seed for reproducibility (ensures identical results every run)
    seed = int(cfg["random_seed"])
    np.random.seed(seed)

    # 2. Time-Based Data Generation
    # Create a range of every possible date between the start and end points
    all_days = pd.date_range(start_date, end_date, freq="D")

    # Randomly pick 1000 dates from that range to act as 'Signup' dates
    signup_ts = pd.to_datetime(
        np.random.choice(all_days, size=N_USERS, replace=True)
    )

    # 3. User Identity and Attribute Generation
    # Generate sequential IDs from 1 to 1000
    user_id = np.arange(1, N_USERS + 1)
    
    # Assign a country to each user (uniform distribution - equal chance for all)
    country = np.random.choice(COUNTRIES, size=N_USERS, replace=True)
    
    # Assign a device using the weighted probability defined above
    device_type = np.random.choice(DEVICE_TYPES, size=N_USERS, p=DEVICE_PROBS)

    # 4. Feature Engineering (Custom Logic)
    # Define a 'New User' as anyone who signed up within the first 30 days of the data start
    is_new_user = signup_ts <= (start_date + pd.Timedelta(days=30))

    # 5. DataFrame Construction
    # Package all generated arrays into a tabular format
    df = pd.DataFrame({
        "user_id": user_id,
        "signup_ts": signup_ts,
        "country": country,
        "device_type": device_type,
        "is_new_user": is_new_user.astype(bool),
    })

    # 6. File Export
    # Create the output folder if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # Save the dataframe to a CSV file (without the index column)
    df.to_csv(OUTPUT_PATH, index=False)

    # Final Verification printout
    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df.head())

if __name__ == "__main__":
    main()