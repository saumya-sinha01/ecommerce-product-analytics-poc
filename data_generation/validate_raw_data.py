import pandas as pd

# Define paths to all generated CSV files
USERS_PATH = "data/raw/users.csv"
PRODUCTS_PATH = "data/raw/products.csv"
SESSIONS_PATH = "data/raw/sessions.csv"
EVENTS_PATH = "data/raw/events.csv"

def main():
    # 1. Loading the Data
    users = pd.read_csv(USERS_PATH)
    products = pd.read_csv(PRODUCTS_PATH)
    sessions = pd.read_csv(SESSIONS_PATH)
    events = pd.read_csv(EVENTS_PATH)

    # Convert timestamp strings into actual Datetime objects for mathematical comparison
    sessions["session_start_ts"] = pd.to_datetime(sessions["session_start_ts"])
    sessions["session_end_ts"] = pd.to_datetime(sessions["session_end_ts"])
    events["event_ts"] = pd.to_datetime(events["event_ts"])

    # 2. Foreign Key (FK) Validation
    # We turn the IDs into "sets" for lightning-fast lookup performance
    user_set = set(users["user_id"])
    session_set = set(sessions["session_id"])
    product_set = set(products["product_id"])

    # Check for "Orphan" records: events that reference IDs that don't exist in the parent files
    bad_users = events.loc[~events["user_id"].isin(user_set)]
    bad_sessions = events.loc[~events["session_id"].isin(session_set)]
    
    # Product check is special: only check if product_id is not empty (null)
    bad_products = events.loc[
        events["product_id"].notna() & ~events["product_id"].isin(product_set)
    ]

    print("FK check:")
    print("  bad user refs:", len(bad_users))
    print("  bad session refs:", len(bad_sessions))
    print("  bad product refs:", len(bad_products))

    # 3. Temporal (Time) Validation
    # Combine Event data with Session start/end times based on the 'session_id'
    merged = events.merge(
        sessions[["session_id", "session_start_ts", "session_end_ts"]],
        on="session_id",
        how="left"
    )

    # Find events that happened BEFORE the session started or AFTER the session ended
    outside = merged.loc[
        (merged["event_ts"] < merged["session_start_ts"]) |
        (merged["event_ts"] > merged["session_end_ts"])
    ]
    print("Time window check:")
    print("  events outside session window:", len(outside))

    # 4. Business Logic Consistency
    # Identify which rows are meant to be purchases
    is_purchase = events["event_type"] == "purchase"
    
    # ERROR 1: It's a purchase, but financial info (price/qty/discount) is missing
    purchase_missing = events.loc[
        is_purchase & (
            events["price_paid"].isna() |
            events["quantity"].isna() |
            events["discount_amount"].isna()
        )
    ]
    
    # ERROR 2: It's NOT a purchase (e.g., 'view'), but it somehow has price/qty data
    nonpurchase_has_price = events.loc[
        (~is_purchase) & (
            events["price_paid"].notna() |
            events["quantity"].notna() |
            events["discount_amount"].notna()
        )
    ]

    print("Purchase field checks:")
    print("  purchase rows missing price/qty/discount:", len(purchase_missing))
    print("  non-purchase rows with price/qty/discount:", len(nonpurchase_has_price))

    # 5. Final Statistics
    print("\nRow counts:")
    print("  users:", len(users))
    print("  products:", len(products))
    print("  sessions:", len(sessions))
    print("  events:", len(events))


if __name__ == "__main__":
    main()