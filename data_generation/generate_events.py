import os
import json
import yaml
import numpy as np
import pandas as pd

CONFIG_PATH = "config/base.yaml"
USERS_PATH = "data/raw/users.csv"
PRODUCTS_PATH = "data/raw/products.csv"
SESSIONS_PATH = "data/raw/sessions.csv"
ASSIGNMENTS_PATH = "data/raw/experiment_assignments.csv"
OUTPUT_PATH = "data/raw/events.csv"

# Funnel base rates (control)
P_VIEW_PRODUCT = 0.70
P_ADD_TO_CART_GIVEN_VIEW = 0.12
P_BEGIN_CHECKOUT_GIVEN_ATC = 0.55
P_PURCHASE_GIVEN_CHECKOUT = 0.60

# Treatment uplift (relative)
REL_LIFT_PURCHASE = 0.06  # +6% relative lift on purchase probability


def load_base_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def clamp(p: float) -> float:
    return max(0.0, min(1.0, p))


def main():
    cfg = load_base_config(CONFIG_PATH)
    seed = int(cfg["random_seed"])
    np.random.seed(seed + 4)

    users = pd.read_csv(USERS_PATH)
    products = pd.read_csv(PRODUCTS_PATH)
    sessions = pd.read_csv(SESSIONS_PATH)
    assignments = pd.read_csv(ASSIGNMENTS_PATH)

    sessions["session_start_ts"] = pd.to_datetime(sessions["session_start_ts"])
    sessions["session_end_ts"] = pd.to_datetime(sessions["session_end_ts"])
    assignments["assignment_ts"] = pd.to_datetime(assignments["assignment_ts"])

    # Map user_id -> variant
    variant_by_user = dict(zip(assignments["user_id"], assignments["variant"]))

    product_ids = products["product_id"].values
    product_price = dict(zip(products["product_id"], products["base_price"]))

    event_rows = []
    event_id = 1

    for _, s in sessions.iterrows():
        user_id = int(s["user_id"])
        session_id = int(s["session_id"])
        start_ts = s["session_start_ts"]
        end_ts = s["session_end_ts"]

        variant = variant_by_user.get(user_id, "control")

        # Helper to generate timestamps within session
        def t(offset_seconds: int) -> pd.Timestamp:
            ts = start_ts + pd.to_timedelta(offset_seconds, unit="s")
            return min(ts, end_ts)

        sec = 0

        # Always emit session_start
        event_rows.append({
            "event_id": str(event_id),
            "event_ts": t(sec),
            "user_id": user_id,
            "session_id": session_id,
            "product_id": None,
            "event_type": "session_start",
            "price_paid": None,
            "quantity": None,
            "discount_amount": None,
            "properties": None
        })
        event_id += 1
        sec += np.random.randint(5, 20)

        # Optional: view_home
        if np.random.rand() < 0.50:
            event_rows.append({
                "event_id": str(event_id),
                "event_ts": t(sec),
                "user_id": user_id,
                "session_id": session_id,
                "product_id": None,
                "event_type": "view_home",
                "price_paid": None,
                "quantity": None,
                "discount_amount": None,
                "properties": None
            })
            event_id += 1
            sec += np.random.randint(5, 25)

        # Optional: search
        if np.random.rand() < 0.35:
            props = {"query_len": int(np.random.randint(2, 15))}
            event_rows.append({
                "event_id": str(event_id),
                "event_ts": t(sec),
                "user_id": user_id,
                "session_id": session_id,
                "product_id": None,
                "event_type": "search",
                "price_paid": None,
                "quantity": None,
                "discount_amount": None,
                "properties": json.dumps(props)
            })
            event_id += 1
            sec += np.random.randint(5, 25)

        # Decide if user views a product
        if np.random.rand() < P_VIEW_PRODUCT:
            pid = int(np.random.choice(product_ids))
            event_rows.append({
                "event_id": str(event_id),
                "event_ts": t(sec),
                "user_id": user_id,
                "session_id": session_id,
                "product_id": pid,
                "event_type": "view_product",
                "price_paid": None,
                "quantity": None,
                "discount_amount": None,
                "properties": None
            })
            event_id += 1
            sec += np.random.randint(5, 30)

            # Add to cart
            if np.random.rand() < P_ADD_TO_CART_GIVEN_VIEW:
                event_rows.append({
                    "event_id": str(event_id),
                    "event_ts": t(sec),
                    "user_id": user_id,
                    "session_id": session_id,
                    "product_id": pid,
                    "event_type": "add_to_cart",
                    "price_paid": None,
                    "quantity": None,
                    "discount_amount": None,
                    "properties": None
                })
                event_id += 1
                sec += np.random.randint(5, 30)

                # Begin checkout
                if np.random.rand() < P_BEGIN_CHECKOUT_GIVEN_ATC:
                    event_rows.append({
                        "event_id": str(event_id),
                        "event_ts": t(sec),
                        "user_id": user_id,
                        "session_id": session_id,
                        "product_id": pid,
                        "event_type": "begin_checkout",
                        "price_paid": None,
                        "quantity": None,
                        "discount_amount": None,
                        "properties": None
                    })
                    event_id += 1
                    sec += np.random.randint(5, 40)

                    # Purchase probability (uplift for treatment)
                    p_purchase = P_PURCHASE_GIVEN_CHECKOUT
                    if variant == "treatment":
                        p_purchase = clamp(p_purchase * (1.0 + REL_LIFT_PURCHASE))

                    if np.random.rand() < p_purchase:
                        qty = int(np.random.choice([1, 1, 1, 2, 2, 3]))
                        base = float(product_price[pid])
                        discount = float(np.random.choice([0.0, 0.0, 0.0, 5.0, 10.0]))
                        paid = max(0.0, base * qty - discount)

                        event_rows.append({
                            "event_id": str(event_id),
                            "event_ts": t(sec),
                            "user_id": user_id,
                            "session_id": session_id,
                            "product_id": pid,
                            "event_type": "purchase",
                            "price_paid": round(paid, 2),
                            "quantity": qty,
                            "discount_amount": round(discount, 2),
                            "properties": None
                        })
                        event_id += 1
                        sec += np.random.randint(5, 30)

        # Optional: logout
        if np.random.rand() < 0.20:
            event_rows.append({
                "event_id": str(event_id),
                "event_ts": t(sec),
                "user_id": user_id,
                "session_id": session_id,
                "product_id": None,
                "event_type": "logout",
                "price_paid": None,
                "quantity": None,
                "discount_amount": None,
                "properties": None
            })
            event_id += 1

    df = pd.DataFrame(event_rows)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df["event_type"].value_counts())
    print(df.head())


if __name__ == "__main__":
    main()
