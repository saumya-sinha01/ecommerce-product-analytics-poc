import os
import yaml
import numpy as np
import pandas as pd

CONFIG_PATH = "config/base.yaml"
OUTPUT_PATH = "data/raw/products.csv"
N_PRODUCTS = 200

CATEGORIES = ["Electronics", "Apparel", "Beauty", "Home", "Sports", "Grocery"]


def load_base_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    cfg = load_base_config(CONFIG_PATH)
    seed = int(cfg["random_seed"])
    np.random.seed(seed + 1)  # deterministic, different stream than users

    product_id = np.arange(1, N_PRODUCTS + 1)
    category = np.random.choice(CATEGORIES, size=N_PRODUCTS, replace=True)

    # Price buckets: most low/mid, few high
    bucket = np.random.choice(["low", "mid", "high"], size=N_PRODUCTS, p=[0.6, 0.3, 0.1])
    base_price = []
    for b in bucket:
        if b == "low":
            base_price.append(np.random.uniform(5.99, 39.99))
        elif b == "mid":
            base_price.append(np.random.uniform(40.00, 149.99))
        else:
            base_price.append(np.random.uniform(150.00, 499.99))

    df = pd.DataFrame({
        "product_id": product_id,
        "category": category,
        "base_price": np.round(base_price, 2),
    })

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df.head())


if __name__ == "__main__":
    main()
