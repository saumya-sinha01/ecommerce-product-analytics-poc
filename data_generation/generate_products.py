import os
import yaml
import numpy as np
import pandas as pd

# Global Constants: Define file paths and basic generation parameters
CONFIG_PATH = "config/base.yaml"
OUTPUT_PATH = "data/raw/products.csv"
N_PRODUCTS = 200

# List of possible categories for the synthetic products
CATEGORIES = ["Electronics", "Apparel", "Beauty", "Home", "Sports", "Grocery"]


def load_base_config(path: str) -> dict:
    """
    Reads a YAML configuration file from the specified path.
    Returns the content as a Python dictionary.
    """
    with open(path, "r") as f:
        # safe_load converts YAML syntax into Python objects (dicts/lists)
        return yaml.safe_load(f)


def main():
    # 1. Setup and Reproducibility
    cfg = load_base_config(CONFIG_PATH)
    seed = int(cfg["random_seed"])
    # Set the random seed to ensure the data is the same every time you run it
    # Adding +1 creates a unique sequence separate from other scripts using the base seed
    np.random.seed(seed + 1)

    # 2. Data Generation
    # Create an array of sequential IDs from 1 to 200
    product_id = np.arange(1, N_PRODUCTS + 1)
    
    # Randomly assign a category to each product from the CATEGORIES list
    category = np.random.choice(CATEGORIES, size=N_PRODUCTS, replace=True)

    # 3. Logic-based Price Generation
    # Pick a "price tier" for each product based on specific probabilities
    # 60% chance for low, 30% for mid, 10% for high
    bucket = np.random.choice(["low", "mid", "high"], size=N_PRODUCTS, p=[0.6, 0.3, 0.1])
    
    base_price = []
    for b in bucket:
        # Assign a random float within a specific range based on the chosen bucket
        if b == "low":
            base_price.append(np.random.uniform(5.99, 39.99))
        elif b == "mid":
            base_price.append(np.random.uniform(40.00, 149.99))
        else:
            base_price.append(np.random.uniform(150.00, 499.99))

    # 4. Data Structuring
    # Combine the lists into a structured DataFrame (table)
    df = pd.DataFrame({
        "product_id": product_id,
        "category": category,
        "base_price": np.round(base_price, 2), # Round to 2 decimal places for currency
    })

    # 5. Output and File Handling
    # Create the 'data/raw' directory if it doesn't exist to avoid errors
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    # Save the generated table to a CSV file without including the row index numbers
    df.to_csv(OUTPUT_PATH, index=False)

    # Logging output to the console
    print("✅ Wrote:", OUTPUT_PATH)
    print("Rows:", len(df))
    print(df.head()) # Preview the first 5 rows of the generated data


if __name__ == "__main__":
    # Standard Python entry point to run the main function
    main()