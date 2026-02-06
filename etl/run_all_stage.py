# This script is the Staging Orchestrator. It acts as a master controller that runs several 
# individual scripts in a specific sequence to prepare your initial dataset. In a real data pipeline, 
# this is the "Airflow" or "Job Scheduler" equivalent

from __future__ import annotations

# Import the 'main' functions from each specific staging script.
# These scripts are responsible for creating the initial raw data files.
from etl.stage_users import main as stage_users
from etl.stage_products import main as stage_products
from etl.stage_experiment_assignments import main as stage_assignments
from etl.stage_sessions import main as stage_sessions


def main():
    """
    Orchestrates the execution of all staging tasks in the correct order.
    """
    
    # 1. Generate the User base (IDs, signup dates, countries)
    stage_users()
    
    # 2. Generate the Product catalog (IDs, categories, price buckets)
    stage_products()
    
    # 3. Assign Users to Experiment Groups (Control vs. Test)
    # This relies on the Users being generated first.
    stage_assignments()
    
    # 4. Generate User Sessions (Logins, durations, device types)
    # This uses User data to ensure session dates happen after signup dates.
    stage_sessions()

    print("✅ All staging complete.")


if __name__ == "__main__":
    # Standard entry point to execute the pipeline locally
    main()