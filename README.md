# Ecommerce Product Analytics & Experimentation Platform (LocalStack S3 + Postgres)

## What this project is
An end-to-end **experiment analytics POC** that simulates an ecommerce **PDP redesign A/B test** using synthetic data, then runs a realistic analytics pipeline:
1) generate raw CSVs → 2) upload to **LocalStack S3** → 3) ETL to Parquet → 4) build analytics marts → 5) load marts to **Postgres** → 6) run **Python** A/B testing + regression.

This README documents **exactly what we built and executed** (no extra features beyond what exists in the repo).

---

## Questions we set out to answer (and what we actually answered)
### Q1) Does the new PDP design increase purchase conversion?
✅ **We computed conversion rates** for control vs treatment and estimated lift.

### Q2) Is the improvement statistically significant?
✅ **Yes, we performed**:
- two-proportion z-test (hypothesis test)
- 95% confidence interval for lift

Result (from `analysis.ab_analysis`):
- control conversion ≈ 0.0424
- treatment conversion ≈ 0.0487
- lift ≈ 0.0063
- p-value ≈ 0.6463 (not significant at 0.05)
- 95% CI ≈ (-0.0206, 0.0333)

### Q3) Negative side effects (bounce/retention/session duration)?
❌ Not implemented in this phase.

### Q4) Segment-level effects (mobile vs desktop)?
❌ Not implemented in this phase.

---

## Metrics implemented (what exists today)
### Primary decision metric
- **Purchase conversion rate** (user-level)

### Additional metrics used
- **Revenue per user (RPU)**
- **events_in_window** (engagement proxy) used in regression

---

## High-level architecture (implemented)
```
data_generation/*.py  ->  data/raw/*.csv
        |
        v
LocalStack S3 (bucket: ecom-poc-raw, prefix: raw/)
        |
        v
ETL staging (Parquet) -> s3://ecom-poc-processed/processed/staged/...
        |
        v
Clean events (Parquet, partitioned) -> s3://ecom-poc-processed/processed/events_clean/...
        |
        v
Gold marts (Parquet) -> s3://ecom-poc-processed/processed/marts/...
        |
        v
Postgres tables -> mart_user_exposure, mart_user_outcomes
        |
        v
Python analysis -> A/B test + CI + logistic regression
```

---

## Repo structure (as committed)
```
config/
  base.yaml

data_generation/
  generate_users.py
  generate_products.py
  generate_experiment_assignments.py
  generate_sessions.py
  generate_events.py
  validate_raw_data.py

etl/
  __init__.py
  config.py
  io_s3.py
  run_all_stage.py
  stage_users.py
  stage_products.py
  stage_sessions.py
  stage_experiment_assignments.py
  raw_to_clean_events.py
  build_marts.py
  load_marts_to_postgres.py

analysis/
  stats_utils.py
  ab_analysis.py
  regression_analysis.py

infrastructure/
  docker-compose.yml
  localstack/create_buckets.ps1

docs/
  EXPERIMENT_DECISION.md
```

---

## Prerequisites
- Python env (conda/venv)
- Docker Desktop
- AWS CLI installed (configured to talk to LocalStack via `--endpoint-url`)
- Python packages:
  - pandas, numpy, pyyaml, boto3, pyarrow, sqlalchemy, psycopg2
  - statsmodels (for regression)

---

## Configuration (what we used)
All configuration lives in:
- `config/base.yaml`

Key items:
- LocalStack endpoint: `http://localhost:4566`
- buckets:
  - raw: `ecom-poc-raw`
  - processed: `ecom-poc-processed`
- raw paths (CSV keys): users/products/sessions/assignments/events
- schema mapping (notably `event_name` mapped from CSV `event_type`)
- mart event names:
  - exposure_event: `view_product`
  - add_to_cart: `add_to_cart`
  - begin_checkout: `begin_checkout`
  - purchase: `purchase`

---

## End-to-end: commands to run everything (from scratch)

> Run these from repo root: `E:\ecommerce-product-analytics-poc`

### 0) Start infra (Postgres + LocalStack)
```bash
docker compose -f infrastructure/docker-compose.yml up -d
docker ps
```

Health check (PowerShell):
```powershell
Invoke-RestMethod http://127.0.0.1:4566/_localstack/health
```

### 1) Create S3 buckets (PowerShell)
```powershell
.\infrastructure\localstack\create_buckets.ps1
```

Verify:
```powershell
aws --endpoint-url=http://127.0.0.1:4566 s3 ls
```

### 2) Generate synthetic raw CSVs
```bash
python data_generation/generate_users.py
python data_generation/generate_products.py
python data_generation/generate_experiment_assignments.py
python data_generation/generate_sessions.py
python data_generation/generate_events.py
python data_generation/validate_raw_data.py
```

### 3) Upload raw CSVs to LocalStack S3
```powershell
aws --endpoint-url=http://127.0.0.1:4566 s3 cp data/raw s3://ecom-poc-raw/raw --recursive
```

### 4) Stage (CSV -> Parquet) to processed bucket
```bash
python -m etl.run_all_stage
```

Expected output includes staged datasets written to:
- `s3://ecom-poc-processed/processed/staged/users/users.parquet`
- `.../products/products.parquet`
- `.../experiment_assignments/experiment_assignments.parquet`
- `.../sessions/sessions.parquet`

### 5) Clean events (normalize + derived fields)
```bash
python -m etl.raw_to_clean_events
```

This produces partitioned clean events under:
- `s3://ecom-poc-processed/processed/events_clean/...`

### 6) Build marts (gold layer)
```bash
python -m etl.build_marts
```

Marts written to:
- `processed/marts/user_exposure/user_exposure.parquet`
- `processed/marts/user_outcomes/user_outcomes.parquet`

### 7) Load marts to Postgres
```bash
python -m etl.load_marts_to_postgres
```

Expected console output we saw:
- Loaded parquet rows=920 (exposure/outcomes)
- Postgres rows verified for both tables
- Variant split printed

### 8) Run Python A/B testing
```bash
python -m analysis.ab_analysis
```

This prints:
- conversion summary
- z-test (p-value)
- 95% CI for lift

### 9) Run Python regression analysis
```bash
python -m analysis.regression_analysis
```

We fit logistic regression:
- purchased ~ is_treatment + events_in_window

And observed:
- is_treatment coefficient positive but not significant
- events_in_window strongly significant predictor

---

## What we actually built in ETL (details)

### Staging layer
- `stage_users.py`: casts IDs, parses timestamps
- `stage_products.py`: casts product_id, numeric price
- `stage_sessions.py`: parses session timestamps, validates keys
- `stage_experiment_assignments.py`: validates 50/50 split, timestamps
- `run_all_stage.py`: orchestrates all staging scripts

### Clean events
- `raw_to_clean_events.py`:
  - reads raw `events.csv`
  - applies schema mapping (`event_type` -> `event_name`)
  - normalizes event names (aliases)
  - adds:
    - `dt` partition column
    - `is_purchase`
    - `net_revenue`

### Marts
- `build_marts.py` produces:
  - **user_exposure**: exposure timestamp + variant per user
  - **user_outcomes**: purchase, revenue, events_in_window within outcome window

### Postgres load
- `load_marts_to_postgres.py` loads marts into:
  - `mart_user_exposure`
  - `mart_user_outcomes`

---

## Challenges we faced (and how we fixed them)

### 1) PowerShell `chmod` / WSL bash issues
- `chmod` failed in Windows PowerShell (expected).
- WSL error: `/bin/bash` missing.
✅ Fixed by using a PowerShell script: `create_buckets.ps1`.

### 2) LocalStack endpoint confusion (`localhost` vs `127.0.0.1`)
- Curl/requests in Windows sometimes behaved differently with `localhost`.
✅ We verified health via:
- `http://127.0.0.1:4566/_localstack/health`
and used `--endpoint-url=http://127.0.0.1:4566` in AWS CLI when needed.

### 3) Python boto3 saw no buckets (`list_buckets` returned [])
- Root cause: endpoint mismatch / bucket creation not in the same endpoint context.
✅ Re-created buckets using the same endpoint, then staging succeeded.

### 4) events column mismatch (`event_name` vs `event_type`)
- Raw CSV uses `event_type`.
- ETL expected `event_name`.
✅ Fixed using schema mapping in `base.yaml`:
- `schema.events.event_name: "event_type"`

### 5) Exposure event mismatch caused “No exposure rows found”
- Mart expected `pdp_view` or `product_view` but actual events were `view_product`.
✅ Updated mart event name to `view_product` (consistent with generated events).

### 6) Regression `Singular matrix`
- Too much collinearity in earlier model attempt.
✅ Simplified model to:
- purchased ~ is_treatment + events_in_window
which converged successfully.

---

## Outputs produced (confirmed)
- S3 buckets created: `ecom-poc-raw`, `ecom-poc-processed`
- Staged parquet written to processed bucket
- Marts created:
  - `processed/marts/user_exposure/user_exposure.parquet`
  - `processed/marts/user_outcomes/user_outcomes.parquet`
- Postgres tables loaded:
  - `mart_user_exposure`
  - `mart_user_outcomes`
- Python A/B analysis and regression executed end-to-end

---

## What to look at first
1) `config/base.yaml` (schema + event names + S3 paths)
2) `data_generation/generate_events.py` (funnel + uplift)
3) `etl/raw_to_clean_events.py` (normalization + derived fields)
4) `etl/build_marts.py` (exposure/outcomes logic)
5) `analysis/ab_analysis.py` and `analysis/regression_analysis.py` (statistics)

---

## Notes
- `.gitignore` is committed to avoid checking in raw CSVs/parquet artifacts.
- A temporary debug script (`fix_events_csv.py`) was explicitly not committed (and ignored).

