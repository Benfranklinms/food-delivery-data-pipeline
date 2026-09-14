# Sales Pipeline

End-to-end ETL + analytics pipeline for a food-delivery order history dataset, orchestrated with Apache Airflow.

Raw CSV → Airflow (extract → transform → validate → load → analytics) → SQLite (`food_delivery.db`) + 10 analytical queries.

## Overview

This project demonstrates a complete data pipeline built for learning and portfolio use:

- **Source:** `data/raw/order_history.csv` (21,321 orders, 29 raw columns, Zomato-style food delivery data)
- **Orchestration:** Apache Airflow 3.3.1 (TaskFlow API, `apache/airflow:3.3.1`)
- **Storage:** SQLite for the analytical warehouse, Postgres 16 for Airflow metadata
- **Transform:** pandas with strict validation, de-duplication, type coercion and derived fields
- **Analytics:** 10 window-function and aggregation queries in `scripts/analytics.sql` executed as the final DAG task

Last verified run: 5/5 tasks Success (`extract`, `transform`, `validate`, `load`, `analytics`) — 21,321 rows loaded into `orders`.

## Architecture

```
data/raw/order_history.csv
        |
     extract  ──> data/intermediate/extracted_orders.csv  (raw dump, 29 cols)
        |
     transform ──> data/intermediate/cleaned_orders.csv   (selected + cleaned, 24 cols)
        |
     validate  ──> checks schema, duplicates, nulls, negatives (pass/fail gate)
        |
     load      ──> data/food_delivery.db  table=orders  + indexes
        |
     analytics ──> reads scripts/analytics.sql, executes 10 SELECTs against SQLite, logs results
```

Airflow infrastructure:

```
docker-compose.yaml
  postgres:16  (metadata DB, healthcheck pg_isready)
  airflow:     build . (Dockerfile) → apache/airflow:3.3.1 + requirements.txt
               volumes: ./dags:/opt/airflow/dags
                        ./data:/opt/airflow/data
                        ./scripts:/opt/airflow/scripts
                        ./logs:/opt/airflow/logs
               command: airflow standalone
               executor: LocalExecutor
```

All DAG file paths are absolute inside the container via `BASE_DIR = Path("/opt/airflow")` (`dags/pipeline.py:12`).

## Features

- **Idempotent and reproducible:** intermediate directories created with `mkdir(parents=True)`, pinned dependencies (`pandas==2.3.3`, `pendulum>=3.0`, `pyarrow>=15.0` in `requirements.txt:5-7`)
- **Strict validation gate:** `validate` fails the run on empty frame, missing required columns, duplicate `order_id`, negative `total`, or nulls in critical fields (`dags/pipeline.py:184-236`)
- **Robust transform:** NaN-preserving string trimming (`dags/pipeline.py:95`), `pd.to_datetime(..., errors="coerce")`, `<1km → 0.5` distance parsing via `re.search` with `match.group(1)` (`dags/pipeline.py:101-114`), `pd.to_numeric(..., errors="coerce")`, derived `order_date`, `order_month` (YYYY-MM string), `distance_km`, `total_pre_delivery_minutes` (`dags/pipeline.py:175`)
- **Analytics as code:** `scripts/analytics.sql` is versioned and executed by the DAG, not manually — ensures lineage `LOAD → ANALYTICS` via XCom (`dags/pipeline.py:348-350` `db_path = load(...); analytics(db_path)`)
- **Backwards-compatible analytics task:** `analytics(db_path: str | None = None)` falls back to `DATABASE_FILE` for old DagRuns where `load` previously returned `None` (`dags/pipeline.py:296-297`)

## Project Structure

```
sales pipeline/
├── dags/
│   └── pipeline.py          # Airflow DAG: sales_pipeline (TaskFlow, 5 tasks)
├── data/
│   ├── raw/
│   │   └── order_history.csv      # 21,342 lines (21,321 data rows, 29 cols)
│   ├── intermediate/              # gitignored, created at runtime
│   │   ├── extracted_orders.csv   # 6.3M, raw dump
│   │   └── cleaned_orders.csv     # 6.0M, 24 cols after transform
│   └── food_delivery.db           # gitignored, SQLite warehouse, table orders
├── scripts/
│   └── analytics.sql        # 10 analytics queries (see below)
├── docker-compose.yaml      # postgres + airflow services
├── Dockerfile               # FROM apache/airflow:3.3.1 + COPY requirements.txt
├── requirements.txt         # pinned: pandas==2.3.3, pendulum>=3.0, pyarrow>=15.0
├── .gitignore               # venv/, logs/, data/intermediate/, *.db, .DS_Store
└── README.md                # this file
```

Airflow standard: DAGs live in `dags/` (plural). The repository previously had a duplicate `dag/` (singular); it has been consolidated to `dags/` via `git mv dag/pipeline.py dags/pipeline.py`.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
- Git
- Optional local dev: Python 3.12+, `pip`

Ports: Airflow UI on `8080`.

## Quick Start (Docker — recommended)

```bash
# 1. Clone and enter
git clone <repo-url>
cd "sales pipeline"

# 2. Start
docker compose up -d

# 3. Follow logs (first start builds image and runs airflow standalone)
docker compose logs -f airflow
# Wait for: "Airflow ready" / "standalone | Airflow is ready"

# 4. Open UI
open http://localhost:8080
# Login: airflow / airflow  (created by airflow standalone)
```

Trigger the pipeline:

- UI: **DAGs → sales_pipeline → Trigger DAG** (button top-right)
- CLI:

```bash
docker exec $(docker ps -q --filter name=airflow | head -1) airflow dags trigger sales_pipeline
```

Watch the run:

```bash
docker exec $(docker ps -q --filter name=airflow | head -1) ls -R /opt/airflow/logs/dag_id=sales_pipeline
# or locally, bind mount:
ls -R logs/dag_id=sales_pipeline
```

Verify outputs:

```bash
ls -lh data/intermediate/ data/food_delivery.db
sqlite3 data/food_delivery.db "SELECT COUNT(*) FROM orders;"
# expected: 21321

sqlite3 data/food_delivery.db < scripts/analytics.sql | head -n 20
docker exec $(docker ps -q --filter name=airflow | head -1) ls -lh /opt/airflow/data/food_delivery.db
```

Stop:

```bash
docker compose down        # keep volumes (DB + Airflow metadata)
docker compose down -v     # full reset (drops postgres_data, intermediate files remain on host)
```

Rebuild after editing `Dockerfile` or `requirements.txt`:

```bash
docker compose down
docker compose up --build -d
```

No rebuild needed for edits to `dags/pipeline.py` or `scripts/analytics.sql` — they are bind-mounted and hot-reloaded by the DAG processor within seconds.

## Local Development (without Docker)

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# pendulum is required for dags/pipeline.py:8 import
pip install "apache-airflow==3.3.1"  # if you want to parse DAG locally

python -m py_compile dags/pipeline.py
python -c "import yaml; yaml.safe_load(open('docker-compose.yaml'))"

# Run transform logic manually (replicates container paths with host BASE_DIR override):
python3 << 'PY'
from pathlib import Path
import pandas as pd
# Note: inside container BASE_DIR is /opt/airflow, locally override if needed
# This snippet just validates pipeline.py imports and runs a dry transform
import dags.pipeline  # if airflow installed
print("DAG id:", dags.pipeline.sales_pipeline.dag_id)
PY
```

## Pipeline Details

### DAG definition — `dags/pipeline.py:45-352`

```python
@dag(
    dag_id="sales_pipeline",
    schedule=None,  # manual trigger only
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["sales", "learning", "food-delivery", "etl"],
)
def sales_pipeline():
    extracted = extract()              # -> str path
    transformed = transform(extracted) # -> str path
    validated = validate(transformed)  # -> str path (gate)
    db_path = load(validated)          # -> str DATABASE_FILE
    analytics(db_path)                 # reads scripts/analytics.sql
```

### 1. extract — `dags/pipeline.py:52-67`

- Checks `RAW_FILE:14` exists, else `FileNotFoundError`
- `pd.read_csv(RAW_FILE)` → logs `Extracted 21321 rows and 29 columns` at `dags/pipeline.py:61`
- Writes `BASE_DIR/data/intermediate/extracted_orders.csv` with `mkdir(parents=True)` at `dags/pipeline.py:64`
- Returns path via XCom

### 2. transform — `dags/pipeline.py:69-182`

- Input: `extracted_orders.csv` (29 cols)
- Validates `SELECTED_COLUMNS:21-42` (20 cols: Restaurant ID/City/Order ID/etc.) — raises `ValueError` if any missing at `dags/pipeline.py:76`
- `df[SELECTED_COLUMNS].copy()` → `drop_duplicates()` → `drop_duplicates(subset=["Order ID"])` at `dags/pipeline.py:81`
- Text columns NaN-preserving trim: `df[col].apply(lambda x: str(x).strip() if pd.notna(x) else x)` for `Restaurant name, Subzone, City, Order Status, Delivery, Items in order` at `dags/pipeline.py:95`
- Datetime: `pd.to_datetime(..., errors="coerce")` → derived `order_date` (date) and `order_month` (`Period("M")` → `string` YYYY-MM) at `dags/pipeline.py:97-99`
- Distance: `clean_distance` handles `<1km → 0.5`, regex `([\d.]+)` with `match.group(1)` at `dags/pipeline.py:108-113`, applied to `Distance` (pre-rename) → `distance_km` at `dags/pipeline.py:116`
- Numeric coercion: `pd.to_numeric(..., errors="coerce")` for 9 financial/duration columns at `dags/pipeline.py:130`
- Rename to snake_case at `dags/pipeline.py:133-156` (e.g., `Order ID → order_id`, `Distance → distance_raw`)
- Null warning + `dropna(subset=required_columns)` where `required = [order_id, restaurant_id, customer_id, order_placed_at, order_status, total]` at `dags/pipeline.py:167-173`
- Derived `total_pre_delivery_minutes = kpt_duration_minutes.fillna(0) + rider_wait_minutes.fillna(0)` at `dags/pipeline.py:175`
- Writes `INTERMEDIATE_FILE:15` `cleaned_orders.csv` (24 cols) with `mkdir(parents=True)` at `dags/pipeline.py:179`

### 3. validate — `dags/pipeline.py:184-236`

Gate that fails the run on bad data:

- `transformed_path.exists()` check at `dags/pipeline.py:188`
- `df.empty` → `ValueError` at `dags/pipeline.py:193`
- Missing required columns → `ValueError` at `dags/pipeline.py:210`
- Duplicate `order_id` → `duplicate_order_ids = df["order_id"][duplicated]; if not empty → ValueError` at `dags/pipeline.py:214-218`
- Negative `total` → `df[df["total"] < 0]; if not empty → ValueError` at `dags/pipeline.py:222-226` (uses `.empty`, not `.any().any()`)
- Nulls in critical fields → `critical_nulls = df[required].isna().sum().sum(); if >0 → ValueError` at `dags/pipeline.py:230-234`
- Returns `transformed_file` on success

### 4. load — `dags/pipeline.py:238-286`

- Reads `cleaned_orders.csv`, `DATABASE_FILE.parent.mkdir` at `dags/pipeline.py:242`
- `sqlite3.connect(DATABASE_FILE:17)` → `df.to_sql("orders", if_exists="replace")` at `dags/pipeline.py:246`
- Creates indexes `idx_orders_customer`, `idx_orders_restaurant`, `idx_orders_date` at `dags/pipeline.py:250-268`
- Logs `Rows loaded into SQLite: 21321` and `Database location: ...` at `dags/pipeline.py:277-279`
- Returns `str(DATABASE_FILE)` for downstream `analytics` dependency

### 5. analytics — `dags/pipeline.py:288-350`

Executes `scripts/analytics.sql` as the final step, not manually:

- Locates `ANALYTICS_SQL:19` `BASE_DIR/scripts/analytics.sql` and the DB from `load()` at `dags/pipeline.py:290,296-297`
- Fallback `Path(db_path) if db_path else DATABASE_FILE` for old DagRuns where `load` returned `None` (pre-v2)
- Splits `analytics.sql` on `;`, skips comment-only fragments at `dags/pipeline.py:312-317`, executes each statement via `sqlite3`
- For `SELECT`s, fetches rows and logs `cols | rows[:10]` and `... N more` at `dags/pipeline.py:324-333`
- Logs `Analytics completed successfully.` at `dags/pipeline.py:340`

Run logs are in `logs/dag_id=sales_pipeline/run_id=<run_id>/task_id=analytics/attempt=1.log` (also in container at `/opt/airflow/logs`).

## Analytics — `scripts/analytics.sql`

10 queries over `orders` (table `orders` in `data/food_delivery.db`). Revenue queries filter `order_status = 'Delivered'` (case-sensitive). `order_month` is string `YYYY-MM` so lexical ordering is chronological. Query 10 intentionally has no `Delivered` filter.

| # | Name | Purpose | Key logic |
|---|------|---------|-----------|
| 1 | How many orders | Row count | `COUNT(*)` |
| 2 | Total revenue | Sum delivered | `SUM(total) WHERE Delivered` |
| 3 | Revenue by city | City breakdown | `GROUP BY city ORDER BY revenue DESC` |
| 4 | Revenue by month | Monthly trend | `GROUP BY order_month ORDER BY order_month` |
| 5 | Top customers | Top 10 spenders | `GROUP BY customer_id ORDER BY SUM(total) DESC LIMIT 10` |
| 6 | Rank customers | Window rank | `RANK() OVER (ORDER BY SUM(total) DESC)` |
| 7 | Most valuable per city | Per-city top | `WITH customer_city_revenue` + `ROW_NUMBER() OVER (PARTITION BY city ...)` + `WHERE rn=1` with `AS ranked` alias (Postgres-compatible) |
| 8 | Avg prep time by restaurant | Ops metric | `AVG(kpt_duration_minutes) WHERE Delivered AND NOT NULL` |
| 9 | Running revenue by month | Cumulative | `SUM(monthly_revenue) OVER (ORDER BY order_month ROWS UNBOUNDED PRECEDING)` |
| 10 | Delivery performance | Distance/time by delivery type | `AVG(distance_km), AVG(total_pre_delivery_minutes) GROUP BY delivery_type` (all statuses) |

Run manually:

```bash
sqlite3 data/food_delivery.db < scripts/analytics.sql
# or per query inside container:
docker exec $(docker ps -q --filter name=airflow | head -1) sqlite3 /opt/airflow/data/food_delivery.db < /opt/airflow/scripts/analytics.sql
```

## Data

- **Raw:** `data/raw/order_history.csv` — 21,342 lines total (header + 21,341 rows, but after `SELECTED_COLUMNS` + de-duplication = 21,321 final rows), 29 columns including `Restaurant ID, Restaurant name, Subzone, City, Order ID, Order Placed At, Order Status, Delivery, Distance, Items in order, Bill subtotal, Packaging charges, Restaurant discount (Promo), Gold discount, Brand pack discount, Total, Rating, KPT duration (minutes), Rider wait time (minutes), Customer ID` plus unused `Instructions, Discount construct, Restaurant discount (Flat offs...), Review, Cancellation reason, compensation, penalty, Order Ready Marked, complaint tag`
- **Intermediate:** `data/intermediate/` (gitignored) — `extracted_orders.csv` (29 cols) and `cleaned_orders.csv` (24 cols: 20 selected + `order_date, order_month, distance_km, total_pre_delivery_minutes`)
- **Warehouse:** `data/food_delivery.db` (gitignored) — single table `orders` with indexes on `customer_id`, `restaurant_id`, `order_date` (`dags/pipeline.py:250-268`)

To inspect:

```bash
sqlite3 data/food_delivery.db "SELECT sql FROM sqlite_master WHERE type='table' AND name='orders';"
sqlite3 data/food_delivery.db "SELECT * FROM orders LIMIT 2;"
sqlite3 data/food_delivery.db "SELECT COUNT(*), AVG(total), AVG(distance_km) FROM orders WHERE order_status='Delivered';"
```

## Configuration

- **Airflow UI:** http://localhost:8080
  - User: `airflow` / Password: `airflow` (auto-created by `airflow standalone` at `docker-compose.yaml:49`)
  - `AIRFLOW__CORE__EXECUTOR: LocalExecutor` at `docker-compose.yaml:36`
  - `AIRFLOW__CORE__LOAD_EXAMPLES: "false"` and `DAGS_ARE_PAUSED_AT_CREATION: "true"`
- **Paths:** `BASE_DIR`, `RAW_FILE`, `INTERMEDIATE_FILE`, `DATABASE_FILE`, `ANALYTICS_SQL` at `dags/pipeline.py:12-19` — all under `/opt/airflow` in container, bind-mounted from host via `docker-compose.yaml:43-47`
- **Schedule:** `schedule=None` at `dags/pipeline.py:47` — manual trigger only (avoids catchup)
- **Pinned deps:** `requirements.txt:5-7` `pandas==2.3.3` compatible with `apache/airflow:3.3.1` Python 3.12, `pendulum>=3.0` required for `pendulum.datetime(2026,1,1)` at `dags/pipeline.py:48`, `pyarrow>=15.0` for parquet; `Dockerfile:3-4` does `COPY requirements.txt` + `pip install -r`
- **Timeouts:** Tasks are short (~0.4–2s per task for 21k rows). Logs may show `UserWarning: Could not infer format, so each element will be parsed individually` at `dags/pipeline.py:97` — expected for mixed date strings, handled via `errors="coerce"`.

## Usage

### Trigger DAG

```bash
# UI
open http://localhost:8080  # DAGs → sales_pipeline → Trigger

# CLI
docker exec $(docker ps -q --filter name=airflow | head -1) airflow dags trigger sales_pipeline
docker exec $(docker ps -q --filter name=airflow | head -1) airflow dags list-runs -d sales_pipeline --limit 5
```

### View logs

```bash
# Host bind mount
ls -R logs/dag_id=sales_pipeline/run_id=manual__<ts>/task_id=*/attempt=1.log
cat logs/dag_id=sales_pipeline/run_id=manual__*/task_id=analytics/attempt=1.log | grep "Analytics Query"

# Container
docker exec $(docker ps -q --filter name=airflow | head -1) ls -R /opt/airflow/logs/dag_id=sales_pipeline
docker logs salespipeline-airflow-1 --tail 100
```

### Query the warehouse

```bash
sqlite3 data/food_delivery.db "SELECT city, COUNT(*) FROM orders GROUP BY city;"
sqlite3 data/food_delivery.db "SELECT order_month, SUM(total) FROM orders WHERE order_status='Delivered' GROUP BY order_month ORDER BY order_month;"
```

### Rerun after code changes

- Editing `dags/pipeline.py` or `scripts/analytics.sql`: no rebuild needed — DAG processor reloads within seconds (check `logs/dag_processor/.../pipeline.py.log`)
- Editing `requirements.txt` or `Dockerfile`: rebuild required (`docker compose up --build -d`)
- Changing `data/raw/order_history.csv`: trigger a new DAG run (it will re-extract)

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `DagImportError` `No module named 'pendulum'` locally | `pendulum` not in local venv (required at `dags/pipeline.py:8`) | `pip install -r requirements.txt` (includes `pendulum>=3.0`) |
| `FileNotFoundError: Dataset not found: /opt/airflow/data/raw/order_history.csv` at `dags/pipeline.py:57` | `data/raw/order_history.csv` missing or not mounted | Ensure file exists on host and `docker-compose.yaml:45` mount is `- ./data:/opt/airflow/data` |
| `FileNotFoundError: data/intermediate/...` | Intermediate dir not created before `to_csv` (fixed at `dags/pipeline.py:64,179,242`) | Update to latest `dags/pipeline.py` which does `mkdir(parents=True)` |
| `KeyError: 'distance'` or `TypeError: 'builtin_function_or_method' not subscriptable` at `dags/pipeline.py:111-114` | Old code used `df["distance"]` (lowercase, pre-rename) or `match.group[1]` (subscript) | Current code uses `df["Distance"]` and `match.group(1)` at `dags/pipeline.py:108-113` |
| `analytics` fails `Path(None)` at `dags/pipeline.py:296` for old run `manual__2026-09-14T06:32...` | That DagRun was created when `load` returned `None` (v1, 4 tasks); new DAG is v2 (5 tasks) retro-fitted | Fixed at `dags/pipeline.py:296-297` fallback to `DATABASE_FILE`; ignore old run or `airflow tasks clear sales_pipeline -t analytics --only-failed -y`, then trigger fresh |
| DAG not visible / 404 after `git mv dag/ → dags/` | Old repo had `dag/` (singular) tracked, Compose expects `dags/` (plural) | Repo now tracks `dags/pipeline.py` (via `git mv dag/pipeline.py dags/pipeline.py`); ensure you pulled latest and have no `dag/` folder |
| `sqlite3.OperationalError: no such table: orders` in analytics | Analytics run before load completed (dependency broken) | Chain is `dags/pipeline.py:348-350` `db_path = load(...); analytics(db_path)` — ensures order; check that `load` succeeded first |
| Airflow UI shows `Upstream Failed` for `validate/load/analytics` | `transform` failed (see its log) | Check `logs/dag_id=sales_pipeline/.../task_id=transform/attempt=1.log` for the `error_detail` traceback (common historic cause: distance parsing) |

## Airflow UI Walkthrough

- **DAGs → sales_pipeline → Grid / Graph:** See 5 nodes linear: `extract → transform → validate → load → analytics`. Last verified run `manual__2026-09-14T06:35:42` → all 5 dark green (Success).
- **Task Instances tab:** Shows `Task ID, State, Start Date, Try Number, Operator (@task), Duration` per task.
- **Logs tab:** Per-task JSON logs with `Pre Execute`, `Post Execute`, and `task.stdout` (e.g., `Extracted 21321 rows`, `Rows loaded into SQLite: 21321`, `Analytics Query 1: 1 rows`).
- **Details / Code tabs:** Show DAG version `v2` (after adding `analytics`) and rendered `pipeline.py` source.

## Roadmap

- Parameterize `RAW_FILE` and `DATABASE_FILE` via Airflow Variables / `params`
- Add data-quality tests (e.g., `Great Expectations` or `dbt`) after `validate`
- Materialize analytics as views/tables (`CREATE VIEW`) instead of just logging `SELECT`s
- Add schedule (e.g., `@daily`) and catchup/backfill support
- Export analytics results to `data/marts/` (CSV/Parquet) or a BI tool
- Add unit tests for `clean_distance` and transform logic, and CI (e.g., `pytest` + `astro dev parse`)

## License

No license file currently. Add `LICENSE` if you intend to open-source.

