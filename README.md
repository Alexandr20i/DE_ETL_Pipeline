# DE ETL Pipeline

Data Engineering project: from operational database to Tableau dashboard.

## Architecture

```
MySQL (operational DB)
        ↓
       MRR  ← raw copy from source (delta via HWM)
        ↓
       STG  ← cleaned and deduplicated data
        ↓
       DWH  ← final storage + data marts for analytics
        ↓
    Tableau ← dashboard
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Operational DB | MySQL 8.0 |
| Data Warehouse | PostgreSQL 15 |
| ETL Orchestration | Apache Airflow 2.8.1 |
| Containerization | Docker / Docker Compose |
| BI | Tableau Public |

## Project Structure

```
DE_etl_pipeline/
├── docker-compose.yml        # infrastructure
├── backup.sh                 # backup for macOS/Linux
├── backup.ps1                # backup for Windows
├── dags/
│   ├── utils.py              # shared connection and logging functions
│   ├── etl_customers.py      # DAG: customers (SCD Type 2)
│   ├── etl_products.py       # DAG: products (SCD Type 1)
│   └── etl_sales.py          # DAG: sales + mart refresh
├── sql/
│   ├── mysql/
│   │   ├── 01_init_operational.sql   # CREATE TABLE
│   │   ├── 02_customers_data.sql     # INSERT data
│   │   ├── 03_products_data.sql      # INSERT data
│   │   └── 04_sales_data.sql         # INSERT data
│   └── postgres/
│       ├── 01_init_databases.sql     # CREATE DATABASE mrr, stg, dwh
│       ├── 02_init_mrr.sql           # MRR layer tables
│       ├── 03_init_stg.sql           # STG layer tables
│       ├── 04_init_dwh.sql           # DWH layer tables + marts
│       └── 05_init_dwh_procedures.sql # procedures and functions
├── pgadmin/
│   └── servers.json          # pgAdmin auto-connection
└── backups/                  # backup storage folder
```

## Quick Start

### Requirements
- Docker Desktop
- Docker Compose

### Run

```bash
# Clone the repository
git clone <repo_url>
cd DE_etl_pipeline

# Start all services
docker-compose up -d

# Check status
docker-compose ps
```

### Service Access

| Service | URL | Login | Password |
|---------|-----|-------|----------|
| Airflow | http://localhost:8080 | admin | admin |
| pgAdmin | http://localhost:5050 | admin@admin.com | admin |
| MySQL | localhost:3307 | etl_user | etl_pass |
| PostgreSQL | localhost:5433 | dwh_user | dwh_pass |

### Running ETL

Trigger DAGs in Airflow UI in this order:
1. `etl_customers`
2. `etl_products`
3. `etl_sales`

## Storage Layers

### MRR (Mirror)
Raw copy of data from the source. Data is extracted using **High Water Mark** — only changes since the last load. Stores historical copies of all loads.

### STG (Staging)
Cleaned layer. Dimensions store the current version via UPSERT. Facts are added incrementally without duplicates.

### DWH (Data Warehouse)
Final layer for analytics:
- `dwh_dim_customers` — **SCD Type 2**, stores customer change history
- `dwh_dim_products` — **SCD Type 1**, overwrites with current data
- `dwh_fact_sales` — sales facts, INSERT only
- `dwh_mart_sales_by_country` — mart: sales by country
- `dwh_mart_top_products` — mart: top products

## ETL Logic

### High Water Mark
```
First run:       HWM = 1900-01-01 → load all data
Subsequent runs: HWM = last updated_at → load delta only
```

### SCD Type 2 (customers)
```
Customer moved from Spain to France:
  Old record: is_current=false, valid_to=today
  New record: is_current=true,  valid_from=today
```

## Procedures and Functions (DWH)

```sql
-- Function: total sales qty by country
SELECT get_total_qty_by_country('Portugal');

-- Procedure: refresh all marts
CALL refresh_marts();
```

## Logging

Each DAG writes logs to the `etl_logs` table in each layer:

```sql
SELECT dag_id, task_id, status, message, finished_at
FROM etl_logs
ORDER BY finished_at DESC;
```

## Backup

### macOS / Linux / Git Bash

```bash
# Grant execute permissions (first time only)
chmod +x backup.sh

bash backup.sh
```

### Windows
```bash
.\backup.ps1
```

Backups are saved to the `backups/` folder with a timestamp in the filename:
```
backups/
├── mrr_2026-06-05_00-52-11.sql
├── stg_2026-06-05_00-52-11.sql
└── dwh_2026-06-05_00-52-11.sql
```

## Tableau Dashboard

[Dashboard link](<https://public.tableau.com/app/profile/alexandr.belov/viz/SalesAnalyticstest/SalesAnalytics?publish=yes>)

```Tableau Public does not support direct connections to a local PostgreSQL — data was exported from DWH marts to CSV. In a production environment with Tableau Desktop, the connection would be directly to dwh via port 5433.```

Two sheets:
- **Sales by Country** — top countries by sales volume
- **Top Products** — top products by category
