# 🏗️ Distributed Data Lakehouse — Olist E-commerce

> An end-to-end **Data Lakehouse** built with open-source tools, implementing the **Medallion Architecture** (Bronze → Silver → Gold) on real-world e-commerce data.

[![Stack](https://img.shields.io/badge/Stack-MinIO%20%7C%20Iceberg%20%7C%20Trino%20%7C%20dbt%20%7C%20Airflow-blue)](#tech-stack)
[![Dataset](https://img.shields.io/badge/Dataset-Olist%20Brazilian%20E--Commerce-green)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)
[![Docker](https://img.shields.io/badge/Infra-Docker%20Compose-2496ED)](docker-compose.yml)

---

## 📌 Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Tech Stack](#tech-stack)
- [Medallion Architecture](#medallion-architecture)
- [Data Pipeline](#data-pipeline)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Data Models](#data-models)
- [Key Metrics & KPIs](#key-metrics--kpis)
- [Screenshots](#screenshots)
- [Lessons Learned](#lessons-learned)
- [Roadmap](#roadmap)

---

## Overview

This project demonstrates how to build a **production-grade Data Lakehouse** entirely with open-source technologies. Using the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~100,000 real orders), it implements a complete data pipeline from raw CSV ingestion to business-ready analytical tables.

The entire stack runs **locally via Docker Compose**, making it reproducible on any machine without cloud dependencies.

### What This Project Covers

- **Distributed object storage** with MinIO (S3-compatible)
- **Open table format** with Apache Iceberg (ACID transactions, time travel, schema evolution)
- **Git-like data versioning** with Project Nessie (branching, rollback at the catalog level)
- **Distributed SQL engine** with Trino (query Iceberg tables at scale)
- **SQL transformations** with dbt (Bronze → Silver → Gold, with testing and documentation)
- **Pipeline orchestration** with Apache Airflow (scheduled DAGs, monitoring, retries)

---

## System Architecture

> 📸 *Add your system architecture diagram here*

<!-- ![System Architecture](docs/images/architecture_overview.png) -->

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│         CSV Files (Olist) — 9 files, ~100k orders              │
└─────────────────────────┬───────────────────────────────────────┘
                          │  Python scripts (boto3)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER — MinIO                         │
│         S3-compatible object store (Parquet files)              │
│    lakehouse-bronze / lakehouse-silver / lakehouse-gold         │
└─────────────────────────┬───────────────────────────────────────┘
                          │  Apache Iceberg (open table format)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                  CATALOG — Project Nessie                       │
│        Git-like versioning for Iceberg tables                   │
│         (branches, commits, rollback, time travel)              │
└─────────────────────────┬───────────────────────────────────────┘
                          │  REST Catalog API
                          ▼
┌──────────────┐   SQL    ┌──────────────────────────────────────┐
│   AIRFLOW    │─────────▶│         TRINO                        │
│  Orchestrate │          │  Distributed SQL engine              │
│  DAGs / ETL  │          │  Reads Iceberg via Nessie + MinIO    │
└──────────────┘          └──────────────┬───────────────────────┘
                                         │  dbt transformations
                          ┌──────────────▼───────────────────────┐
                          │              dbt                      │
                          │  Bronze → Silver → Gold models        │
                          │  Tests, documentation, lineage        │
                          └──────────────┬───────────────────────┘
                                         │
                          ┌──────────────▼───────────────────────┐
                          │         CONSUMPTION                   │
                          │  Trino SQL · BI Tools · Notebooks     │
                          └──────────────────────────────────────┘
```

---

## Tech Stack

| Tool | Version | Role |
|------|---------|------|
| **MinIO** | Latest | S3-compatible object storage — stores all Parquet files |
| **Apache Iceberg** | — | Open table format — ACID, time travel, schema evolution |
| **Project Nessie** | Latest | Git-like Iceberg catalog — branching, versioning, rollback |
| **Trino** | 438 | Distributed SQL query engine — analytical queries at scale |
| **dbt** | 1.8.x | SQL transformation framework — models, tests, documentation |
| **Apache Airflow** | 2.9.1 | Workflow orchestration — scheduled DAGs, monitoring |
| **PostgreSQL** | 15 | Airflow metadata database |
| **Docker Compose** | v2 | Local infrastructure orchestration |
| **Python** | 3.11 | Data ingestion scripts (boto3, trino, pandas) |

---

## Medallion Architecture

> 📸 *Add your Medallion layers diagram here*

<!-- ![Medallion Architecture](docs/images/medallion_architecture.png) -->

The pipeline follows the **Medallion Architecture** pattern, progressively refining data across three layers:

### 🟤 Bronze — Raw Ingestion
- Data ingested **as-is** from CSV files into Iceberg tables
- No transformation, no cleaning — preserves the original source
- Adds `_loaded_at` timestamp for auditing
- 9 tables: orders, items, customers, payments, reviews, products, sellers, geolocation, category translations

### ⚪ Silver — Cleaned & Enriched
- **Type casting**: VARCHAR timestamps → real `TIMESTAMP(6)` values
- **Filtering**: Invalid statuses and null timestamps removed
- **Calculated fields**: `delivery_days`, `delay_days` (actual vs. estimated)
- **Joins**: Orders enriched with payment amounts, customer location, and review scores

### 🥇 Gold — Business KPIs
- Aggregated, analytics-ready tables consumed by BI tools and dashboards
- **`daily_revenue`** — Revenue, order count, delivery performance by day and state
- **`top_categories`** — Product category ranking by revenue, rating, and delivery time
- **`seller_performance`** — Vendor metrics: revenue, on-time rate, average rating

---

## Data Pipeline

> 📸 *Add your Airflow DAG screenshot here*

<!-- ![Airflow DAG](docs/images/airflow_dag.png) -->

The pipeline is orchestrated by **Apache Airflow** and runs daily at 06:00 UTC:

```
check_minio ──┐
              ├──▶ upload_csv_minio ──▶ create_bronze_tables ──▶ load_bronze
check_trino ──┘
                                                                      │
                                          ┌───────────────────────────┘
                                          ▼
                                   dbt_run_bronze ──▶ dbt_test_bronze
                                                              │
                                                    dbt_run_silver ──▶ dbt_test_silver
                                                                              │
                                                                    dbt_run_gold ──▶ dbt_test_gold
                                                                                           │
                                                                                    notify_success
```

Each layer is **tested before moving to the next** — if Bronze quality tests fail, Silver and Gold do not run.

---

## Project Structure

```
lakehouse/
├── 📄 docker-compose.yml            ← Full stack (8 services)
├── 🚀 start.sh                      ← One-command startup
├── 🛑 stop.sh                       ← Clean shutdown
├── ⚙️  run_pipeline.sh              ← Run dbt Bronze → Silver → Gold
├── 📋 requirements.txt              ← Python dependencies
├── 🔒 .env                          ← Credentials (not committed)
│
├── 📁 data/olist/                   ← Place the 9 CSV files here
│
├── 📁 scripts/
│   ├── 01_upload_to_minio.py        ← Upload CSVs to MinIO
│   ├── 02_create_bronze_tables.py   ← Create 9 Iceberg Bronze tables
│   ├── 03_load_bronze.py            ← Insert data (batched, 500 rows)
│   └── 04_verify_results.py        ← Validate Gold KPIs in terminal
│
├── 📁 trino/catalog/
│   └── iceberg.properties           ← Trino → Nessie → MinIO config
│
├── 📁 dbt/
│   ├── Dockerfile                   ← Custom dbt-trino image (pip install)
│   ├── dbt_project.yml
│   ├── profiles.yml                 ← Trino connection (dev + local targets)
│   ├── models/
│   │   ├── bronze/                  ← 6 staging views + schema.yml
│   │   ├── silver/                  ← 2 cleaned tables + schema.yml
│   │   └── gold/                    ← 3 KPI tables + schema.yml
│   └── tests/                      ← 3 custom data quality tests
│
└── 📁 airflow/dags/
    └── olist_pipeline.py            ← Main DAG (12 tasks, daily at 6h UTC)
```

---

## Getting Started

### Prerequisites

- **Docker Desktop** with at least **8 GB RAM** allocated
  *(Preferences → Resources → Memory → 8192 MB)*
- **Python 3.10+**
- **Kaggle account** (free) to download the dataset

### Step 1 — Download the Dataset

```bash
pip install kaggle
# Place your kaggle.json in ~/.kaggle/
kaggle datasets download -d olistbr/brazilian-ecommerce
unzip brazilian-ecommerce.zip -d ./data/olist/
```

Or download manually: [Olist on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) → extract into `./data/olist/`

### Step 2 — Start the Stack

```bash
chmod +x start.sh && ./start.sh
```

This single command will:
1. Start MinIO, Nessie, PostgreSQL — and create the 4 buckets
2. Start Trino and wait for it to be healthy (~60s)
3. Build the dbt image via `pip install dbt-trino`
4. Initialize Airflow and create the admin user
5. Create the Iceberg schemas (bronze / silver / gold)

### Step 3 — Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Load Bronze Data

```bash
python scripts/01_upload_to_minio.py        # Upload CSVs to MinIO
python scripts/02_create_bronze_tables.py   # Create Iceberg Bronze tables
python scripts/03_load_bronze.py            # Insert data (5–15 min)
```

### Step 5 — Run dbt Transformations

```bash
./run_pipeline.sh
```

Or step by step:

```bash
docker compose exec dbt dbt run --select bronze.* --profiles-dir /usr/app/dbt
docker compose exec dbt dbt run --select silver.* --profiles-dir /usr/app/dbt
docker compose exec dbt dbt run --select gold.*   --profiles-dir /usr/app/dbt
docker compose exec dbt dbt test                  --profiles-dir /usr/app/dbt
```

### Step 6 — Verify Results

```bash
python scripts/04_verify_results.py
```

### Available Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| **MinIO Console** | http://localhost:9001 | admin / password123 |
| **Nessie UI** | http://localhost:19120/ui | — |
| **Trino UI** | http://localhost:8080 | — |
| **Airflow UI** | http://localhost:8081 | admin / admin |

---

## Data Models

> 📸 *Add your dbt lineage graph here*

<!-- ![dbt Lineage](docs/images/dbt_lineage.png) -->

### Dependency Graph

```
bronze.orders ──────────────────────────────────────────────┐
bronze.order_payments ──▶ stg_payments ──┐                  ├──▶ orders_clean ──▶ orders_enriched
bronze.order_reviews  ──▶ stg_reviews  ──┤                  │                          │
bronze.customers      ──▶ stg_customers ─┘                  │                          │
bronze.order_items    ──▶ stg_order_items ──────────────────┼──────────────────────────┤
bronze.products       ──▶ stg_products   ──────────────────┘                          │
                                                                                        │
                                                          ┌─────────────────────────────┘
                                                          ▼
                                                daily_revenue
                                                top_categories
                                                seller_performance
```

### Gold Tables Schema

**`iceberg.gold.daily_revenue`**

| Column | Type | Description |
|--------|------|-------------|
| `revenue_date` | DATE | Order date |
| `state` | VARCHAR | Brazilian state (SP, RJ, MG...) |
| `order_count` | BIGINT | Number of orders |
| `total_revenue` | DOUBLE | Total payment amount (BRL) |
| `avg_order_value` | DOUBLE | Average basket size |
| `avg_delivery_days` | DOUBLE | Average actual delivery time |
| `late_delivery_pct` | DOUBLE | % of orders delivered late |
| `avg_review_score` | DOUBLE | Average customer rating (1–5) |

**`iceberg.gold.top_categories`**

| Column | Type | Description |
|--------|------|-------------|
| `category` | VARCHAR | Product category (English) |
| `total_orders` | BIGINT | Number of orders |
| `total_revenue` | DOUBLE | Total revenue (BRL) |
| `avg_price` | DOUBLE | Average item price |
| `avg_rating` | DOUBLE | Average customer rating |
| `avg_delivery_days` | DOUBLE | Average delivery time |
| `revenue_rank` | BIGINT | Rank by revenue (1 = highest) |

**`iceberg.gold.seller_performance`**

| Column | Type | Description |
|--------|------|-------------|
| `seller_id` | VARCHAR | Unique seller identifier |
| `seller_state` | VARCHAR | Seller location |
| `total_orders` | BIGINT | Orders fulfilled |
| `total_revenue` | DOUBLE | Total revenue generated |
| `avg_rating` | DOUBLE | Average review score |
| `on_time_pct` | DOUBLE | % of orders delivered on time |

---

## Key Metrics & KPIs

> 📸 *Add your Trino query results or dashboard screenshot here*

<!-- ![KPI Results](docs/images/kpi_results.png) -->

```sql
-- Total revenue across all orders
SELECT
    SUM(order_count)             AS total_orders,
    ROUND(SUM(total_revenue), 2) AS total_revenue_brl
FROM iceberg.gold.daily_revenue;

-- Top 10 product categories by revenue
SELECT category, total_orders, ROUND(total_revenue, 2) AS revenue_brl, avg_rating
FROM iceberg.gold.top_categories
ORDER BY revenue_rank
LIMIT 10;

-- States with highest average basket size
SELECT state, SUM(order_count) AS orders, ROUND(AVG(avg_order_value), 2) AS avg_basket
FROM iceberg.gold.daily_revenue
GROUP BY state
ORDER BY avg_basket DESC
LIMIT 10;

-- Iceberg time travel — query data as of a past snapshot
SELECT COUNT(*) FROM iceberg.bronze.orders
FOR TIMESTAMP AS OF TIMESTAMP '2025-01-01 00:00:00';
```

---

## Screenshots

> 📸 *Add your project screenshots below*

| | |
|--|--|
| **MinIO Buckets** | **Nessie Catalog UI** |
| *(add screenshot)* | *(add screenshot)* |
| **Trino Query UI** | **Airflow DAG** |
| *(add screenshot)* | *(add screenshot)* |
| **dbt Run Output** | **dbt Lineage Graph** |
| *(add screenshot)* | *(add screenshot)* |

<!-- Uncomment and fill in after adding images to docs/images/:
![MinIO Console](docs/images/minio_console.png)
![Nessie UI](docs/images/nessie_ui.png)
![Trino UI](docs/images/trino_ui.png)
![Airflow DAG](docs/images/airflow_dag.png)
![dbt Run](docs/images/dbt_run.png)
![dbt Lineage](docs/images/dbt_lineage.png)
-->

---

## Lessons Learned

### Technical Challenges

**WSL2 & Docker bind-mounts**
Mounting individual config files (e.g. `jvm.config`) as Docker volumes fails on Windows/WSL2 — Docker creates an empty directory instead of the expected file. Fixed by passing JVM options via `JAVA_TOOL_OPTIONS` and writing `config.properties` at container startup via entrypoint script.

**Trino entrypoint override**
Overriding the default Trino entrypoint breaks internal startup hooks (`run-trino`). Solution: rely on Trino's built-in defaults and only mount the `catalog/` directory — never individual files.

**dbt-trino image availability**
`ghcr.io/dbt-labs/dbt-trino` requires authentication and is not publicly accessible. Replaced with a custom `Dockerfile` using `pip install dbt-trino` from PyPI — simpler and always up-to-date.

**Airflow multi-line entrypoint**
YAML folded block scalar (`>`) with line-continuation breaks Airflow's `users create` command, passing each flag as a separate shell command. Fixed by using `entrypoint: ["/bin/bash", "-c"]` combined with a literal block scalar (`|`) for the command string.

### Key Design Decisions

- **Nessie over Hive Metastore**: Nessie adds Git-like branching at the catalog level, enabling safe schema changes and atomic multi-table commits without a Hadoop dependency.
- **Iceberg over raw Parquet**: ACID compliance, time travel, and schema evolution make the data lake behave like a data warehouse while retaining the cost and flexibility benefits of object storage.
- **dbt over custom ETL scripts**: Version-controlled SQL, built-in testing, auto-generated lineage and documentation make transformations auditable, reproducible, and maintainable.

---

## Roadmap

- [ ] Add **Apache Superset** dashboard consuming Gold tables via Trino JDBC
- [ ] Implement **incremental dbt models** to avoid full table recomputation daily
- [ ] Add **Nessie branching workflow**: dev branch → test → merge to main
- [ ] Extend Gold layer: customer segmentation, cohort analysis, RFM scoring
- [ ] Integrate **Great Expectations** for advanced data quality monitoring
- [ ] Deploy on **cloud infrastructure** (AWS S3 / GCS replacing MinIO, EKS replacing Docker)
- [ ] Add **CI/CD pipeline** for dbt model validation on pull requests (GitHub Actions)

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist**
- 📦 Source: [Kaggle — olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- 📊 ~100,000 orders from 2016 to 2018
- 📁 9 CSV files covering the full order lifecycle
- 🔒 Real marketplace data, fully anonymized

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  Built with ❤️ for learning · Open source · Contributions welcome
</p>
