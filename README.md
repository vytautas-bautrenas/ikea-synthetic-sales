# 1. IKEA Synthetic Sales Data Pipeline

An end-to-end synthetic data pipeline that generates 22 billion e-commerce order rows in 4m 44s, for £0.61.

<p align="center">
   <img src="etc/uk-sales-geographic-heatmap.png" width="51%" alt="UK Sales 10km Grid Heatmap" />
  <img src="etc/uk-sales-invoice.png" width="48%" alt="UK Sales VAT Invoice Web App" />
</p>

<br>

# 2. Table of Contents

- [1. Project Overview](#1-ikea-synthetic-sales-data-pipeline)
- [2. Table of Contents](#2-table-of-contents)
- [3. Problem Statement](#3-problem-statement)
- [4. Architecture](#4-architecture)
- [5. Running Time and Costs](#5-running-time-and-costs)
- [6. Repo Structure](#6-repo-structure)
- [7. Setup](#7-setup)
  - [7.1. Prerequisites](#71-prerequisites)
  - [7.2. Environment Configuration](#72-environment-configuration)
  - [7.3. Service Account Credentials](#73-service-account-credentials)
  - [7.4. Provision Infrastructure](#74-provision-infrastructure)
- [8. Running Instructions](#8-running-instructions)
  - [8.1. Option A: Automated Pipeline via Airflow](#81-option-a-automated-pipeline-via-airflow)
  - [8.2. Option B: Manual Step-by-Step via Makefile](#82-option-b-manual-step-by-step-via-makefile)
  - [8.3. Cleanup](#83-cleanup)
- [9. Viewing The Results Via Web Applications](#9-viewing-the-results-via-web-applications)
  - [9.1. Invoicing Web App](#91-invoicing-web-app)
  - [9.2. Geographic Sales Heatmap Web App](#92-geographic-sales-heatmap-web-app)
- [10. Known Issues](#10-known-issues)
- [11. Data Sources](#11-data-sources)
- [12. Licence](#12-licence)

<br>

# 3. Problem Statement

- There is a need for large-scale, production-grade e-commerce sales datasets for data engineering projects.
- Public commercial sales datasets are virtually non-existent at scale.
- Real retailer data cannot be published due to commercial confidentiality and GDPR; results cannot be reproduced publicly even if a data engineer acquired access to a real company data.
- LLM generation at scale (10 billion+ rows) is prohibitively slow and may cost tens of thousands of pounds in API tokens.
- **Solution**: Generate a deterministic, scalable, mathematically rigorous synthetic dataset with 22 billion detail rows, 6 billion orders and 70 million customers, in 4m 44s, for £0.61 in cloud compute, with zero LLM API costs.

<br>

# 4. Architecture

![Architecture](etc/architecture.svg)

Pipeline flow:
1. **Source**: Download external seed data via Python scripts (`python/source_*.py`).
2. **Ingest**: Convert to typed columnar Parquet and stream upload directly to BigQuery raw landing tables (`python/ingest_*.py`).
3. **Transform**: dbt generates synthetic entities, dimensional models, and marts (`dbt/models/`).
4. **Audit**: dbt reconciliation tests verify financial accuracy and dimensional counts against input seeds.
5. **Serve**: Deliver data to downstream FastAPI web applications (`web_app_invoicing/` and `web_app_heatmap/`).
6. **Orchestrate**: Airflow orchestrates the DAG with automated task-level retries.


Core mechanisms enabling sub-5-minute generation of 22 billion rows without data hallucinations:

1. **Storage-Driven Slot Parallelism (`int_number_expansion`)**:
   In BigQuery, generating rows in memory with `GENERATE_ARRAY()` runs on a single worker, whereas reading from a table assigns one parallel worker per storage partition. `int_number_expansion` pre-generates 150 million integers (~1.15 GB) split across 2,000 integer-range partitions (`MOD(i, 2000)`). When `fact_orders` reads this table, BigQuery immediately spins up 2,000 parallel workers — saturating the project's 2,000-slot quota — to generate all 6 billion orders and 15.9 billion line items in under 3 minutes.

2. **Fault Resilience & Retry Architecture**:
   To ensure cold-start execution integrity across external scrapers and long-running warehouse transformations, the pipeline implements a 4-layer defense-in-depth shield: in-process client SDK retries (`urllib` 3× exponential backoff, BigQuery 8MB resumable chunk uploads with 600s budgets, and dbt `job_retries: 3`), atomic zero-corruption staging (`.part` streaming temp files with byte-size verification before rename, `WRITE_TRUNCATE` table loads, and atomic `CREATE OR REPLACE TABLE` dbt models), uniform Airflow task retries (`retries: 3` with exponential backoff across all 17 DAG tasks), and web app runtime resilience with warm BigQuery client session pooling and self-healing memory caches.

   ![Retry Resilience](etc/retry_resilience.svg)

3. **Discrete CDF Slotting for O(1) Broadcast Equi-Joins**:
   Arbitrary non-uniform continuous and discrete probability distributions (Pareto, geometric basket sizes, product popularity) are pre-calculated into a 1,000,000-row discrete integer slot lookup table. Random sampling reduces to `MOD(ABS(FARM_FINGERPRINT(...)), 1000000)` joined via integer equi-joins. BigQuery broadcasts the 1M-row lookup table into worker RAM, making 16-billion-row lookups fast and fully deterministic.

4. **Streaming CSV-to-Parquet Pre-Processing**:
   Raw text parsing in BigQuery on multi-GB files (e.g. 1.4 GB ONSPD) takes 40–60s and suffers from loose type inference. Streaming raw CSVs locally through Polars (`scan_csv().sink_parquet()`) produces compressed columnar Parquet in ~3s. BigQuery loads Parquet directly, bypassing text parsing bottlenecks.

5. **Nested Line Items & Zero-Join Order Retrieval (`fact_orders`)**:
   Instead of storing order headers and order lines in separate tables that require expensive multi-billion-row shuffle joins to recombine, `fact_orders` pairs order-level attributes (`customer_id`, `payment_method`, `delivery_type`, timestamps, totals) directly with a nested `ARRAY<STRUCT>` of item details (`product_id`, `unit_price`, `unit_vat`).
   - **Zero-Join Reads**: Point lookups (such as generating a customer's VAT invoice) read the entire order and all its line items from a single clustered row with no table joins.
   - **Columnar Skipping**: Downstream aggregations (such as the 10km grid sales mart) scan only the header columns and completely skip reading the nested item arrays.
   - **Relational View Flexibility**: A flat `fact_order_lines` view is provided for traditional BI tools by unnesting on the fly, with zero duplicate storage cost.

6. **Lossless Integer Minor-Units Currency**:
   All monetary amounts are stored as integer minor units (`INT64` pence). Eliminates IEEE 754 floating-point inaccuracies, rounding divergence between rollups, and overhead across all models.

<br>

# 5. Running Time and Costs

Below are the results of a cold-start Airflow DAG run (`manual__2026-10-30T20:49:09.217595+00:00`). Because Airflow and `dbt build` execute independent tasks concurrently, the table shows each layer's **critical-path wall time** alongside work hidden in parallel.

| Layer | Critical-Path Time | BigQuery Costs | Data Scanned | What it Does |
|---|---|---|---|---|
| **Source** | **35s** *(69s in parallel)* | **£0.00** *(free)* | ~1.5 GB | Scrapes external seed data (OS, ONS Census, IKEA website) |
| **Ingest** | **44s** | **£0.00** *(free)* | ~1.5 GB | Polars streaming $\to$ typed Parquet $\to$ BigQuery raw landing |
| **Transform** | **11s** *(+62s in parallel)* | **< £0.01** | 0.13 GB | 2,000-partition driver (`int_number_expansion`), product CDF slots & name tables |
| **Serve** | **3m 09s** | **£0.44** | 93.4 GB | Generates 6B `fact_orders` (15.9B nested lines), 70M `dim_customers`, & 10km grid mart |
| **Audit & Tests** | **5s** *(+5s in parallel)* | **£0.17** | 35.8 GB | Reconciles financials, counts & statistical distributions (`10% TABLESAMPLE`) + `dbt test` |
| **Total** | **4m 44s** | **£0.61** | **129.4 GB** | **Full cold-start end-to-end pipeline run** |

**Key Takeaways**

1. **Generating 22 Billion Rows Costs £0.01 (£0.44 Total Serve Layer)**:
   Materialising 6 billion clustered orders (with 15.9 billion nested line items) and 70 million customers scans just **2.75 GB (£0.013)** by generating rows in memory from the 1.15 GB partitioned driver table. Nearly all of the Serve layer bill (**90.6 GB / £0.43**) comes from aggregating those 6 billion orders into the downstream 10km geographic heatmap mart.

2. **10% Block Sampling & Single-Pass Audit Costs £0.17**:
   By evaluating reconciliation and statistical distribution checks in [`inputs_vs_outputs_checks`](dbt/models/audit/inputs_vs_outputs_checks.sql) using `TABLESAMPLE SYSTEM (10 PERCENT)` (35.8 GB) and running all `dbt test` assertions concurrently alongside the 10km grid build, full quality assurance adds **0 seconds** to the critical path and costs just **£0.17**.


<br>

# 6. Repo Structure

```
├── airflow/            # Airflow DAG, Dockerfile, docker-compose configuration
├── data/               # Local seed data storage (git-ignored)
├── dbt/                # dbt models (ingest, transform, serve, audit), macros, tests
├── etc/                # Architecture diagrams, retry diagrams, screenshots
├── python/             # Sourcing (source_*.py) and ingestion (ingest_*.py) scripts
├── secrets/            # GCP & Terraform service account keys (git-ignored)
├── terraform/          # Terraform configuration for BigQuery datasets and IAM resources
├── web_app_heatmap/    # FastAPI + Leaflet UK sales 10km grid heatmap app
├── web_app_invoicing/  # FastAPI + Code128 barcode UK VAT invoice app
├── .env_example        # Environment variable template
├── .gitignore          # Git exclusion rules
├── .python-version     # Python version
├── LICENSE             # Licence
├── Makefile            # Single-command pipeline interface
├── pyproject.toml      # Python dependencies and project settings
└── uv.lock             # Pinned dependency lockfile
```

<br>

# 7. Setup

## 7.1. Prerequisites

- GCP account with billing enabled
- BigQuery and Cloud Storage APIs enabled
- Terraform >= 1.5
- Docker & Docker Compose
- `uv` Python package manager

## 7.2. Environment Configuration

Copy the example environment file:

```bash
make env
```

Edit `.env` with your GCP project ID and region:

```bash
export GCLOUD_PROJECT=your-gcp-project-id
export LOCATION=europe-west2
```

## 7.3. Service Account Credentials

Place GCP service account JSON credentials in `secrets/`:
- `secrets/terraform-sa-key.json`: Terraform credentials (`roles/resourcemanager.projectIamAdmin`, `roles/bigquery.admin`).

## 7.4. Provision Infrastructure

```bash
make terra-apply
```

<br>

# 8. Running Instructions

Option A is preferred as it is a way to run the whole end-to-end data pipeline.

Should any issues arise, the suggestion is to point your AI agent at the repo, and ask it to troubleshoot the process.


## 8.1. Option A: Automated Pipeline via Airflow

1. Start Airflow services:
   ```bash
   make airflow-up
   ```

2. Trigger pipeline run:
   ```bash
   make airflow-trigger
   ```

3. Follow execution logs:
   ```bash
   make airflow-logs
   ```

4. View pipeline progress in Airflow:
   Open [http://localhost:8080/dags](http://localhost:8080/dags), login with username & password `admin`, and click on the latest run to see the progress.

5. Shut down Airflow:
   ```bash
   make airflow-down
   ```

## 8.2. Option B: Manual Step-by-Step via Makefile

1. **Download External Seed Data**:
   ```bash
   make source-onspd source-prod-cat source-names source-streets source-stores source-postcode-areas
   ```

2. **Ingest to BigQuery**:
   ```bash
   make ingest-onspd ingest-prod-cat ingest-names ingest-streets ingest-stores
   ```

3. **Run Data Synthesis**:
   ```bash
   make dbt-run
   ```

4. **Run Data Integrity & Audit Tests**:
   ```bash
   make dbt-test
   ```

## 8.3. Cleanup

1. Destroy service account & BigQuery datasets:
   ```bash
   make terra-destroy
   ```

2. Delete project within GCP to ensure no further usage accrues.

3. Delete local credentials to ensure no leaked credentials:
   ```bash
   rm secrets/terraform-sa-key.json
   ```

<br>

# 9. Viewing The Results Via Web Applications

## 9.1. Invoicing Web App

```bash
make invoice
```
Open [http://localhost:8000](http://localhost:8000).

Sample invoice from screenshot: **Order Number**: `7777777`, **Postcode**: `TA18 8PB`

To look up the postcode for any order number, run this BigQuery SQL:

```sql
SELECT
  fo.order_id,
  c.customer_id,
  c.first_name,
  c.last_name,
  c.postcode
FROM `<project-id>.serve.fact_orders` fo
LEFT JOIN `<project-id>.serve.dim_customers` c
  ON fo.customer_id = c.customer_id
WHERE fo.order_id = 7777777
```
*(replace `<project-id>` with your GCP project ID)*

## 9.2. Geographic Sales Heatmap Web App

```bash
make heatmap
```
Open [http://localhost:8001](http://localhost:8001) to explore UK sales aggregated by 10km grid cell over ONS boundaries.

<br>

# 10. Known Issues

- **VAT Edge Case Rounding**: VAT may appear as 19% or 21% on select low-priced items due to integer pence rounding on minor currency units.

<br>

# 11. Data Sources

- **ONS Postcode Directory (ONSPD)**: Office for National Statistics (Open Government Licence v3.0).
- **IKEA UK Catalog**: Public catalog product metadata.
- **UK School Census**: Forename and surname frequency distributions.
- **UK Street Names**: Public road and street directory.

<br>

# 12. Licence

Released to the public domain under [The Unlicense](http://unlicense.org/).
