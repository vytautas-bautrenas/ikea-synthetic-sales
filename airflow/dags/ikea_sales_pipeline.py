"""
IKEA Sales Data Pipeline - Master Airflow DAG
Orchestrates Sourcing -> Ingest -> dbt (Transform & Serve) -> Web App Readiness Probes.
Granular TaskGroups allow re-running individual tasks or entire layers.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import TaskGroup

from airflow import DAG

# Default task arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

PROJECT_DIR = "/opt/airflow"
PYTHON_DIR = f"{PROJECT_DIR}/python"
DBT_DIR = f"{PROJECT_DIR}/dbt"

with DAG(
    dag_id="ikea_sales_pipeline",
    default_args=default_args,
    description="End-to-end IKEA sales pipeline with granular re-run capability",
    schedule=None,  # Triggered on-demand or via single trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ikea", "sales", "dbt", "bigquery"],
) as dag:

    pipeline_start = EmptyOperator(task_id="pipeline_start")

    # =========================================================================
    # 1. SOURCING TASKGROUP (Parallel downloads & scraping)
    # =========================================================================
    with TaskGroup("sourcing", tooltip="Source layer downloads and scrapers") as sourcing:
        src_names = BashOperator(
            task_id="source_names",
            bash_command=f"python3 {PYTHON_DIR}/source_names.py",
        )
        src_stores = BashOperator(
            task_id="source_stores",
            bash_command=f"python3 {PYTHON_DIR}/source_stores.py",
        )
        src_prod_cat = BashOperator(
            task_id="source_prod_cat",
            bash_command=f"python3 {PYTHON_DIR}/source_prod_cat.py",
        )
        src_streets = BashOperator(
            task_id="source_streets",
            bash_command=f"python3 {PYTHON_DIR}/source_streets.py",
        )
        src_onspd = BashOperator(
            task_id="source_onspd",
            bash_command=f"python3 {PYTHON_DIR}/source_onspd.py",
        )
        src_postcode_areas = BashOperator(
            task_id="source_postcode_areas",
            bash_command=f"python3 {PYTHON_DIR}/source_postcode_areas.py",
        )

        # Postcode areas processing depends on ONSPD extract
        src_onspd >> src_postcode_areas

    # =========================================================================
    # 2. INGESTION TASKGROUP (Raw BigQuery table loads)
    # =========================================================================
    with TaskGroup("ingestion", tooltip="Ingestion layer BigQuery uploads") as ingestion:
        ing_names = BashOperator(
            task_id="ingest_names",
            bash_command=f"python3 {PYTHON_DIR}/ingest_names.py",
        )
        ing_stores = BashOperator(
            task_id="ingest_stores",
            bash_command=f"python3 {PYTHON_DIR}/ingest_stores.py",
        )
        ing_prod_cat = BashOperator(
            task_id="ingest_prod_cat",
            bash_command=f"python3 {PYTHON_DIR}/ingest_prod_cat.py",
        )
        ing_streets = BashOperator(
            task_id="ingest_streets",
            bash_command=f"python3 {PYTHON_DIR}/ingest_streets.py",
        )
        ing_onspd = BashOperator(
            task_id="ingest_onspd",
            bash_command=f"python3 {PYTHON_DIR}/ingest_onspd.py",
        )

        ingest_complete = EmptyOperator(task_id="ingest_complete")

        # All ingestion tasks converge into completion checkpoint
        [ing_names, ing_stores, ing_prod_cat, ing_streets, ing_onspd] >> ingest_complete

    # Granular wiring: each ingestion step begins as soon as its source completes
    pipeline_start >> sourcing
    src_names >> ing_names
    src_stores >> ing_stores
    src_prod_cat >> ing_prod_cat
    src_streets >> ing_streets
    src_postcode_areas >> ing_onspd

    # =========================================================================
    # 3. DBT TASKGROUP (Transform, Serve & Quality Assurance)
    # =========================================================================
    with TaskGroup("dbt_modeling", tooltip="dbt transformation, tests, and audit") as dbt_modeling:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"cd {DBT_DIR} && dbt run --exclude models/audit",
        )
        dbt_audit = BashOperator(
            task_id="dbt_audit",
            bash_command=f"cd {DBT_DIR} && dbt run --select models/audit",
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_DIR} && dbt test",
        )

        dbt_run >> dbt_audit >> dbt_test

    # =========================================================================
    # 4. WEB APPS VERIFICATION TASKGROUP (Readiness Probes via /health)
    # =========================================================================
    with TaskGroup("web_apps_readiness", tooltip="Verify FastAPI web app readiness") as web_apps_readiness:
        # Curl-based health checks with automatic retries inside the Docker network
        wait_invoicing = BashOperator(
            task_id="wait_for_invoicing_app",
            bash_command="python3 -c \"import urllib.request; urllib.request.urlopen('http://web_app_invoicing:8000/health', timeout=10)\"",
        )
        wait_heatmap = BashOperator(
            task_id="wait_for_heatmap_app",
            bash_command="python3 -c \"import urllib.request; urllib.request.urlopen('http://web_app_heatmap:8001/health', timeout=10)\"",
        )

    pipeline_complete = EmptyOperator(task_id="pipeline_complete")

    # End-to-end waterfall flow
    ingest_complete >> dbt_modeling >> web_apps_readiness >> pipeline_complete
