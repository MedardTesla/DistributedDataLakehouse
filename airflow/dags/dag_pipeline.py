"""
DAG principal : ingestion Bronze → transformation dbt Silver → Gold
Planification : quotidienne à 6h00 UTC
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json, logging

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def ingest_orders(**context):
    """Simule l'ingestion de commandes — remplacer par votre source réelle."""
    import boto3
    ds = context["ds"]
    sample_data = [
        {"order_id": f"ORD-{ds}-001", "user_id": "USR-1", "total_amount": 149.99,
         "status": "completed", "created_at": f"{ds}T10:00:00"},
        {"order_id": f"ORD-{ds}-002", "user_id": "USR-2", "total_amount": 89.50,
         "status": "shipped",   "created_at": f"{ds}T11:30:00"},
    ]
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password123",
    )
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_data, f)
        tmp = f.name
    s3.upload_file(tmp, "lakehouse-bronze", f"orders/dt={ds}/data.json")
    logging.info(f"[Bronze] {len(sample_data)} commandes ingérées pour {ds}")
    return len(sample_data)

with DAG(
    dag_id="ecommerce_full_pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 4, 7),
    default_args=default_args,
    catchup=False,
    tags=["ecommerce", "medallion"],
    description="Pipeline complet Bronze → Silver → Gold",
) as dag:

    t_ingest = PythonOperator(
        task_id="ingest_bronze_orders",
        python_callable=ingest_orders,
    )

    t_dbt_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command="cd /opt/dbt && dbt run --select bronze.* --profiles-dir .",
    )

    t_dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command="cd /opt/dbt && dbt run --select silver.* --profiles-dir .",
    )

    t_dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /opt/dbt && dbt run --select gold.* --profiles-dir .",
    )

    t_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test --profiles-dir .",
    )

    # Ordre d'exécution
    t_ingest >> t_dbt_bronze >> t_dbt_silver >> t_dbt_gold >> t_dbt_test