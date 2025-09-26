from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from scripts.create_tables import create_tables

# A one-shot DAG that initializes the database schema.
# This avoids running DDL on every ETL schedule while remaining idempotent.
with DAG(
        dag_id="init_db",
        start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
        schedule="@once",
        catchup=True,
        description="Initialize Postgres tables for the weather pipeline",
):
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )
