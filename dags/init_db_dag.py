# -*- coding: utf-8 -*-
# ======================================
# Airflow DAG: Initialize Database
# ======================================
# Defines the one-shot DAG that creates required Postgres tables for the pipeline.
#
# Author: Sergej Lembke
# License: See LICENSE file
# ======================================

# --- Standard library imports ---
from datetime import datetime, timezone

# --- Third-party imports ---
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# --- Local module imports ---
from scripts.create_tables import create_tables

# A one-shot DAG that initializes the database schema.
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
