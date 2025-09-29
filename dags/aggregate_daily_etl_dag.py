# -*- coding: utf-8 -*-
# ======================================
# Airflow DAG: Aggregate Daily Weather
# ======================================
# Defines the DAG that aggregates daily weather metrics from interval data
# and loads a daily summary into Postgres.
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
from scripts.aggregate_daily_data import get_postgres, transform_to_df, aggregate_daily_data, load_daily_data

with DAG(
        dag_id='aggregate_daily',
        start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
        schedule='@daily',
        catchup=False
):
    get_postgres_data = PythonOperator(
        task_id='get_postgres_data',
        python_callable=get_postgres,
        do_xcom_push=False
    )

    transform_to_df = PythonOperator(
        task_id='transform_to_df',
        python_callable=transform_to_df,
        do_xcom_push=False
    )

    aggregate_daily_data = PythonOperator(
        task_id='aggregate_daily_data',
        python_callable=aggregate_daily_data,
        do_xcom_push=False
    )

    load_daily_data = PythonOperator(
        task_id='load_daily_data',
        python_callable=load_daily_data,
        do_xcom_push=False
    )

    get_postgres_data >> transform_to_df >> aggregate_daily_data >> load_daily_data
