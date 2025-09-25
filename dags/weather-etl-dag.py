from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from scripts.extract_transform_load_raw import get_weather, load_weather_to_postgres, transform_weather

with DAG(
        dag_id='weather-etl-dag',
        start_date=datetime(2025, 9, 1),
        schedule="0,5,10,15,20,25,30,35,40,45,50,55 * * * *",
        catchup=False
):
    extract_weather_from_api = PythonOperator(
        task_id='extract_weather_from_api_task',
        python_callable=get_weather,
        do_xcom_push=False
    )

    transform_weather_data = PythonOperator(
        task_id='transform_weather_data_task',
        python_callable=transform_weather,
        do_xcom_push=False
    )

    load_weather_to_postgres = PythonOperator(
        task_id='load_weather_to_postgres_task',
        python_callable=load_weather_to_postgres,
        do_xcom_push=False
    )

    extract_weather_from_api >> transform_weather_data >> load_weather_to_postgres
