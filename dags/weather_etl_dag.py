from datetime import datetime, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from scripts.extract_transform_load_raw import get_weather, load_weather_to_postgres, transform_weather

with DAG(
        dag_id='weather_etl',
        start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
        schedule='0,5,10,15,20,25,30,35,40,45,50,55 * * * *',
        catchup=False
):
    # Wait for the one-time init DB DAG to complete successfully
    wait_for_init = ExternalTaskSensor(
        task_id='wait_for_init_db',
        external_dag_id='init_db',
        external_task_id='create_tables',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        execution_date_fn=lambda _: datetime(2025, 9, 1, tzinfo=timezone.utc),
        mode='reschedule',
        poke_interval=60,
        timeout=60 * 60,
    )

    extract_weather_from_api = PythonOperator(
        task_id='extract_weather_from_api',
        python_callable=get_weather,
        do_xcom_push=False
    )

    transform_weather_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather,
        do_xcom_push=False
    )

    load_weather_to_postgres = PythonOperator(
        task_id='load_weather_to_postgres',
        python_callable=load_weather_to_postgres,
        do_xcom_push=False
    )

    wait_for_init >> extract_weather_from_api >> transform_weather_data >> load_weather_to_postgres
