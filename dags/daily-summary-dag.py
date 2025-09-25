# from datetime import datetime
#
# from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator
#
# with DAG(
#         dag_id='daily-report-dag',
#         start_date=datetime(2025, 9, 1),
#         schedule="58 23 * * *"
# ):
#     get_weather_data = PythonOperator(
#         task_id='get_weather_data_task',
#         python_callable=get_weather_data,
#     )
