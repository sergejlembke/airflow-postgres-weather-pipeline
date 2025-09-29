# -*- coding: utf-8 -*-
# ======================================
# ETL Utility Functions (Raw Interval)
# ======================================
# Helper functions to extract weather data from OpenWeather API, transform it,
# and load the interval records into Postgres.
#
# Author: Sergej Lembke
# License: See LICENSE file
# ======================================

# --- Standard library imports ---
import json
import os
from datetime import datetime
from typing import Any, Dict

# --- Third-party imports ---
import psycopg2
import requests


def get_weather(ti) -> Dict[str, Any]:
    """
    Fetch weather data from OpenWeather One Call API and push to XCom.

    Notes on failures:
    - One Call 3.0 API requires a paid subscription (1000 requests per day for free).
      If the API key does not have a One Call subscription, OpenWeather will respond
      with 401/403 and a JSON body like {"cod":"401", "message":"Invalid API key"} or
      a message indicating subscription is required.
    - PythonOperator log shows the actual value.
    """
    print("Start: Extract weather data")

    # Load config from the repository root
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    api_key = config['api_key']
    lat = config['lat']
    lon = config['lon']
    units = config.get('units', 'metric')
    exclude = config.get('exclude', [])

    # Normalize exclude to comma-separated string
    if isinstance(exclude, (list, tuple)):
        exclude_param = ','.join(exclude)
    else:
        exclude_param = str(exclude)

    # API endpoint (requires subscription on OpenWeather)
    url = (
        'https://api.openweathermap.org/data/3.0/onecall'
        f'?lat={lat}&lon={lon}&exclude={exclude_param}&appid={api_key}&units={units}'
    )

    response = None
    try:
        response = requests.get(url, timeout=30)
        # Raise for obvious HTTP errors
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        # Include body text for diagnostics (e.g., subscription required)
        body = getattr(response, 'text', '')
        raise RuntimeError(
            f"OpenWeather API request failed with status {response.status_code}: {body}"
        ) from http_err
    except requests.exceptions.RequestException as req_err:
        raise RuntimeError(f"OpenWeather API request error: {req_err}") from req_err

    # Parse JSON and validate
    try:
        data = response.json()
    except ValueError as json_err:
        raise RuntimeError(f"OpenWeather API returned non-JSON response: {response.text}") from json_err

    # Push to XCom for downstream tasks and also return the data for visibility
    ti.xcom_push(key='data_api_raw', value=data)
    print("Finished: Extract weather data")
    return data


def transform_weather(ti):
    print("Start: Transforming weather data")
    # Pull the data pushed by the extract task under key='data_api_raw'
    data_raw = ti.xcom_pull(task_ids='extract_weather_from_api', key='data_api_raw')
    if data_raw is None:
        raise ValueError("No data received from task 'extract_weather_from_api'. Ensure the extract task pushes data or returns it.")

    data_transformed = {
        'lat_lon': f"{data_raw['lat']}, {data_raw['lon']}",
        'datetime': datetime.fromtimestamp(data_raw['current']['dt']),
        'temperature': data_raw['current']['temp'],
        'humidity': data_raw['current']['humidity'],
        'wind_speed': data_raw['current']['wind_speed'],
        'raw_data': data_raw
    }

    # Push to XCom for downstream tasks and also return the data for visibility
    ti.xcom_push(key='data_transformed', value=data_transformed)
    print("Finished: Transforming weather data")
    return data_transformed


def load_weather_to_postgres(ti):
    print("Start: Loading weather data")
    # Pull the data pushed by the transform task under key='data_transformed'
    data = ti.xcom_pull(task_ids='transform_weather_data', key='data_transformed')

    try:
        print("Connecting to Postgres")
        # If running this script on the host machine, use localhost and port 5432.
        # If running inside the Airflow container, use host="postgres" (the docker-compose service name).
        conn = psycopg2.connect(
            dbname='weather',
            user='airflow',
            password='airflow',
            host='postgres'
        )
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO weather_interval (lat_lon, datetime, temperature, humidity, wind_speed, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                data['lat_lon'],
                data['datetime'],
                data['temperature'],
                data['humidity'],
                data['wind_speed'],
                json.dumps(data['raw_data'])
            )
        )

        conn.commit()
        cur.close()
        conn.close()

        print("Finished: Loading weather data")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
