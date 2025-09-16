import json

import psycopg2


def load_weather_to_postgres(ti):
    print("Start: Loading weather data")
    data = ti.xcom_pull(task_ids='transform_weather_data_task', key='transformed')

    # If running this script on the host machine, use localhost and port 5432.
    # If running inside the Airflow container, use host="postgres" (the docker-compose service name).
    conn = psycopg2.connect(
        dbname="weather",
        user="airflow",
        password="airflow",
        host="postgres"
    )
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO weather_raw (city, datetime, temperature, humidity, wind_speed, raw_json)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            data['city'],
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
