import psycopg
import json
from datetime import datetime

def load_weather_to_postgres(data, city="Berlin"):
    conn = psycopg.connect(
        dbname="weather",
        user="airflow",
        password="airflow",
        host="postgres"
    )
    cur = conn.cursor()

    cur.execute(
        """
        # language=sql
        INSERT INTO weather_raw (city, datetime, temperature, humidity, wind_speed, raw_json)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
        city,
        datetime.utcfromtimestamp(data["dt"]),
        data["main"]["temp"],
        data["main"]["humidity"],
        data["wind"]["speed"],
        json.dumps(data)
    ))

    conn.commit()
    cur.close()
    conn.close()