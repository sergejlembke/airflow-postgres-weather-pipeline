import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor, DictRow


def get_postgres_raw(ti):
    try:
        print("Connecting to Postgres")
        # If running this script on the host machine, use localhost and port 5432.
        # If running inside the Airflow container, use host="postgres" (the docker-compose service name).
        conn = psycopg2.connect(
            dbname="weather",
            user="airflow",
            password="airflow",
            host="postgres"
        )
        # Use DictCursor so each row is a DictRow supporting mapping-style access
        cur = conn.cursor(cursor_factory=DictCursor)

        print("Start: Get data from db")
        cur.execute('SELECT * FROM weather_raw')
        data_raw: list[DictRow] = cur.fetchall()

        cur.close()
        conn.close()
        print("Closed connection to Postgres")

        # Push to XCom for downstream tasks
        ti.xcom_push(key='data_postgres_raw', value=data_raw)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def transform_raw_to_df(ti):
    print("Start: Aggregating weather data")
    # Pull the data pushed by the extract task under key='data_postgres_raw'
    data_raw = ti.xcom_pull(task_ids='get_postgres_raw_task', key='data_postgres_raw')
    if data_raw is None:
        raise ValueError("No data received from 'get_postgres_raw_task'. Ensure the extract task pushes data or returns it.")

    # Convert list[DictRow] -> list[dict] to ensure pandas builds columns correctly
    rows_as_dicts = [dict(row) for row in data_raw]

    # Build DataFrame
    df = pd.DataFrame(rows_as_dicts)

    # Optionally, convert timestamp-like columns to pandas datetime if present
    for col in ("datetime", "created_at"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Push DataFrame to XCom for downstream tasks; additionally push a dict-of-records fallback
    ti.xcom_push(key='data_raw_df', value=df)
    ti.xcom_push(key='data_raw_records', value=df.to_dict(orient='records'))
    print("Finished: Aggregating weather data -> DataFrame shape:", df.shape)
    return df
