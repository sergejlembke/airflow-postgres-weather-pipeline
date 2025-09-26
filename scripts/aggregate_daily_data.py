import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor, DictRow


def get_postgres(ti):
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
        # Use DictCursor so each row is a DictRow supporting mapping-style access
        cur = conn.cursor(cursor_factory=DictCursor)

        print("Start: Get data from db")
        cur.execute("""
                    SELECT lat_lon,
                           datetime,
                           temperature
                    FROM weather_interval
                    WHERE datetime >= CURRENT_DATE - INTERVAL '1 day'
                      AND datetime < CURRENT_DATE
                    """)
        data: list[DictRow] = cur.fetchall()
        # Convert rows to plain dicts for safe XCom serialization and downstream processing
        rows_as_dicts = [dict(row) for row in data]

        cur.close()
        conn.close()
        print("Closed connection to Postgres")

        # Push to XCom for downstream tasks
        ti.xcom_push(key='data_postgres', value=rows_as_dicts)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def transform_to_df(ti):
    print("Start: Transform weather data into DataFrame")
    # Pull the data pushed by the extract task under key='data_postgres'
    data = ti.xcom_pull(task_ids='get_postgres_data', key='data_postgres')
    if data is None:
        raise ValueError("No data received from task 'get_postgres'. Ensure the extract task pushes data or returns it.")

    # Build DataFrame directly from list of dicts
    df = pd.DataFrame(data)

    # # Optionally, convert timestamp-like columns to pandas datetime if present
    # for col in ('datetime', 'created_at'):
    #     if col in df.columns:
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

    # Push DataFrame to XCom for downstream tasks and also return the data for visibility
    ti.xcom_push(key='data_df', value=df)
    print("Finished: Transforming weather data into DataFrame")
    return df


def aggregate_daily_data(ti):
    print("Start: Aggregate daily data")
    # Pull the data pushed by the transform task under key='data_df'
    df = ti.xcom_pull(task_ids='transform_to_df', key='data_df')
    if df is None:
        raise ValueError("No data received from task 'transform_to_df'. Ensure the extract task pushes data or returns it.")

    avg_temp = df['temperature'].mean()
    max_temp = df['temperature'].max()
    min_temp = df['temperature'].min()

    daily_temp_list = [avg_temp, max_temp, min_temp]
    # Push DataFrame to XCom for downstream tasks and also return the data for visibility
    ti.xcom_push(key='agg_daily', value=daily_temp_list)
    print("Finished: Aggregating daily data")
    return [avg_temp, max_temp, min_temp]


def load_daily_data(ti):
    print("Start: Loading daily data")
    # Pull the data pushed by the aggregate task under key='agg_daily'
    daily_temp_list = ti.xcom_pull(task_ids='aggregate_daily_data', key='agg_daily')
    if daily_temp_list is None:
        raise ValueError("No data received from task 'aggregate_daily_data'. Ensure the extract task pushes data or returns it.")

    # Pull the data pushed by the transform task under key='data_df'
    df = ti.xcom_pull(task_ids='transform_to_df', key='data_df')
    if df is None:
        raise ValueError("No data received from task 'transform_to_df'. Ensure the extract task pushes data or returns it.")

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
            INSERT INTO weather_daily_summary (lat_lon, date, avg_temp, max_temp, min_temp)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                (df['lat_lon'].iloc[0] if 'lat_lon' in df.columns else None),
                (
                    pd.to_datetime(df['date'].iloc[0]).date() if 'date' in df.columns and not pd.isna(df['date'].iloc[0])
                    else (pd.to_datetime(df['datetime'].iloc[0]).date() if 'datetime' in df.columns else None)
                ),
                float(daily_temp_list[0]) if daily_temp_list[0] is not None else None,
                float(daily_temp_list[1]) if daily_temp_list[1] is not None else None,
                float(daily_temp_list[2]) if daily_temp_list[2] is not None else None
            )
        )

        conn.commit()
        cur.close()
        conn.close()

        print("Finished: Loading daily data")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
