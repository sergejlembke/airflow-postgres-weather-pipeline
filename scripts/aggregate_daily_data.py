# -*- coding: utf-8 -*-
# ======================================
# Aggregation Utility Functions (Daily)
# ======================================
# This module contains Airflow task callables that:
# - extract yesterday's interval data from Postgres,
# - transform it into a pandas DataFrame,
# - compute daily aggregates
# - insert a daily summary row into Postgres.
#
# Author: Sergej Lembke
# License: See LICENSE file
# ======================================

# --- Third-party imports ---
import logging

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor, DictRow

logger = logging.getLogger(__name__)


def get_postgres(ti):
    """Fetch yesterday's interval records from Postgres and push as list of dicts to XCom."""
    try:
        logger.info("Connecting to Postgres")
        # If running this script on the host machine, use localhost and port 5432.
        # If running inside the Airflow container, use host="postgres" (the docker-compose service name).
        conn = psycopg2.connect(
            dbname='weather',
            user='airflow',
            password='airflow',
            host='postgres'
        )
        with conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                logger.info("Start: Get data from db")
                cur.execute(
                    """
                    SELECT lat_lon,
                           datetime,
                           temperature
                    FROM weather_interval
                    WHERE datetime >= CURRENT_DATE - INTERVAL '1 day'
                      AND datetime < CURRENT_DATE
                    """
                )
                data: list[DictRow] = cur.fetchall()
        logger.info("Closed connection to Postgres")
        rows_as_dicts = [dict(row) for row in data]
        ti.xcom_push(key='data_postgres', value=rows_as_dicts)
    except psycopg2.DatabaseError:
        logger.exception("Database error while fetching interval data from Postgres")
        raise
    except Exception:
        logger.exception("Unexpected error while fetching interval data from Postgres")
        raise


def transform_to_df(ti):
    """Transform list of dicts from XCom into DataFrame and push back as list-of-dicts.

    Airflow's default XCom backend uses JSON serialization. Avoid pushing
    raw pandas DataFrame, instead push a list of dicts (records).
    """
    logger.info("Start: Transform weather data into DataFrame")
    # Pull the data pushed by the extract task under key='data_postgres'
    data = ti.xcom_pull(task_ids='get_postgres_data', key='data_postgres')
    if data is None:
        raise ValueError("No data received from task 'get_postgres_data'. Ensure the task pushes data or returns it.")

    # Build DataFrame directly from list of dicts
    df = pd.DataFrame(data)

    # Optionally, convert timestamp-like columns to pandas datetime if present
    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')

    # Push JSON-serializable records to XCom and also return the DataFrame
    ti.xcom_push(key='data_df', value=df.to_dict(orient='records'))
    logger.info("Finished: Transforming weather data into DataFrame")
    return df


def aggregate_daily_data(ti):
    """Compute average, max, and min temperature for the day and push list of floats."""
    logger.info("Start: Aggregate daily data")
    # Pull the data pushed by the transform task under key='data_df' (list of dicts)
    df_records = ti.xcom_pull(task_ids='transform_to_df', key='data_df')
    if df_records is None:
        raise ValueError("No data received from task 'transform_to_df'. Ensure the task pushes data or returns it.")

    df = pd.DataFrame(df_records)
    if df.empty or 'temperature' not in df.columns:
        raise ValueError("DataFrame is empty or lacks 'temperature' column for aggregation.")

    avg_temp = float(pd.to_numeric(df['temperature'], errors='coerce').mean())
    max_temp = float(pd.to_numeric(df['temperature'], errors='coerce').max())
    min_temp = float(pd.to_numeric(df['temperature'], errors='coerce').min())

    daily_temp_list = [avg_temp, max_temp, min_temp]
    # Push aggregates to XCom for downstream tasks and also return the list for visibility
    ti.xcom_push(key='agg_daily', value=daily_temp_list)
    logger.info("Finished: Aggregating daily data")
    return daily_temp_list


def load_daily_data(ti):
    """Insert the daily summary row into Postgres from previously computed aggregates."""
    logger.info("Start: Loading daily data")
    # Pull the data pushed by the aggregate task under key='agg_daily'
    daily_temp_list = ti.xcom_pull(task_ids='aggregate_daily_data', key='agg_daily')
    if daily_temp_list is None or not isinstance(daily_temp_list, (list, tuple)) or len(daily_temp_list) < 3:
        raise ValueError("Invalid or missing data from task 'aggregate_daily_data' under key 'agg_daily' (expected a list with 3 values).")

    # Pull the data pushed by the transform task under key='data_df' and reconstruct DataFrame
    df_records = ti.xcom_pull(task_ids='transform_to_df', key='data_df')
    df = pd.DataFrame(df_records or [])
    if df.empty:
        raise ValueError("Invalid or missing DataFrame data from task 'transform_to_df' under key 'data_df'.")
    try:
        logger.info("Connecting to Postgres")
        conn = psycopg2.connect(
            dbname='weather',
            user='airflow',
            password='airflow',
            host='postgres'
        )
        with conn:
            with conn.cursor() as cur:
                lat_lon = df['lat_lon'].iloc[0] if 'lat_lon' in df.columns else None
                # Derive date value from 'date' or 'datetime' column, coercing to datetime
                raw_date = None
                if 'date' in df.columns:
                    raw_date = df['date'].iloc[0]
                elif 'datetime' in df.columns:
                    raw_date = df['datetime'].iloc[0]

                date_val = pd.to_datetime(raw_date, errors='coerce') if raw_date is not None else None
                if pd.notna(date_val):
                    date_val = date_val.date()
                else:
                    raise ValueError("No valid date in DataFrame (columns 'date' or 'datetime').")

                def _to_float(v):
                    try:
                        return float(v) if v is not None else None
                    except Exception:
                        return None

                avg_val = _to_float(daily_temp_list[0])
                max_val = _to_float(daily_temp_list[1])
                min_val = _to_float(daily_temp_list[2])
                cur.execute(
                    """
                    INSERT INTO weather_daily_summary (lat_lon, date, avg_temp, max_temp, min_temp)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (lat_lon, date_val, avg_val, max_val, min_val)
                )
        logger.info("Finished: Loading daily data")
    except psycopg2.DatabaseError:
        logger.exception("Database error while inserting daily summary into Postgres")
        raise
    except Exception:
        logger.exception("Unexpected error while inserting daily summary into Postgres")
        raise
