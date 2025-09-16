import os

import psycopg2


def create_tables():
    # If running this script on the host machine, use localhost and port 5432.
    # If running inside the Airflow Docker container, use host="postgres" (the docker-compose service name).
    conn = psycopg2.connect(
        dbname="weather",
        user="airflow",
        password="airflow",
        host="postgres"
    )
    cur = conn.cursor()

    # create_tables.sql is located in the project-root/sql directory
    root_dir = os.path.dirname(os.path.dirname(__file__))
    sql_path = os.path.join(root_dir, "sql", "create_tables.sql")
    with open(sql_path, "r") as f:
        sql = f.read()
    cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()
