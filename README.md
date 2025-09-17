# 🌤️ Airflow Weather ETL Pipeline

A containerized data pipeline built with Apache Airflow and PostgreSQL that periodically fetches weather data from the
OpenWeather One Call API, transforms it, and stores it in a Postgres database.

- Extracts current weather data for configured coordinates
- Transforms and normalizes the payload
- Loads raw and aggregated results into Postgres tables
- Ships with a one-shot DB init DAG and a scheduled ETL DAG

---

## ✨ Features

- 📥 Extract current weather from OpenWeather One Call API
- 🧭 Configurable coordinates, units, and excluded sections via `config.json`
- 🔁 Scheduled ETL every 5 minutes (customizable)
- 🗄️ PostgreSQL storage with raw JSON preserved (JSONB)
- 🧮 Will be added in the future: Daily summary of data and report (`daily-summary-dag.py`)
- 🐳 Dockerized setup using `docker-compose`

---

## 🛠 Installation

You can run the entire stack using Docker. The Airflow image is extended to install project requirements.

Required software:

- Docker Desktop 4.x+
- Docker Compose v2 (integrated with Docker Desktop)

Clone the repository and build/start the stack:

```bash
# Clone the repository and change into the directory
git clone https://github.com/your-user/airflow-postgres-weather-pipeline.git
cd airflow-postgres-weather-pipeline

# Build and start (Windows PowerShell / macOS / Linux)
docker compose up -d --build
```

This will:

- Build a custom Airflow image using the provided `Dockerfile` that installs packages from `requirements.txt`
- Start `postgres` and `airflow` services as defined in `docker-compose.yaml`
- Mount your local `dags`, `scripts`, `sql`, and `config.json` into the Airflow container

If you prefer a local Python setup instead of Docker, ensure Python 3.10+ and install:

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate
pip install -r requirements.txt
```

---

## ⚙️ Configuration

1. Copy the example config and adjust values
   ```bash
   cp config_example.json config.json
   # Windows (PowerShell)
   Copy-Item config_example.json config.json
   ```
2. Edit `config.json` fields:
    - `api_key`: Your OpenWeather API key
    - `lat`, `lon`: Coordinates to fetch weather for
    - `units`: `metric`, `imperial`, or `standard`
    - `exclude`: Comma-separated parts to exclude (e.g., `minutely,hourly,daily,alerts`)

The file is mounted read-only in the Airflow container at `/opt/airflow/config.json`.

---

## 🚀 Usage

1. Start the stack:
   ```bash
   docker compose up -d --build
   ```
2. Wait for services to initialize. Open Airflow UI at:
    - http://localhost:8080
3. Log in with credentials
    - Docker > Containers > airflow > Logs > 2nd log entry:
    - `Password for user 'admin': ExamplePassword`
4. Initialize the database tables (one-time):
    - DAG: `init-db-dag` (scheduled `@once`) — trigger if not already run
5. Run the ETL:
    - DAG: `weather-etl-dag` — scheduled every 5 minutes by default and will start automatically (
      DAGS_ARE_PAUSED_AT_CREATION=false)

DAG overview:

- `init-db-dag`: Calls `scripts.create_tables.create_tables()` to execute `sql/create_tables.sql` and create tables in
  Postgres.
- `weather-etl-dag`:
    - `extract_weather_from_api_task`: `scripts.fetch_weather.get_weather()` — calls OpenWeather and pushes data via
      XCom (key=`data`).
    - `transform_weather_data_task`: `scripts.transform_weather.transform_weather()` — builds a `city` label and
      converts epoch to timestamp.
    - `load_weather_to_postgres_task`: `scripts.load_weather.load_weather_to_postgres()` — inserts into `weather_raw`
      with `raw_json` preserved.

---

## 📂 Database Schema

Defined in `sql/create_tables.sql`:

- `weather_raw`
    - id SERIAL PRIMARY KEY
    - city TEXT
    - datetime TIMESTAMP
    - temperature FLOAT
    - humidity FLOAT
    - wind_speed FLOAT
    - raw_json JSONB
    - created_at TIMESTAMP DEFAULT NOW()

- `weather_daily_summary` (scaffold for aggregations)
    - id SERIAL PRIMARY KEY
    - city TEXT
    - date DATE
    - avg_temp FLOAT
    - max_temp FLOAT
    - min_temp FLOAT
    - created_at TIMESTAMP DEFAULT NOW()

---

## 📊 Example Query

From the host, after the pipeline has run at least once:

```bash
# Enter the Postgres container
docker compose exec -it postgres psql -U airflow -d weather -c "SELECT city, datetime, temperature, humidity, wind_speed FROM weather_raw ORDER BY id DESC LIMIT 5;"
```

Example output (columns):

| city         | datetime            | temperature | humidity | wind_speed |
|--------------|---------------------|-------------|----------|------------|
| 52.52,13.405 | 2025-09-02 10:44:22 | 23.1        | 58       | 4.2        |

---

## 🧪 Troubleshooting

- Airflow shows no DAGs:
    - Ensure `PYTHONPATH=/opt/airflow` is set (it is configured in `docker-compose.yaml`).
    - Check container logs: `docker compose logs -f airflow`.
- Postgres connection errors:
    - The Airflow tasks connect to host `postgres` (service name) with DB `weather`.
    - Ensure the Postgres healthcheck passes and the `init-db-dag` has run.
- Build fails on psycopg2:
    - Use `psycopg2-binary` to avoid compiling from source.
- API returns errors or empty data:
    - Verify your `config.json` (valid `api_key`, allowed `lat`/`lon`, and `units`).

---

## 📜 License

This project is licensed under the GNU GPL-3.0 License — see the LICENSE file
or https://www.gnu.org/licenses/gpl-3.0.en.html for details.

---

## 🧩 Third-Party Dependencies

- Apache Airflow (base image)
- psycopg2-binary
- requests

See `requirements.txt` for exact versions.

---

## 🔒 Disclaimer

This project is intended for educational and personal use. Ensure your use of the OpenWeather API complies with their
Terms of Service and local regulations.

---

## 🙌 Contribution

Contributions are welcome! Please open issues and pull requests for enhancements, bug fixes, or documentation
improvements.