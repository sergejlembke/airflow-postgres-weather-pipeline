CREATE TABLE IF NOT EXISTS weather_interval
(
    id          SERIAL PRIMARY KEY,
    lat_lon     TEXT,
    datetime    TIMESTAMP,
    temperature FLOAT,
    humidity    FLOAT,
    wind_speed  FLOAT,
    raw_json    JSONB,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS weather_daily_summary
(
    id         SERIAL PRIMARY KEY,
    lat_lon    TEXT,
    date       DATE,
    avg_temp   FLOAT,
    max_temp   FLOAT,
    min_temp   FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);