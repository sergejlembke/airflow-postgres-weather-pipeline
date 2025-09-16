from datetime import datetime


def transform_weather(ti):
    print("Start: Transforming weather data")
    # Pull the data pushed by the extract task under key='data'
    raw_data = ti.xcom_pull(task_ids='extract_weather_from_api_task', key='data')
    if raw_data is None:
        raise ValueError("No data received from 'extract_weather_from_api_task'. Ensure the extract task pushes data or returns it.")

    # Build a city label from coordinates and convert epoch to timestamp
    city_label = f"{raw_data['lat']},{raw_data['lon']}"
    transformed = {
        'city': city_label,
        'datetime': datetime.fromtimestamp(raw_data['current']['dt']),
        'temperature': raw_data['current']['temp'],
        'humidity': raw_data['current']['humidity'],
        'wind_speed': raw_data['current']['wind_speed'],
        'raw_data': raw_data
    }

    ti.xcom_push(key='transformed', value=transformed)
    print("Finished: Transforming weather data")
