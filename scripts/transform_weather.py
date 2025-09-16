def transform_weather(ti):
    raw_data = ti.xcom_pull(task_ids='fetch_weather')

    transformed = {
        'lat': raw_data['lat'],
        'lon': raw_data['lon'],
        'datetime': raw_data['current']['dt'],
        'temperature': raw_data['current']['temp'],
        'humidity': raw_data['current']['humidity'],
        'wind_speed': raw_data['current']['wind_speed'],
        'raw_data': raw_data
    }

    return transformed
