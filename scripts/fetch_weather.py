import json
import os

import requests


def get_weather(ti):
    print("Start: Extract weather data")
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)

    api_key = config['api_key']
    lat = config['lat']
    lon = config['lon']
    units = config['units']
    exclude = config['exclude']

    url = f'https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={exclude}&appid={api_key}&units={units}'
    response = requests.get(url)
    data = response.json()
    ti.xcom_push(key='data', value=data)
    print("Finished: Extract weather data")
