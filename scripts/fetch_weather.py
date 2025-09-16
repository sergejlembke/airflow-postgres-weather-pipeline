import os

import requests
import json

def get_weather(lat, lon, api_key):
    url = f'https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={api_key}'
    response = requests.get(url)
    data = response.json()
    return data

def get_api_key():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config['api_key']

lat = 52.52437
lon = 13.41053

api_key = get_api_key()

weather = get_weather(lat, lon, api_key)
print(json.dumps(weather, indent=4))

