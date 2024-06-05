import logging

import requests
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class WeatherHook(BaseHook):
    def __init__(self, weather_conn: str, **kwargs):
        super().__init__(**kwargs)
        self.weather_conn = weather_conn


    def get_weather(self, city: str):
        url, key = self._get_url_and_key()
        response = requests.get(url + "/current.json", params={"key": key, "q": city})
        logger = logging.getLogger(__name__)
        logger.info(f"Response to request to {url + '/current.json'}: {response.status_code}, {response.text}")
        temperature = response.json()["current"]["temp_c"]
        humidity = response.json()["current"]["humidity"]
        feelslike_c = response.json()["current"]["feelslike_c"]
        return temperature, humidity, feelslike_c

    def _get_url_and_key(self):
        connection = self.get_connection(self.weather_conn)
        if not connection.password:
            raise AirflowException("No API Key provided")
        if not connection.host:
            raise AirflowException("No host provided")
        return connection.host, connection.password
