from typing import Any

from airflow.models.baseoperator import BaseOperator

from .hook import WeatherHook

class WeatherOperator(BaseOperator):
    def __init__(self, weather_connection: str, **kwargs):
        super().__init__()
        self.weather_connection = weather_connection

    def execute(self, context: Any):
        hook = WeatherHook(self.weather_connection)
        weather = hook.get_weather()
        return weather
