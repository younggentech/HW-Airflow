import requests
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class Task4Hook(BaseHook):
    def __init__(self, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def get_fact(self) -> str:
        url = "https://catfact.ninja/fact"
        response = requests.get(url)
        return response.json()["fact"]
