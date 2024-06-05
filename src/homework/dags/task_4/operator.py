from typing import Any
import logging

from airflow.models.baseoperator import BaseOperator

from .hook import Task4Hook


class Task4Operator(BaseOperator):
    def __init__(self, conn_id: str = "http_default", **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def execute(self, context: Any):
        fact_hook = Task4Hook(self.conn_id)
        logger = logging.getLogger(__name__)
        fact = fact_hook.get_fact()
        logger.info(fact)
        return fact
