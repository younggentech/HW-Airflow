from airflow.models import DAG
import pendulum

from .operator import Task4Operator


default_args = {
    "owner": "al3515",
    "start_date": pendulum.datetime(2024, 6, 4),
}

with DAG(dag_id="task_4",
         default_args=default_args,
         schedule_interval="@hourly",
         catchup=False) as dag:
    get_facts = Task4Operator(task_id="get_facts", dag=dag, do_xcom_push=False)
    get_facts
