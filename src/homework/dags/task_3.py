from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.bash import BashOperator
import pendulum

default_args = {
    "owner": "al3515",
    "start_date": pendulum.datetime(2024, 6, 4),
}

def after_response():
    print("the request is done")


with DAG(dag_id="task_3", schedule_interval="@daily",
         default_args=default_args, catchup=False) as dag:
    http_task = HttpOperator(task_id="retrieve", endpoint="catfact.ninja/fact",
                             method="GET", dag=dag,
                             http_conn_id="http_default",
                             response_check=lambda response: "fact" in response.json(),
)
    python_task = PythonOperator(task_id="python", python_callable=after_response)
    email_task = BashOperator(task_id="bash_operator", dag=dag, bash_command="echo 'all done'")
    http_task >> [python_task, email_task]
