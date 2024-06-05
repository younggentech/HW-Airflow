from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
import pendulum


default_args = {
    "owner": "al3515",
    "start_date": pendulum.datetime(2024, 6, 4, tz="UTC")
}

with DAG(dag_id="task_1",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    op1 = DummyOperator(task_id="op1", dag=dag)
    op2 = DummyOperator(task_id="op2", dag=dag)
    some_other_task = DummyOperator(task_id="some-other-task", dag=dag)
    op3 = DummyOperator(task_id="op3", dag=dag)
    op4 = DummyOperator(task_id="op4", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)
    start >> [op1, op2] >> some_other_task >> [op3, op4] >> end
