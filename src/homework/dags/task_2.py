from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
import pendulum

default_args = {
    "owner": "al3515",
    "start_date": pendulum.datetime(2024, 6, 4, tz="UTC"),
}

with DAG(dag_id="task_2",
         schedule_interval="@hourly",
         default_args=default_args,
         catchup=False) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    stop_task = DummyOperator(task_id="stop_task", dag=dag)
    task_1 = DummyOperator(task_id="t_1", dag=dag)
    tasks_2 = []
    task_4 = DummyOperator(task_id="t_4", dag=dag)
    end = DummyOperator(task_id="end", dag=dag)
    for task_num in range(1, 4):
        task_2_level = DummyOperator(task_id=f"t_2_{task_num}", dag=dag)
        task_3_level = DummyOperator(task_id=f"t_3_{task_num}", dag=dag)
        task_2_level >> task_3_level >> task_4
        tasks_2.append(task_2_level)
    task_1.set_downstream(tasks_2)
    start >> [stop_task, task_1]
    task_4 >> end
