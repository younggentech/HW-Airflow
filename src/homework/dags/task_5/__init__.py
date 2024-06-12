from airflow.models import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from .hook import WeatherHook


default_args = {
    'owner': 'airflow',
    "start_date": pendulum.datetime(2024, 6, 4),
}


with DAG(dag_id='task_5', default_args=default_args, schedule_interval='*/30 * * * *', catchup=False) as dag:
    create_table = PostgresOperator(task_id="create_table",
                                    postgres_conn_id='RESULT_DB',
                                    sql="sql/create_table.sql")

    @task
    def get_data_and_save_to_db(city: str):
        weather = WeatherHook(weather_conn="weather_conn")
        temperature, humidity, feel_temperature = weather.get_weather(city)
        postgres = PostgresHook(postgres_conn_id="RESULT_DB")
        query = """
        INSERT INTO weather.main_metrics (city, temperature, humidity, feel_temperature) VALUES (%s, %s, %s, %s)
        """
        postgres.run(query, parameters=(city, temperature, humidity, feel_temperature))

    CITIES = ("Moscow", "London", "Dubai")
    tasks = []
    for city in CITIES:
        tasks.append(get_data_and_save_to_db(city))
    create_table.set_downstream(tasks)
