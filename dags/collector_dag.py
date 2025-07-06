from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_data_collection',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description="Сбор данных о погоде и сохранение в PostgreSQL",
)

collect_data = BashOperator(
    task_id='collect_weather_data',
    bash_command='python3 /opt/airflow/dags/weather_collector.py',
    dag=dag
)

collect_data
