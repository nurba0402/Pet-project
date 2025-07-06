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
    'load_weather_data',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description="Сбор данных о погоде и сохранение в PostgreSQL",
)

load_data_to_dds = BashOperator(
    task_id='load_weather_data',
    bash_command='python3 /opt/airflow/dags/dds_for_weather.py',
    dag=dag
)

quality_checks = BashOperator(
    task_id='quality_checks',
    bash_command='python3 /opt/airflow/dags/weather_data_quality.py',
    dag=dag
)

load_data_to_dds >> quality_checks