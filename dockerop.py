from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="example_docker_on_celery",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = DockerOperator(
        task_id="run_my_container",
        image="python:3.11-slim",
        api_version="auto",
        auto_remove=True,
        command="python -c \"print('Hello from inside DockerOperator')\"",
        docker_url="unix://var/run/docker.sock",  # default
        network_mode="bridge",
    )