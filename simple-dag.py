# ملف: test_simple_dag.py
# ضعه داخل مجلد dags/ في تثبيت Airflow الخاص بك.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow_user",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 5, 30),
}

with DAG(
    dag_id="test_simple_dag",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=["test"],
) as dag:

    task_print_date = BashOperator(
        task_id="print_date",
        bash_command="echo 'Current date is: $(date)'",
    )

    task_create_file = BashOperator(
        task_id="create_file",
        bash_command="echo 'Airflow is working!' > /tmp/airflow_test.txt",
    )

    task_print_date >> task_create_file
