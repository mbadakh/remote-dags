import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 7),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "sla": timedelta(hours=23),
}

dag = DAG(
    dag_id="example_using_k8s_executor",
    schedule_interval="*/2 * * * *",
    catchup=False,
    default_args=default_args,
)

# Task 1: run "alpine:latest" and echo "hello from task-1"
task_1 = KubernetesPodOperator(
    namespace="default",                      # Kubernetes namespace
    image="alpine:latest",                    # Replace with your image
    cmds=["/bin/sh", "-c"],
    arguments=["echo 'hello from task-1'"],
    labels={"task": "task-1"},                # Optional: Kubernetes labels
    name="task-1-pod",                        # Pod name (must be unique per run)
    task_id="task-1",                         # Airflow task_id
    get_logs=True,                            # Stream pod logs back to Airflow
    dag=dag,
)

# Task 2: run "alpine:latest" and echo "hello from task-2"
task_2 = KubernetesPodOperator(
    namespace="default",
    image="alpine:latest",                    # Replace with your image
    cmds=["/bin/sh", "-c"],
    arguments=["echo 'hello from task-2'"],
    labels={"task": "task-2"},
    name="task-2-pod",
    task_id="task-2",
    get_logs=True,
    dag=dag,
)

task_1 >> task_2
