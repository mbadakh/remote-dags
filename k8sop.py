from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='kubernetes_pod_operator_example',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    task_1 = KubernetesPodOperator(
        task_id="task_1",
        name="task-1",
        namespace="default",
        image="alpine",
        cmds=["sh", "-c"],
        arguments=["echo 'Hello from Task 1' && sleep 5"],
        is_delete_operator_pod=True,
    )

    task_2 = KubernetesPodOperator(
        task_id="task_2",
        name="task-2",
        namespace="default",
        image="alpine",
        cmds=["sh", "-c"],
        arguments=["echo 'Hello from Task 2'"],
        is_delete_operator_pod=True,
    )

    task_1 >> task_2
