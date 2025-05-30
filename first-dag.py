from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import (
    V1Volume,
    V1PersistentVolumeClaimVolumeSource,
    V1VolumeMount,
)

# Define a shared volume (only needed if you didn't set extraVolumes in values.yaml)
shared_volume = V1Volume(
    name="shared-data",
    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
        claim_name="shared-pvc"
    ),
)
shared_mount = V1VolumeMount(name="shared-data", mount_path="/shared")

default_args = {
    "start_date": datetime(2025, 5, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="first_k8s_pod_dag",
    schedule_interval=None,  # manual trigger
    default_args=default_args,
    catchup=False,
    tags=["example"],
) as dag:

    task1 = KubernetesPodOperator(
        namespace="airflow",
        image="your.registry.com/image1:latest",
        cmds=["sh", "-c"],
        arguments=["echo task1 > /shared/output1.txt"],
        name="task1",
        task_id="task1",
        volumes=[shared_volume],
        volume_mounts=[shared_mount],
    )

    task2 = KubernetesPodOperator(
        namespace="airflow",
        image="your.registry.com/image2:latest",
        cmds=["sh", "-c"],
        arguments=["cat /shared/output1.txt && echo task2 >> /shared/output2.txt"],
        name="task2",
        task_id="task2",
        volumes=[shared_volume],
        volume_mounts=[shared_mount],
    )

    # Similarly for task3 and task4...
    task3 = KubernetesPodOperator(
        namespace="airflow",
        image="your.registry.com/image3:latest",
        cmds=["sh", "-c"],
        arguments=["cat /shared/output2.txt && echo task3 >> /shared/output3.txt"],
        name="task3",
        task_id="task3",
        volumes=[shared_volume],
        volume_mounts=[shared_mount],
    )

    task4 = KubernetesPodOperator(
        namespace="airflow",
        image="your.registry.com/image4:latest",
        cmds=["sh", "-c"],
        arguments=["cat /shared/output3.txt && echo task4 >> /shared/output4.txt"],
        name="task4",
        task_id="task4",
        volumes=[shared_volume],
        volume_mounts=[shared_mount],
    )

    task1 >> task2 >> task3 >> task4
