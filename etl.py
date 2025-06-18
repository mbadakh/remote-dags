from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from kubernetes.client import models as k8s
from kubernetes import client, config

import datetime

default_args = {
    'start_date': days_ago(1),
}

PVC_NAME = "monday-etl-pvc"

def delete_pvc():
    config.load_incluster_config()
    core_v1 = client.CoreV1Api()
    core_v1.delete_namespaced_persistent_volume_claim(
        name=PVC_NAME,
        namespace="airflow",  # Change if using another namespace
        body=client.V1DeleteOptions()
    )

with DAG(
    dag_id="monday_k8s_etl_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["kubernetes", "etl"]
) as dag:

    volume = k8s.V1Volume(
        name="etl-storage",
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
    )

    volume_mount = k8s.V1VolumeMount(
        mount_path="/app/boards",
        name="etl-storage",
        read_only=False,
    )

    extractor = KubernetesPodOperator(
        task_id="extractor",
        name="monday-extractor",
        image="registry.infinitylabs.co.il/ai/data-infrastructure/monday_extractor:51-65b0d7df",
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
        namespace="airflow",
    )

    transformer = KubernetesPodOperator(
        task_id="transformer",
        name="monday-transformer",
        image="registry.infinitylabs.co.il/ai/data-infrastructure/monday_transformer:58-c24e7ee0",
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
        namespace="airflow",
    )

    cleanup_pvc = PythonOperator(
        task_id="delete_pvc",
        python_callable=delete_pvc
    )

    extractor >> transformer >> cleanup_pvc
