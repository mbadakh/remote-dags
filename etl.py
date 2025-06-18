from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from kubernetes import client, config

PVC_NAME = "monday-etl-pvc"
PVC_SIZE = "1Gi"
PVC_NAMESPACE = "airflow"  # Change this if your Airflow runs in a different namespace

default_args = {
    'start_date': days_ago(1),
}

def create_pvc():
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=PVC_NAME),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteOnce"],
            resources=client.V1ResourceRequirements(
                requests={"storage": PVC_SIZE}
            )
        )
    )
    v1.create_namespaced_persistent_volume_claim(namespace=PVC_NAMESPACE, body=pvc)

def delete_pvc():
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    v1.delete_namespaced_persistent_volume_claim(
        name=PVC_NAME,
        namespace=PVC_NAMESPACE,
        body=client.V1DeleteOptions()
    )

with DAG(
    dag_id="monday_k8s_etl_dag_dynamic_pvc",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["kubernetes", "etl"],
) as dag:

    create_pvc_task = PythonOperator(
        task_id="create_pvc",
        python_callable=create_pvc,
    )

    volume = client.V1Volume(
        name="etl-storage",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
    )

    volume_mount = client.V1VolumeMount(
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
        namespace=PVC_NAMESPACE,
    )

    transformer = KubernetesPodOperator(
        task_id="transformer",
        name="monday-transformer",
        image="registry.infinitylabs.co.il/ai/data-infrastructure/monday_transformer:58-c24e7ee0",
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
        namespace=PVC_NAMESPACE,
    )

    delete_pvc_task = PythonOperator(
        task_id="delete_pvc",
        python_callable=delete_pvc,
        trigger_rule="all_done",  # Ensure PVC is deleted even if previous tasks fail
    )

    create_pvc_task >> extractor >> transformer >> delete_pvc_task
