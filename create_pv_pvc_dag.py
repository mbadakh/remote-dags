from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.kubernetes.secret import Secret
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from kubernetes import client
from k8sUtils import create_pv_and_pvc, delete_pv_and_pvc

PVC_NAME = "monday-etl-pvc"
PV_NAME = "monday-etl-pv"
STORAGE_PATH = "/mnt/nfs/airflow/monday_etl"  # Change this path to a valid hostPath directory on your K8s node
NAMESPACE = "airflow"

default_args = {
    'start_date': days_ago(1),
}

nfs_pv_config = {
    "mode": ["ReadWriteMany"],
    "storage": "10Gi",
    "reclaim_policy": "Delete",
    "nfs_path": STORAGE_PATH,
    "nfs_server": "10.40.0.33",
    "pv_name": PV_NAME,
    "pvc_name": PVC_NAME,
    "namespace": NAMESPACE
}

delete_pv_config = {
    "pv_name":PV_NAME,
    "pvc_name":PVC_NAME,
    "namespace":NAMESPACE
}

extractor_secret = Secret(
    deploy_type="env",           # inject as environment variable
    deploy_target=None,          # None means all keys will be injected as their own env vars
    secret="monday-extractor-secret",          # name of your secret in the `airflow` namespace
)

with DAG(
    dag_id="monday_k8s_etl_with_pv",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["k8s", "pv", "etl"]
) as dag:

    volume = client.V1Volume(
        name="etl-storage",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
    )

    volume_mount = client.V1VolumeMount(
        mount_path="/app/boards",
        name="etl-storage",
        read_only=False,
    )

    common = {
        'namespace': NAMESPACE,
        'is_delete_operator_pod': True,
        'volumes': [volume],
        'image_pull_secrets': [client.V1LocalObjectReference(name="gitlab-registry-secret")],
        'volume_mounts': [volume_mount],
        'get_logs': True

    }

    create_pv_pvc = PythonOperator(
        task_id="create_pv_pvc",
        python_callable=create_pv_and_pvc,
        op_kwargs={
            "pv_config": nfs_pv_config
        }
    )

    extractor = KubernetesPodOperator(
        task_id="extractor",
        name="extractor",
        secrets=[extractor_secret],
        image="registry.infinitylabs.co.il/ai/data-infrastructure/monday_extractor:51-65b0d7df",
        **common
    )

    transformer = KubernetesPodOperator(
        task_id="transformer",
        name="transformer",
        image="registry.infinitylabs.co.il/ai/data-infrastructure/monday_transformer:58-c24e7ee0",
        **common
    )

    cleanup = PythonOperator(
        task_id="delete_pv_pvc",
        python_callable=delete_pv_and_pvc,
        op_kwargs={
            "pv_config": delete_pv_config
        },
        trigger_rule="all_done",
    )

    create_pv_pvc >> extractor >> transformer >> cleanup

dag = dag