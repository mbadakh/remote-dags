from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from kubernetes import client, config

PVC_NAME = "monday-etl-pvc"
PV_NAME = "monday-etl-pv"
STORAGE_PATH = "/mnt/data/monday"  # Change this path to a valid hostPath directory on your K8s node
NAMESPACE = "airflow"

default_args = {
    'start_date': days_ago(1),
}

def create_pv_and_pvc():
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    pv = client.V1PersistentVolume(
        metadata=client.V1ObjectMeta(
            name=PV_NAME
        ),
        spec=client.V1PersistentVolumeSpec(
            access_modes=["ReadWriteMany"],
            capacity={"storage": "10Gi"},
            persistent_volume_reclaim_policy="Delete",
            volume_mode="Filesystem",
            mount_options=["hard", "nfsvers=4.1"],
            nfs=client.V1NFSVolumeSource(
                path="/mnt/nfs/airflow/monday_etl",
                server="10.40.0.33"
            )
        )
    )

    v1.create_persistent_volume(body=pv)

    # Create PersistentVolumeClaim
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=PVC_NAME),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=["ReadWriteMany"],
            resources=client.V1ResourceRequirements(requests={"storage": "10Gi"}),
            volume_name=PV_NAME,
        )
    )
    v1.create_namespaced_persistent_volume_claim(namespace=NAMESPACE, body=pvc)

def delete_pv_and_pvc():
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    v1.delete_namespaced_persistent_volume_claim(name=PVC_NAME, namespace=NAMESPACE)
    v1.delete_persistent_volume(name=PV_NAME)

with DAG(
    dag_id="monday_k8s_etl_with_pv",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["k8s", "pv", "etl"]
) as dag:

    create_pv_pvc = PythonOperator(
        task_id="create_pv_pvc",
        python_callable=create_pv_and_pvc
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
        name="extractor",
        image="registry.infinitylabs.co.il/ai/data-infrastructure/monday_extractor:51-65b0d7df",
        volumes=[volume],
        image_pull_secrets=[client.V1LocalObjectReference(name="gitlab-registry-secret")],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
        namespace=NAMESPACE,
    )

    transformer = KubernetesPodOperator(
        task_id="transformer",
        name="transformer",
        image="registry.infinitylabs.co.il/ai/data-infrastructure/monday_transformer:58-c24e7ee0",
        volumes=[volume],
        image_pull_secrets=[client.V1LocalObjectReference(name="gitlab-registry-secret")],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
        namespace=NAMESPACE,
    )

    cleanup = PythonOperator(
        task_id="delete_pv_pvc",
        python_callable=delete_pv_and_pvc,
        trigger_rule="all_done",  # Clean up even if task fails
    )

    create_pv_pvc >> extractor >> transformer >> cleanup

dag = dag