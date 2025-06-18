from kubernetes import client, config

PV_NAME = "monday-etl-pv"
PVC_NAME = "monday-etl-pvc"
NAMESPACE = "airflow"

def create_pv_and_pvc(pv_config):
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    pv = client.V1PersistentVolume(
        metadata=client.V1ObjectMeta(
            name=pv_config["pv_name"]
        ),
        spec=client.V1PersistentVolumeSpec(
            access_modes=pv_config["mode"],
            capacity={"storage": pv_config["storage"]},
            persistent_volume_reclaim_policy=pv_config["reclaim_policy"],
            volume_mode="Filesystem",
            mount_options=["hard", "nfsvers=4.1"],
            nfs=client.V1NFSVolumeSource(
                path=pv_config["nfs_path"],
                server=pv_config["nfs_server"]
            )
        )
    )

    v1.create_persistent_volume(body=pv)

    # Create PersistentVolumeClaim
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=pv_config["pvc_name"]),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=pv_config["mode"],
            resources=client.V1ResourceRequirements(requests={"storage": pv_config["storage"]}),
            volume_name=pv_config["pv_name"],
        )
    )
    v1.create_namespaced_persistent_volume_claim(namespace=pv_config["namespace"], body=pvc)


def delete_pv_and_pvc(pv_config):
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    v1.delete_namespaced_persistent_volume_claim(name=pv_config["pvc_name"], namespace=pv_config["namespace"])
    v1.delete_persistent_volume(name=pv_config["pv_name"])
