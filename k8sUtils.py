from kubernetes import client, config

PV_NAME = "monday-etl-pv"
PVC_NAME = "monday-etl-pvc"
NAMESPACE = "airflow"

def create_pv_and_pvc(config):
    config.load_incluster_config()
    v1 = client.CoreV1Api()

    pv = client.V1PersistentVolume(
        metadata=client.V1ObjectMeta(
            name=config["pv_name"]
        ),
        spec=client.V1PersistentVolumeSpec(
            access_modes=config["mode"],
            capacity={"storage": config["storage"]},
            persistent_volume_reclaim_policy=config["reclaim_policy"],
            volume_mode="Filesystem",
            mount_options=["hard", "nfsvers=4.1"],
            nfs=client.V1NFSVolumeSource(
                path=config["nfs_path"],
                server=config["nfs_server"]
            )
        )
    )

    v1.create_persistent_volume(body=pv)

    # Create PersistentVolumeClaim
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(name=config["pvc_name"]),
        spec=client.V1PersistentVolumeClaimSpec(
            access_modes=config["mode"],
            resources=client.V1ResourceRequirements(requests={"storage": config["storage"]}),
            volume_name=config["pv_name"],
        )
    )
    v1.create_namespaced_persistent_volume_claim(namespace=config["namespace"], body=pvc)


def delete_pv_and_pvc(config):
    config.load_incluster_config()
    v1 = client.CoreV1Api()
    v1.delete_namespaced_persistent_volume_claim(name=config["pvc_name"], namespace=config["namespace"])
    v1.delete_persistent_volume(name=config["pv_name"])
