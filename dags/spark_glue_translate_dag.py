from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime
from kubernetes.client import models as k8s
volume_mount = k8s.V1VolumeMount(
    name="airflow-data",
    mount_path="/opt/airflow/data",
    read_only=False
)

volume = k8s.V1Volume(
    name="airflow-data",
    host_path=k8s.V1HostPathVolumeSource(
        path="/opt/airflow/data"
    )
)

with DAG(
    dag_id="spark_glue_translate_th_en",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "glue", "etl"],
) as dag:

    run_spark_etl = KubernetesPodOperator(
        task_id="spark_glue_translate",
        name="spark-glue-translate",
        namespace="airflow",
        image="spark-glue:1.0",
        cmds=["spark-submit"],
        arguments=["/app/glue_etl_translate_th_en.py"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

