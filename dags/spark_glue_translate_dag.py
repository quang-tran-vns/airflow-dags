from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

with DAG(
    dag_id="spark_glue_translate_th_en",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "glue", "etl"],
) as dag:

    run_spark_etl = KubernetesPodOperator(
        task_id="run_spark_glue_etl",
        name="spark-glue-etl",
        namespace="airflow",
        image="spark-glue:1.0",

        # ENTRYPOINT = spark-submit
        arguments=["/app/glue_etl_translate_th_en.py"],

        get_logs=True,
        is_delete_operator_pod=True,

        volumes=[
            {
                "name": "airflow-data",
                "persistentVolumeClaim": {
                    "claimName": "airflow-data"
                },
            }
        ],
        volume_mounts=[
            {
                "name": "airflow-data",
                "mountPath": "/opt/airflow/data",
            }
        ],
    )
