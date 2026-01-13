from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import os

INPUT_PATH = "/opt/airflow/data/input.csv"
OUTPUT_PATH = "/opt/airflow/data/output.csv"


def etl_csv():
    rows = []

    with open(INPUT_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            score = int(row["score"])
            row["passed"] = "true" if score >= 80 else "false"
            rows.append(row)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    with open(OUTPUT_PATH, "w", newline="") as f:
        fieldnames = ["id", "name", "score", "passed"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"ETL done. Output written to {OUTPUT_PATH}")


with DAG(
    dag_id="etl_csv_simple",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "csv"],
) as dag:

    etl_task = PythonOperator(
        task_id="etl_csv_task",
        python_callable=etl_csv,
    )
