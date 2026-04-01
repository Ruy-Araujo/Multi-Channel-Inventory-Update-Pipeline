from __future__ import annotations

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = os.getenv("PIPELINE_PROJECT_DIR", "/opt/pipeline")
DEFAULT_PROJECT_ID = os.getenv("DATAMISSION_PROJECT_ID", "")
DEFAULT_FORMAT = os.getenv("DATAMISSION_DEFAULT_FORMAT", "parquet")

with DAG(
    dag_id="datamission_dataset_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["datamission", "etl"],
) as dag:
    _run_pipeline = BashOperator(
        task_id="run_dataset_pipeline",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m datamission_pipeline.cli run "
            f"--project-id {DEFAULT_PROJECT_ID} "
            f"--format {DEFAULT_FORMAT}"
        ),
    )
