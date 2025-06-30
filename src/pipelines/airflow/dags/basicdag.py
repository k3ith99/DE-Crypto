from airflow.decorators import (
    dag,
    task,
)
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(
    schedule = "@daily",
    start_date = datetime(2025,6,25),
    catchup = True,
    default_args={
        "retries":2,
    },
)

def ingestion():
    ingestion_pipeline = BashOperator(
        task_id = "ingestion_pipeline",
        bash_command = "docker-compose -f docker-compose.ingest.yaml up --build -d"
    )
    ingestion_pipeline
ingestion()