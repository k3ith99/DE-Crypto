from airflow.decorators import (
    dag,
    task,
)
from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator

@dag(
    schedule = "@monthly",
    start_date = datetime(2025,6,25),
    catchup = True,
    is_paused_upon_creation=True,
    default_args = {
        "retries":1,
    }
)

def airflow_monthly_etl():
    ingestion_monthly = DockerOperator(
        task_id = "ingestion_monthly",
        image = "crypto_etl/monthly_ingestion",
        container_name  = "monthly_ingestion",
        network_mode = "container:postgres1",
        #docker_url= "unix://var/run/docker.sock",
        mount_tmp_dir= False,
        auto_remove = 'success'
    )
    
    transform_monthly = DockerOperator(
    task_id = "transform_monthly",
    image = "crypto_etl/monthly_transform",
    container_name  = "monthly_transform",
    network_mode = "container:postgres1",
    #docker_url= "unix://var/run/docker.sock",
    mount_tmp_dir= False,
    auto_remove = 'success',
    trigger_rule = "none_failed"
    )

    ingestion_monthly >> transform_monthly

airflow_monthly_etl()