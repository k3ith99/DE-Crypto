from airflow.decorators import (
    dag,
    task,
)
from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator
@dag(
    schedule = "@daily",
    start_date = datetime(2025,6,25),
    catchup = True,
    is_paused_upon_creation=True,
    default_args = {
        "retries":1,
    }
)
#
def airflow_daily_etl():
    ingestion_daily = DockerOperator(
        task_id = "ingestion_daily",
        image = "crypto_etl/daily_ingestion",
        container_name  = "daily_ingestionn",
        network_mode = "container:postgres1",
        #docker_url= "unix://var/run/docker.sock",
        mount_tmp_dir= False,
        auto_remove = 'success'
    )
    
    transform_daily = DockerOperator(
    task_id = "transform_daily",
    image = "crypto_etl/daily_transform",
    container_name  = "daily_transform",
    network_mode = "container:postgres1",
    #docker_url= "unix://var/run/docker.sock",
    mount_tmp_dir= False,
    auto_remove = 'success',
    trigger_rule = "none_failed"
    )

    ingestion_daily >> transform_daily

airflow_daily_etl()


