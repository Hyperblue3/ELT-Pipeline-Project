from datetime import datetime
from airflow import DAG
from docker.types import Mount
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.docker.operators.docker import DockerOperator  # Güncel import
import subprocess

CONN_ID = '****************************'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}



with DAG(
    'elt_and_dbt',
    default_args=default_args,
    description='An ELT workflow with dbt',
    start_date=datetime(2025,4,25),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    t1 = AirbyteTriggerSyncOperator(
        task_id='airbyte_postgres_postgres',
        airbyte_conn_id = 'airbyte',
        connection_id=CONN_ID,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
        dag=dag


    )

    t2 = DockerOperator(
        task_id='dbt_run',
        image='ghcr.io/dbt-labs/dbt-postgres:1.4.7',
        command=[
            "run",
            "--profiles-dir",
            "/root",
            "--project-dir",
            "/opt/dbt"
        ],
        auto_remove='force',
        docker_url="tcp://docker-proxy:2375",  # Docker Desktop için
        network_mode="host",
        mounts=[
            Mount(source='/home/sektor34/ELT/custom_postgres',
                 target='/dbt', type='bind'),
            Mount(source='/home/sektor34/dbt-env',
                 target='/root', type='bind'),
        ],
    )

    t1 >> t2