from os import remove
import configparser
import pathlib
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

"""
DAG to extract FPL data, load into PostgreSQL
"""
#Read Configuration File


parser = configparser.ConfigParser()
config_file = "/opt/airflow/dbt/configuration.conf"
parser.read(config_file)

#Configuration Variables
DBT_PROJECT_DIR = parser.get("DBT","project_dir")
DBT_PROFILE_DIR = parser.get("DBT","profile_dir")

output_name = datetime.now().strftime("%Y%m%d")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(0),
    "retries": 1,
    'retry_delay': timedelta(minutes = 5)
}

dag = DAG(
    dag_id = 'elt_FPL_pipeline_PG',
    description = 'FPL ELT for PostgeSQL',
    default_args = default_args,
    schedule_interval = "0 0 * * 1,4",
    tags=["FPL_ETL_PG"]
)

extract_FPL_data = BashOperator(
    task_id="extract_FPL_data",
    bash_command=f"python /opt/airflow/fpl_functions/extract_FPL.py {output_name}",
    dag=dag
    )

upload_to_postgres = BashOperator(
    task_id="upload_to_posgres",
    bash_command=f"python /opt/airflow/fpl_functions/load_data.py {output_name}",
    dag=dag
    )

dbt = DockerOperator(
    task_id='docker_dbt_command',
    image='ghcr.io/dbt-labs/dbt-postgres:1.8.2',
    container_name='dbt_run',
    api_version='auto',
    auto_remove=True,
    command='run',
    docker_url="tcp://docker-proxy:2375",
    network_mode="bridge",
    mounts = [Mount(
        source=DBT_PROJECT_DIR, target="/usr/app", type="bind"),
        Mount(
        source=DBT_PROFILE_DIR, target="/root/.dbt", type="bind")],
        mount_tmp_dir = False
    )

extract_FPL_data >> upload_to_postgres >> dbt