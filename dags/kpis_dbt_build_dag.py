import logging
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Connection, Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import DriverConfig, Mount

logger = logging.getLogger(__name__)

my_email = Variable.get("fail_email")
addr = Variable.get("repo_server_url")

__doc__ = """
# dbt build periodically

Runs dbt build periodically.
"""

args = {
    "email": my_email,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

nfs_config = {
    "type": "nfs",
    "o": f"addr={addr},nfsvers=4",
    "device": ":/opt/airflow/repos",
}


def get_random_moll() -> str:
    available_molls = Variable.get("available_molls").split()
    # trunk-ignore(bandit/B311)
    return random.choice(available_molls)


driver_config = DriverConfig(name="local", options=nfs_config)

mount_nfs = Mount(
    source="local",
    target="/repos",
    type="volume",
    driver_config=driver_config,
)

with DAG(
    dag_id="dbt_build_kpis_v1",
    start_date=datetime(2025, 1, 21, 0, 0, 0),
    schedule_interval=None,
    catchup=False,
    tags=[
        "project:kpis",
        "dbt-build",
        "dbt",
    ],
    default_args=args,
    max_active_runs=1,
    doc_md=__doc__,
) as dag:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()

    dbdades_con = Connection.get_connection_from_secrets("dbdades_kpis")

    environment = {
        "DBUSER": dbdades_con.login,
        "DBPASSWORD": dbdades_con.password,
        "DBHOST": dbdades_con.host,
        "DBPORT": dbdades_con.port,
        "DBNAME": dbdades_con.schema,
        "DBT_PACKAGES_INSTALL_PATH": "/home/somenergia/.dbt/dbt_packages",
    }

    image = "harbor.somenergia.coop/dades/somenergia-kpis-main:latest"

    build_kpis_task = DockerOperator(
        api_version="auto",
        task_id="build_kpis_task_id",
        environment=environment,
        docker_conn_id="somenergia_harbor_dades_registry",
        image=image,
        working_dir=f"/repos/{repo_name}/dbt_kpis",
        command="dbt build",
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule=TriggerRule.NONE_FAILED,
        force_pull=True,
        retries=0,
        email_on_failure=False,
    )
