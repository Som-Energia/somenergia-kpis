import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import DriverConfig, Mount

my_email = Variable.get("fail_email")
addr = Variable.get("repo_server_url")


def get_random_moll() -> str:
    available_molls = Variable.get("available_molls").split()
    return random.choice(available_molls)


args = {
    "email": my_email,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

nfs_config = {
    "type": "nfs",
    "o": f"addr={addr},nfsvers=4",
    "device": ":/opt/airflow/repos",
}

driver_config = DriverConfig(name="local", options=nfs_config)
mount_nfs = Mount(
    source="local", target="/repos", type="volume", driver_config=driver_config
)

with DAG(
    dag_id="odoo_get_hr_employee_dag_v2",
    start_date=datetime(2022, 5, 23),
    schedule_interval="@weekly",
    catchup=False,
    tags=["Odoo", "Extract"],
    max_active_runs=1,
    default_args=args,
) as dag:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()

    get_hr_employee_task = DockerOperator(
        api_version="auto",
        task_id="odoo_get_hr_employee",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-requirements:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m datasources.odoo.hr_employees "{{ var.value.odoo_dbapi}}" "{{ var.value.puppis_prod_db}}" "{{ dag_run.start_date }}"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
    )

