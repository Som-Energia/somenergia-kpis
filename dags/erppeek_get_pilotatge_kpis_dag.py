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
    dag_id="erppeek_get_pilotatge_kpis_daily_dag",
    start_date=datetime(2022, 9, 1),
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["ERPPeek", "Extract"],
    default_args=args,
) as dag:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()


    get_conversations_task = DockerOperator(
        api_version="auto",
        task_id="erppeek_get_pilotatge_kpis_daily",
        docker_conn_id="somenergia_registry",
        image="{}/{}-requirements:latest".format(
            "{{ conn.somenergia_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m datasources.erppeek.filtered_models_single_kpis "{{ var.value.dades_prod_db }}" "daily" \
                "{{ var.value.erp_server_prod_ro }}" "{{ var.value.erp_database }}" "{{ var.value.erp_user_airflow }}" \
                "{{ var.value.erp_pass_airflow_secret }}" "{{ var.value.schema_kpis_dades_prod_db }}"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
    )

