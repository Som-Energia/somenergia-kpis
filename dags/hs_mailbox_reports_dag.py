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
    "retries": 3,
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
    dag_id="hs_mailbox_reports_dag",
    start_date=datetime(2021, 12, 1),
    schedule_interval="@weekly",
    catchup=True,
    tags=["Helpscout", "Extract"],
    max_active_runs=1,
    default_args=args,
) as dag:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()

    get_conversations_task = DockerOperator(
        api_version="auto",
        task_id="hs_mailbox_reports",
        docker_conn_id="somenergia_registry",
        image="{}/{}-requirements:latest".format(
            "{{ conn.somenergia_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m datasources.helpscout.hs_mailbox_reports update-hs-kpis-pilotatge "{{ data_interval_start }}" "{{ data_interval_end }}" \
                "{{ var.value.dades_prod_db }}" prod "{{ var.value.helpscout_api_id }}" "{{ var.value.helpscout_api_secret }}"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
    )

