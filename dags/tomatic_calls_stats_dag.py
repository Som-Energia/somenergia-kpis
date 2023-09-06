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
    dag_id="tomatic_calls_stats_dag",
    start_date=datetime(2022, 11, 28),
    schedule_interval="@daily",
    catchup=False,
    tags=["Tomatic", "Extract"],
    max_active_runs=1,
    default_args=args,
) as dag:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()

    tomatic_calls_stats_task = DockerOperator(
        api_version="auto",
        task_id="tomatic_calls_stats",
        docker_conn_id="somenergia_registry",
        image="{}/{}-requirements:latest".format(
            "{{ conn.somenergia_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m datasources.tomatic.tomatic_stats "{{ var.value.dades_prod_db }}" prod "{{ var.value.tomatera_user }}" \
                 "{{ var.value.tomatera_password }}" "{{ var.value.tomatera_host }}" "{{ var.value.tomatera_port }}" \
                  "/opt/www/somenergia-tomatic"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
    )

