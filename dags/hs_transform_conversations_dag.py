import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    dag_id="hs_transform_conversations_dag",
    start_date=datetime(2020, 3, 20),
    schedule_interval="@hourly",
    catchup=True,
    tags=["Helpscout", "Transform"],
    max_active_runs=1,
    default_args=args,
) as dag:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()

    sensor_hs_cru = ExternalTaskSensor(
        task_id="sensor_hs_cru",
        external_dag_id="hs_get_conversations_dag",
        external_task_id="hs_get_conversations",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
    )

    transform_conversations_task = DockerOperator(
        api_version="auto",
        task_id="hs_transform_conversations",
        docker_conn_id="somenergia_harbor_dades_registry",
        image="{}/{}-requirements:latest".format(
            "{{ conn.somenergia_harbor_dades_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m pipelines.hs_transform_conversations "{{ data_interval_start }}" "{{ data_interval_end }}" \
                "{{ var.value.puppis_prod_db }}"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
    )

    sensor_hs_cru >> transform_conversations_task
