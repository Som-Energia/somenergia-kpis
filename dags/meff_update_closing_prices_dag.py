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
    dag_id="meff_update_closing_prices_dag",
    start_date=datetime(2022, 6, 9),
    schedule_interval="0 15 * * *",
    catchup=False,
    tags=["Meff", "Extract"],
    default_args=args,
) as dag:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()


    meff_update_closing_prices_task = DockerOperator(
        api_version="auto",
        task_id="meff_update_closing_prices",
        docker_conn_id="somenergia_registry",
        image="{}/{}-requirements:latest".format(
            "{{ conn.somenergia_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m datasources.meff.meff_update_closing_prices "{{ var.value.puppis_prod_db}}"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
    )



with DAG(
    dag_id="meff_slice_closing_prices_dag",
    start_date=datetime(2022, 6, 9),
    schedule_interval="0 16 * * *",
    catchup=False,
    tags=["Meff", "Transform"],
    max_active_runs=1,
    default_args=args,
) as dag_slice:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()

    meff_slice_day_closing_prices_task = DockerOperator(
        api_version="auto",
        task_id="meff_slice_day_closing_prices",
        docker_conn_id="somenergia_registry",
        image="{}/{}-requirements:latest".format(
            "{{ conn.somenergia_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m pipelines.meff_closing_prices_day_slice "{{ var.value.puppis_prod_db}}"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
    )

    meff_slice_month_closing_prices_task = DockerOperator(
        api_version="auto",
        task_id="meff_slice_month_closing_prices",
        docker_conn_id="somenergia_registry",
        image="{}/{}-requirements:latest".format(
            "{{ conn.somenergia_registry.host }}", repo_name
        ),
        working_dir=f"/repos/{repo_name}",
        command='python3 -m pipelines.meff_closing_prices_month_slice "{{ var.value.puppis_prod_db}}"',
        docker_url=sampled_moll,
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule="none_failed",
    )
