import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import DriverConfig, Mount
from util_tasks.t_branch_pull_ssh import build_branch_pull_ssh_task
from util_tasks.t_check_repo import build_check_repo_task
from util_tasks.t_git_clone_ssh import build_git_clone_ssh_task
from util_tasks.t_update_docker_image import build_update_image_task

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
    default_args=args,
) as dag:
    repo_name = "somenergia-kpis"

    sampled_moll = get_random_moll()

    task_check_repo = build_check_repo_task(dag=dag, repo_name=repo_name)
    task_git_clone = build_git_clone_ssh_task(dag=dag, repo_name=repo_name)
    task_branch_pull_ssh = build_branch_pull_ssh_task(
        dag=dag, task_name="tomatic_calls_stats", repo_name=repo_name
    )
    task_update_image = build_update_image_task(dag=dag, repo_name=repo_name)

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

    task_check_repo >> task_git_clone
    task_check_repo >> task_branch_pull_ssh
    task_git_clone >> task_update_image
    task_branch_pull_ssh >> tomatic_calls_stats_task
    task_branch_pull_ssh >> task_update_image
    task_update_image >> tomatic_calls_stats_task
