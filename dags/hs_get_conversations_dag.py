from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from tasks.t_branch_pull_ssh import build_branch_pull_ssh_task
from tasks.t_git_clone_ssh import build_git_clone_ssh_task
from tasks.t_check_repo import build_check_repo_task
from tasks.t_image_build import build_image_build_task
from docker.types import Mount


from datetime import datetime

with DAG(dag_id='hs_get_conversations_dag', start_date=datetime(2022,4,20), schedule_interval='@hourly', catchup=True) as dag:

    task_branch_pull_ssh = build_branch_pull_ssh_task(dag=dag)
    task_git_clone = build_git_clone_ssh_task(dag=dag)
    task_check_repo = build_check_repo_task(dag=dag)
    task_image_build = build_image_build_task(dag=dag)

    get_conversations_task = DockerOperator(
        api_version='auto',
        task_id='hs_get_conversations_dag',
        image='somenergia-kpis-requirements:latest',
        command='python3 /repos/somenergia-kpis/datasources/helpscout/hs_get_conversations.py "{{ data_interval_start }}" "{{ data_interval_end }}" \
                "{{ var.value.puppis_prod_db}}" "{{ var.value.helpscout_api_id}}" "{{ var.value.helpscout_api_secret}}"',
        docker_url='tcp://moll2.somenergia.lan:2375',
        mounts=[
          Mount(source="somenergia_repositoris", target="/repos", type="volume")
        ],
        auto_remove=True,
        retrieve_output=True,
        trigger_rule='none_failed',
    )

    task_check_repo >> task_git_clone
    task_check_repo >> task_branch_pull_ssh
    task_git_clone >> task_image_build
    task_branch_pull_ssh >> get_conversations_task
    task_branch_pull_ssh >> task_image_build
    task_image_build >> get_conversations_task