from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from kpis_tasks.t_branch_pull_ssh import build_branch_pull_ssh_task
from kpis_tasks.t_git_clone_ssh import build_git_clone_ssh_task
from kpis_tasks.t_check_repo import build_check_repo_task
from kpis_tasks.t_image_build import build_image_build_task
from kpis_tasks.t_remove_image import build_remove_image_task
from docker.types import Mount, DriverConfig
from datetime import datetime, timedelta
from airflow.models import Variable

my_email = Variable.get("fail_email")
addr = Variable.get("repo_server_url")

args= {
  'email': my_email,
  'email_on_failure': True,
  'email_on_retry': False,
  'retries': 5,
  'retry_delay': timedelta(minutes=5),
}

nfs_config = {
    'type': 'nfs',
    'o': f'addr={addr},nfsvers=4',
    'device': ':/opt/airflow/repos'
}

driver_config = DriverConfig(name='local', options=nfs_config)
mount_nfs = Mount(source="local", target="/repos", type="volume", driver_config=driver_config)

with DAG(dag_id='hs_get_conversations_dag', start_date=datetime(2020,3,20), schedule_interval='@hourly', catchup=True, tags=["Helpscout"], default_args=args) as dag:

    task_branch_pull_ssh = build_branch_pull_ssh_task(dag=dag, task_name='hs_get_conversations')
    task_git_clone = build_git_clone_ssh_task(dag=dag)
    task_check_repo = build_check_repo_task(dag=dag)
    task_image_build = build_image_build_task(dag=dag)
    task_remove_image= build_remove_image_task(dag=dag)

    get_conversations_task = DockerOperator(
        api_version='auto',
        task_id='hs_get_conversations',
        image='somenergia-kpis-requirements:latest',
        command='python3 /repos/somenergia-kpis/datasources/helpscout/hs_get_conversations.py "{{ data_interval_start }}" "{{ data_interval_end }}" \
                "{{ var.value.puppis_prod_db}}" "{{ var.value.helpscout_api_id}}" "{{ var.value.helpscout_api_secret}}"',
        docker_url=Variable.get("moll_url"),
        mounts=[mount_nfs],
        auto_remove=True,
        retrieve_output=True,
        trigger_rule='none_failed',
    )

    task_check_repo >> task_git_clone
    task_check_repo >> task_branch_pull_ssh
    task_git_clone >> task_image_build
    task_branch_pull_ssh >> get_conversations_task
    task_branch_pull_ssh >> task_remove_image
    task_remove_image >> task_image_build >> get_conversations_task