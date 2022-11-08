from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from util_tasks.t_branch_pull_ssh import build_branch_pull_ssh_task
from util_tasks.t_git_clone_ssh import build_git_clone_ssh_task
from util_tasks.t_check_repo import build_check_repo_task
from util_tasks.t_update_docker_image import build_update_image_task
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

with DAG(dag_id='hs_get_tags_dag', start_date=datetime(2022,6, 15), schedule_interval='@hourly', catchup=True, tags=["Helpscout", "Extract"], default_args=args) as dag:

    repo_name = 'somenergia-kpis'

    task_check_repo = build_check_repo_task(dag=dag, repo_name=repo_name)
    task_git_clone = build_git_clone_ssh_task(dag=dag, repo_name=repo_name)
    task_branch_pull_ssh = build_branch_pull_ssh_task(dag=dag, task_name='hs_get_conversations', repo_name=repo_name)
    task_update_image = build_update_image_task(dag=dag, repo_name=repo_name)

    get_tags_task = DockerOperator(
        api_version='auto',
        task_id='hs_get_tags',
        docker_conn_id='somenergia_registry',
        image='{{ conn.somenergia_registry.host }}/{}-requirements:latest'.format(repo_name),
        working_dir=f'/repos/{repo_name}',
        command='python3 -m datasources.helpscout.hs_get_tags "{{ data_interval_start }}" "{{ data_interval_end }}" \
                "{{ var.value.puppis_prod_db}}" "{{ var.value.helpscout_api_id}}" "{{ var.value.helpscout_api_secret}}"',
        docker_url=Variable.get("generic_moll_url"),
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule='none_failed',
    )

    task_check_repo >> task_git_clone
    task_check_repo >> task_branch_pull_ssh
    task_git_clone >> task_update_image
    task_branch_pull_ssh >> task_update_image
    task_branch_pull_ssh >> get_tags_task
    task_update_image >> get_tags_task