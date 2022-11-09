from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from util_tasks.t_branch_pull_ssh import build_branch_pull_ssh_task
from util_tasks.t_git_clone_ssh import build_git_clone_ssh_task
from util_tasks.t_check_repo import build_check_repo_task
from util_tasks.t_update_docker_image import build_update_image_task
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

with DAG(dag_id='erppeek_get_pilotatge_kpis_daily_dag', start_date=datetime(2022,9,1), schedule_interval='0 3 * * *', catchup=False, tags=["ERPPeek", "Extract"], default_args=args) as dag:

    repo_name = 'somenergia-kpis'

    task_check_repo = build_check_repo_task(dag=dag, repo_name=repo_name)
    task_git_clone = build_git_clone_ssh_task(dag=dag, repo_name=repo_name)
    task_branch_pull_ssh = build_branch_pull_ssh_task(dag=dag, task_name='erppeek_get_pilotatge_kpis_daily', repo_name=repo_name)
    task_update_image = build_update_image_task(dag=dag, repo_name=repo_name)


    get_conversations_task = DockerOperator(
        api_version='auto',
        task_id='erppeek_get_pilotatge_kpis_daily',
        docker_conn_id='somenergia_registry',
        image='{}/{}-requirements:latest'.format('{{ conn.somenergia_registry.host }}',repo_name),
        working_dir=f'/repos/{repo_name}',
        command='python3 -m datasources.erppeek.filtered_models_single_kpis "{{ var.value.dades_prod_db }}" "daily" \
                "{{ var.value.erp_server_prod_ro }}" "{{ var.value.erp_database }}" "{{ var.value.erp_user_airflow }}" \
                "{{ var.value.erp_pass_airflow_secret }}" "{{ var.value.schema_kpis_dades_prod_db }}"',
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
    task_branch_pull_ssh >> get_conversations_task
    task_update_image >> get_conversations_task