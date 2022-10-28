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

with DAG(dag_id='erppeek_get_pilotatge_kpis_daily_dag', start_date=datetime(2022,9,1), schedule_interval='0 3 * * *', catchup=False, tags=["ERPPeek", "Extract"], default_args=args) as dag:

    task_branch_pull_ssh = build_branch_pull_ssh_task(dag=dag, task_name='erppeek_get_pilotatge_kpis_daily')
    task_git_clone = build_git_clone_ssh_task(dag=dag)
    task_check_repo = build_check_repo_task(dag=dag)
    task_image_build = build_image_build_task(dag=dag)
    task_remove_image= build_remove_image_task(dag=dag)

    get_conversations_task = DockerOperator(
        api_version='auto',
        task_id='erppeek_get_pilotatge_kpis_daily',
        docker_conn_id='somenergia_registry',
        image='{{ conn.somenergia_registry.host }}/somenergia-kpis-requirements:latest',
        working_dir='/repos/somenergia-kpis',
        command='python3 /repos/somenergia-kpis/datasources/erppeek/filtered_models_single_kpis.py "{{ var.value.dades_prod_db }}" "daily" \
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
    task_git_clone >> task_image_build
    task_branch_pull_ssh >> get_conversations_task
    task_branch_pull_ssh >> task_remove_image
    task_remove_image >> task_image_build >> get_conversations_task