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

with DAG(dag_id='meff_update_closing_prices_dag', start_date=datetime(2022,6,9), schedule_interval='0 15 * * *', catchup=False, tags=["Meff","Extract"], default_args=args) as dag:

    repo_name = 'somenergia-kpis'

    task_check_repo = build_check_repo_task(dag=dag, repo_name=repo_name)
    task_git_clone = build_git_clone_ssh_task(dag=dag, repo_name=repo_name)
    task_branch_pull_ssh = build_branch_pull_ssh_task(dag=dag, task_name='meff_update_closing_prices', repo_name=repo_name)
    task_update_image = build_update_image_task(dag=dag, repo_name=repo_name)

    meff_update_closing_prices_task = DockerOperator(
        api_version='auto',
        task_id='meff_update_closing_prices',
        docker_conn_id='somenergia_registry',
        image='{{ conn.somenergia_registry.host }}/{}-requirements:latest'.format(repo_name),
        working_dir=f'/repos/{repo_name}',
        command='python3 /repos/{}/datasources/meff/meff_update_closing_prices.py "{{ var.value.puppis_prod_db}}"'.format(repo_name),
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
    task_branch_pull_ssh >> meff_update_closing_prices_task
    task_branch_pull_ssh >> task_update_image
    task_update_image >> meff_update_closing_prices_task



with DAG(dag_id='meff_slice_closing_prices_dag', start_date=datetime(2022,6,9), schedule_interval='0 16 * * *', catchup=False, tags=["Meff","Transform"], default_args=args) as dag_slice:

    repo_name = 'somenergia-kpis'

    task_check_repo = build_check_repo_task(dag=dag, repo_name=repo_name)
    task_git_clone = build_git_clone_ssh_task(dag=dag, repo_name=repo_name)
    task_branch_pull_ssh = build_branch_pull_ssh_task(dag=dag, task_name=['meff_slice_day_closing_prices','meff_slice_month_closing_prices'], repo_name=repo_name)
    task_update_image = build_update_image_task(dag=dag, repo_name=repo_name)

    meff_slice_day_closing_prices_task = DockerOperator(
        api_version='auto',
        task_id='meff_slice_day_closing_prices',
        docker_conn_id='somenergia_registry',
        image='{{ conn.somenergia_registry.host }}/{}-requirements:latest'.format(repo_name),
        working_dir=f'/repos/{repo_name}',
        command='python3 -m pipelines.meff_closing_prices_day_slice "{{ var.value.puppis_prod_db}}"',
        docker_url=Variable.get("generic_moll_url"),
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule='none_failed',
    )

    meff_slice_month_closing_prices_task = DockerOperator(
        api_version='auto',
        task_id='meff_slice_month_closing_prices',
        docker_conn_id='somenergia_registry',
        image='{{ conn.somenergia_registry.host }}/somenergia-kpis-requirements:latest',
        working_dir=f'/repos/{repo_name}',
        command='python3 -m pipelines.meff_closing_prices_month_slice "{{ var.value.puppis_prod_db}}"',
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
    task_branch_pull_ssh >> [meff_slice_day_closing_prices_task, meff_slice_month_closing_prices_task]
    task_branch_pull_ssh >> task_update_image
    task_update_image >> [meff_slice_day_closing_prices_task, meff_slice_month_closing_prices_task]