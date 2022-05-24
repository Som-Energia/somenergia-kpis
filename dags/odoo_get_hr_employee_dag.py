from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from kpis_tasks.t_branch_pull_ssh import build_branch_pull_ssh_task
from kpis_tasks.t_git_clone_ssh import build_git_clone_ssh_task
from kpis_tasks.t_check_repo import build_check_repo_task
from kpis_tasks.t_image_build import build_image_build_task
from kpis_tasks.t_remove_image import build_remove_image_task
from docker.types import Mount
from datetime import datetime, timedelta

args= {
  'email': ['equip.dades@somenergia.coop'],
  'email_on_failure': True,
  'retries': 5,
  'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='odoo_get_hr_employee_dag', start_date=datetime(2022,5,23), schedule_interval='@weekly', catchup=True, tags=["Odoo"], default_args=args) as dag:

    task_branch_pull_ssh = build_branch_pull_ssh_task(dag=dag, task_name='odoo_get_hr_employee')
    task_git_clone = build_git_clone_ssh_task(dag=dag)
    task_check_repo = build_check_repo_task(dag=dag)
    task_image_build = build_image_build_task(dag=dag)
    task_remove_image= build_remove_image_task(dag=dag)

    get_hr_employee_task = DockerOperator(
        api_version='auto',
        task_id='odoo_get_hr_employee',
        image='somenergia-kpis-requirements:latest',
        command='python3 /repos/somenergia-kpis/datasources/odoo/odoo_get_hr_employee.py "{{ var.value.odoo_dbapi}}" "{{ var.value.puppis_prod_db}}" "{{ data_interval_start }}"',
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
    task_branch_pull_ssh >> get_hr_employee_task
    task_branch_pull_ssh >> task_remove_image
    task_remove_image >> task_image_build >> get_hr_employee_task