from airflow.operators.python_operator import BranchPythonOperator
from airflow import DAG
import os.path

def checkIfRepoAlreadyCloned():
    if os.path.exists('/opt/airflow/repos/somenergia-kpis/.git'):
        return 'git_pull_task'
    return 'git_clone'

def build_check_repo_task(dag: DAG) -> BranchPythonOperator:
    check_repo_task = BranchPythonOperator(
        task_id='check_repo_task',
        python_callable=checkIfRepoAlreadyCloned,
        do_xcom_push=False,
        dag=dag,
    )

    return check_repo_task
