from airflow.operators.python_operator import PythonOperator
from airflow import DAG
import paramiko

def myfakefile(keystring):
    myfakefile.readlines=lambda: keystring.split("\n")
    return myfakefile

def cloneRepoSSH(scheat_key):
    p = paramiko.SSHClient()
    p.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    mykey = paramiko.RSAKey.from_private_key(myfakefile(scheat_key))
    p.connect("scheat.somenergia.lan", port=2200, username="airflow", pkey=mykey)
    p.exec_command("git clone https://github.com/Som-Energia/somenergia-kpis.git /opt/airflow/repos/somenergia-kpis")

def build_git_clone_ssh_task(dag: DAG) -> PythonOperator:
    git_clone_ssh_task = PythonOperator(
        task_id='git_clone_ssh_task',
        python_callable=cloneRepoSSH,
        op_kwargs={ "scheat_key": "{{ var.value.scheat_key }}" },
        dag=dag,
    )

    return git_clone_ssh_task
