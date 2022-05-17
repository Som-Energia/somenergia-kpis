from airflow.operators.python_operator import BranchPythonOperator
from airflow import DAG
import paramiko

def myfakefile(keystring):
    myfakefile.readlines=lambda: keystring.split("\n")
    return myfakefile

def pullRepoSSH(scheat_key):
    p = paramiko.SSHClient()
    p.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    mykey = paramiko.RSAKey.from_private_key(myfakefile(scheat_key))
    p.connect("scheat.somenergia.lan", port=2200, username="airflow", pkey=mykey)
    stdin, stdout, stderr = p.exec_command("git -C /opt/airflow/repos/somenergia-kpis pull")
    txt_stderr = stderr.readlines()
    txt_stderr = "".join(txt_stderr)
    if txt_stderr.count('\n')>0: # si stderr té més de 0 \n és que hi ha canvis al fer pull
        return "image_build"
    return "hs_conversations"

def build_branch_pull_ssh_task(dag: DAG) -> BranchPythonOperator:
    branch_ssh_task = BranchPythonOperator(
        task_id='git_pull_task',
        python_callable=pullRepoSSH,
        op_kwargs={ "scheat_key": "{{ var.value.scheat_key }}" },
        do_xcom_push=False,
        dag=dag,
    )

    return branch_ssh_task
