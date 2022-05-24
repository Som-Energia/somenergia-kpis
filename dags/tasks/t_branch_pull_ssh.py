from airflow.operators.python_operator import BranchPythonOperator
from airflow import DAG
import paramiko
import io


def pull_repo_ssh(repo_server_url, repo_server_key, task_name):
    p = paramiko.SSHClient()
    p.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    keyfile = io.StringIO(repo_server_key)
    mykey = paramiko.RSAKey.from_private_key(keyfile)
    p.connect(repo_server_url, port=2200, username="airflow", pkey=mykey)
    stdin, stdout, stderr = p.exec_command("git -C /opt/airflow/repos/somenergia-kpis pull")
    txt_stderr = stderr.readlines()
    txt_stderr = "".join(txt_stderr)
    print (f"Stderr de git pull ha retornat {txt_stderr}")
    # si stderr té més de 0 \n és que hi ha canvis al fer pull
    #Your configuration specifies to merge with the ref 'refs/heads/main' from the remote, but no such ref was fetched.
    #Apareix quan fem molts git pull a la vegada
    return "image_remove" if txt_stderr.count('\n')>0 and not 'no such ref was fetched' in txt_stderr else task_name

def build_branch_pull_ssh_task(dag: DAG, task_name) -> BranchPythonOperator:
    branch_pull_ssh_task = BranchPythonOperator(
        task_id='git_pull_task',
        python_callable=pull_repo_ssh,
        op_kwargs={ "repo_server_url": "{{ var.value.repo_server_url }}",
                    "repo_server_key": "{{ var.value.repo_server_key }}",
                    "task_name": task_name},
        do_xcom_push=False,
        dag=dag,
    )

    return branch_pull_ssh_task
