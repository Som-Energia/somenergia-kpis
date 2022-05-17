from airflow import DAG
from airflow.operators.python import PythonOperator
import requests

def _process(portainer_key):
    r = requests.post(url='https://moll2.somenergia.lan:9443/api/endpoints/1/docker/build', headers={'x-api-key' : portainer_key, 'Content-Type':'application/json'}, params={'t':'somenergia-kpis-requirements:latest', 'remote': 'https://github.com/Som-Energia/somenergia-kpis.git#main', 'nocache': 'true'}, data='{}', verify=False)

def build_image_build_task(dag: DAG) -> PythonOperator:

    task_image_build = PythonOperator(
             task_id='image_build',
             python_callable=_process,
             op_kwargs={'portainer_key':"{{ var.value.portainer_api_key }}"},
             trigger_rule='one_success',
             dag=dag,
             )

    return task_image_build
