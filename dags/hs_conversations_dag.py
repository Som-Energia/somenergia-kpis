from sys import api_version
from airflow.decorators import dag
from airflow.providers.docker.operators.docker_swarm import DockerSwarmOperator

from datetime import datetime

#pendent decidir el start date i el catchup
@dag(start_date=datetime(2022, 4, 11), schedule_interval='@hourly', catchup=True)
def hs_conversations_dag():

    get_conversations_task = DockerSwarmOperator(
        api_version='auto',
        task_id='hs_conversations',
        image='somenergia-indicadors-KPIs:latest',
        command='python3 datasources/helpscout/hs_get_conversations.py "{{ data_interval_start }}" "{{ data_interval_end }}" \
                "{{ var.value.puppis_prod_db}}" "{{ var.value.helpscout_api_id}}" "{{ var.value.helpscout_api_secret}}"',
        docker_url='tcp://moll1.somenergia.lan:2375',
        auto_remove=True,
    )

    get_conversations_task

dag = hs_conversations_dag()