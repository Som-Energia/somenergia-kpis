from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

with DAG(dag_id='hs_transform_conversations_dag', start_date=datetime(2020,3,20), schedule_interval='@hourly', catchup=True, tags=["Helpscout", "Transform"], default_args=args) as dag:

    sensor_hs_cru = ExternalTaskSensor(
        task_id="sensor_hs_cru",
        external_dag_id='hs_get_conversations_dag',
        external_task_id='hs_get_conversations',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
    )

    transform_conversations_task = DockerOperator(
        api_version='auto',
        task_id='hs_transform_conversations',
        image='somenergia-kpis-requirements:latest',
        working_dir='/repos/somenergia-kpis',
        command='python3 -m pipelines.hs_transform_conversations "{{ data_interval_start }}" "{{ data_interval_end }}" \
                "{{ var.value.puppis_prod_db }}"',
        docker_url=Variable.get("moll_url"),
        mounts=[mount_nfs],
        mount_tmp_dir=False,
        auto_remove=True,
        retrieve_output=True,
        trigger_rule='none_failed',
    )

    sensor_hs_cru >> transform_conversations_task