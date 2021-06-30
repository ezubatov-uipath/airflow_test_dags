from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
#from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os
import uuid
#from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from airflow.utils.dates import days_ago
from airflow.contrib.hooks.wasb_hook import WasbHook

args = {
    'owner': 'evgeny.zubatov@uipath.com',
}

with DAG(
    dag_id='p2p_setup_1',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['p2p', 'setup'],
    params={},
) as dag:
    title_task = BashOperator(
        task_id='show_title',
        bash_command='echo "Running p2p setup script"',
    )

    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    print_context_task = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )

    title_task >> print_context_task

    def create_container(ds, **kwargs):
        hook = WasbHook('pmc_wasb')
        print(hook.get_connection('pmc_wasb').get_uri)
        container_name = kwargs['dag_run'].conf.get('application_id') #"{{ dag_run.conf['application_id'] }}"
        print(f"creating container: {container_name}")
        container = hook.create_container(container_name)
        return

    create_container_task = PythonOperator(
        task_id='create_container',
        python_callable=create_container,
    )

    print_context_task >> create_container_task
