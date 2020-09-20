"""DAG to drop a redshift cluster."""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from redshift_utils import destroy_cluster


from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": days_ago(15),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "destroy_redshift_cluster",
    default_args=default_args,
    description="dag to destroy a redshift cluster",
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id="start")

    source_env = BashOperator(
        task_Id="source_env_variables", bash_command="source .env"
    )

    destroy_cluster_task = PythonOperator(
        task_id="destroy_redshift_cluster", python_callable=destroy_cluster.main,
    )

    end_task = DummyOperator(task_id="end")

    start_task >> source_env >> destroy_cluster_task >> end_task
