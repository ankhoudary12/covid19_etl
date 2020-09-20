"""DAG to refresh the covid schema by dropping everything."""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago

from datetime import timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def drop_everything() -> None:
    """Drops covid schema."""
    redshift = PostgresHook("redshift")

    redshift.run("DROP SCHEMA IF EXISTS covid CASCADE")


with DAG(
    "REFRESH_DB",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
) as dag:

    drop_task = PythonOperator(
        task_id="drop_everything", python_callable=drop_everything,
    )
