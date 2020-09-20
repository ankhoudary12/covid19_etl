"""DAG representing COVID-19 public data ETL."""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.macros import ds_add
from airflow.macros import ds_format
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

from datetime import timedelta
import pandas as pd
import csv
import os
import logging
from typing import Any

import sql_queries
import config


DAYS_TO_RECALCULATE = config.DAYS_TO_RECALCULATE
S3_BUCKET = config.S3_BUCKET
ACCESS_KEY_ID = config.ACCESS_KEY_ID
SECRET_ACCESS_KEY = config.SECRET_ACCESS_KEY

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


def clear_s3_bucket(bucket_name: str) -> None:
    """Deletes contents of specified s3 bucket.

    Args:
        bucket_name (str): name of s3 to clear contents
    """
    s3 = S3Hook(aws_conn_id="aws_credentials")

    keys = s3.list_keys(bucket_name)

    if keys is not None:
        s3.delete_objects(bucket_name, keys)
    else:
        pass


def clear_local_raw_covid_data(path: str) -> None:
    """Clear raw covid csvs from specified path.

    Args:
        path (str): path to delete files
    """
    files = os.listdir(path)

    if files:

        for file in files:
            os.remove(os.path.join(path, file))


def stage_to_s3(*args: Any, **kwargs: Any) -> None:
    """Stages raw covid csv to an s3 bucket."""
    date = ds_add(kwargs["ds"], kwargs["days_ago"])
    date = ds_format(date, "%Y-%m-%d", "%m-%d-%Y")

    df = pd.read_csv(
        config.COVID_DATA_URL.format(date), dtype={"FIPS": "Int64", "Active": "Int64"},
    )

    df["Last_Update"] = pd.to_datetime(date)

    df.to_csv(
        f"/usr/local/airflow/dags/raw_covid_data/raw_data_{date}.csv",
        index=False,
        quotechar="'",
        quoting=csv.QUOTE_NONNUMERIC,
        sep="|",
    )

    s3 = S3Hook(aws_conn_id="aws_credentials")

    s3.load_file(
        filename=f"/usr/local/airflow/dags/raw_covid_data/raw_data_{date}.csv",
        bucket_name="covid19-etl-staging-covid",
        key=f"raw_data_{date}.csv",
    )


def insert_star_schema():
    """Inserts data into covid star schema."""
    redshift = PostgresHook("redshift")

    for query in sql_queries.insert_queries:
        redshift.run(query)


def validate_load():
    """Validates that rows have been inserted into the star schema."""
    redshift = PostgresHook("redshift")

    for query in sql_queries.validate_queries:
        records = redshift.get_records(query)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {query} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {query} contained 0 rows")
        logging.info(f"Data quality on {query}  passed with {records[0][0]} records")


with DAG(
    "COVID-19_ETL",
    default_args=default_args,
    description="a dag for loading covid19 data to a redshift warehouse",
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
) as dag:

    start = DummyOperator(task_id="start")

    source_env = BashOperator(
        task_id="source_env_variables", bash_command="source .env"
    )

    clear_local_raw_covid_data_task = PythonOperator(
        task_id="clear_local_raw_covid_data",
        python_callable=clear_local_raw_covid_data,
        op_kwargs={"path": "/usr/local/airflow/dags/raw_covid_data"},
    )

    clear_s3_bucket_task = PythonOperator(
        task_id="clear_s3_bucket",
        python_callable=clear_s3_bucket,
        op_kwargs={"bucket_name": "covid19-etl-staging-covid"},
    )

    start >> source_env >> clear_local_raw_covid_data_task >> clear_s3_bucket_task

    create_covid_schema = PostgresOperator(
        task_id="create_covid_schema",
        postgres_conn_id="redshift",
        sql="CREATE SCHEMA IF NOT EXISTS covid",
    )

    for i in range(-DAYS_TO_RECALCULATE, 0):
        stage_to_s3_task = PythonOperator(
            task_id="stage_s3_days_ago_{}".format(i),
            python_callable=stage_to_s3,
            provide_context=True,
            op_kwargs={"days_ago": i},
        )

        clear_s3_bucket_task >> stage_to_s3_task >> create_covid_schema

    create_table_dummy = DummyOperator(task_id="create_table_dummy")

    for value, query in enumerate(sql_queries.create_queries):
        create_dim_tables = PostgresOperator(
            task_id="running_create_tables_{}".format(value),
            postgres_conn_id="redshift",
            sql=query,
        )

        create_covid_schema >> create_dim_tables >> create_table_dummy

    delete_staging_data = PostgresOperator(
        task_id="delete_raw_covid_data",
        postgres_conn_id="redshift",
        sql="DELETE FROM covid.staging_raw_covid",
    )

    insert_staging_data = PostgresOperator(
        task_id="insert_raw_covid_data",
        postgres_conn_id="redshift",
        sql=sql_queries.insert_staging_covid.format(
            S3_BUCKET, ACCESS_KEY_ID, SECRET_ACCESS_KEY,
        ),
    )

    delete_fact_covid_data = PostgresOperator(
        task_id="delete_past_fact_covid_data",
        postgres_conn_id="redshift",
        sql=sql_queries.delete_fact_covid,
    )

    insert_star_schema_task = PythonOperator(
        task_id="insert_star_schema_data", python_callable=insert_star_schema
    )

    validate_load = PythonOperator(
        task_id="validate_load_task", python_callable=validate_load
    )

    create_table_dummy >> delete_staging_data >> insert_staging_data

    insert_staging_data >> delete_fact_covid_data >> insert_star_schema_task

    insert_star_schema_task >> validate_load
