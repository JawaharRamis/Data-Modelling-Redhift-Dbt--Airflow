from datetime import datetime, timedelta

from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.task_group import TaskGroup

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from airflow.decorators import (
    dag,
    task,
)
from airflow import DAG
from airflow.operators.empty import EmptyOperator
import os

DAG_NAME = 'superstore_data_pipeline'

DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

profile_config = ProfileConfig(
    profile_name="dbt_initial_load",
    target_name="dev",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_default",
        profile_args={"schema": "stage"},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval='@daily',
    description='Superstore Sales Pipeline',
    template_searchpath='/usr/local/airflow/include',
    catchup=False
) as dag:
    
    start_operator = EmptyOperator(
        task_id='start_operator',
        dag=dag,
    )

    create_local_to_s3_job = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename='/data/Superstore.csv',
        dest_key="superstore",
        dest_bucket="superstore-kaggle",
        replace=True,
        aws_conn_id= "aws_default"
    )

    with TaskGroup("create_schema", tooltip="task group #1") as create_schema_task_group:
        create_stage_schema = RedshiftSQLOperator(
            task_id='create_stage_schema',
            sql='/sql/create_schema.sql',
            params={
                "schema": "stage",
            },
        )

        create_dimensions_schema = RedshiftSQLOperator(
            task_id='create_dimensions_schema',
            sql='/sql/create_schema.sql',
            params={
                "schema": "dimensions",
            },
        )

        create_facts_schema = RedshiftSQLOperator(
            task_id='create_facts_schema',
            sql='/sql/create_schema.sql',
            params={
                "schema": "facts",
            },
        )

    create_staging_table = RedshiftSQLOperator(
        task_id='create_staging_table',
        sql='/sql/create_staging.sql',
        params={
            "schema": "stage",
            "table": "sales_records"
        },
    )

    s3_to_redshift_stage = S3ToRedshiftOperator(
        task_id='s3_to_redshift_stage',
        schema='stage',
        table='stage',
        s3_bucket='superstore-kaggle',
        s3_key='superstore',
        redshift_conn_id='redshift_default',
        aws_conn_id='aws_default',
        copy_options=[
            "TIMEFORMAT 'auto'",
            "IGNOREHEADER 1",
            "csv",
            "ACCEPTINVCHARS"
        ]
    )

    transform_data  = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig("/usr/local/airflow/include/dbt/dbt_initial_load"),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )

        # delete_staging_table = RedshiftSQLOperator(
    #     task_id='delete_staging_table',
    #     sql='/sql/delete_staging.sql',
    #     params={
    #         "schema": "public",
    #         "table": "sample"
    #     },
    # )


    start_operator>>create_local_to_s3_job>>create_schema_task_group>>create_staging_table>>s3_to_redshift_stage>>transform_data