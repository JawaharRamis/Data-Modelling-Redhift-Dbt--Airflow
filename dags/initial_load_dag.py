from datetime import datetime, timedelta

from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.decorators import (
    dag,
    task,
)
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

DAG_NAME = 'superstore_data_pipeline'

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
        filename='/usr/local/airflow/include/data/Superstore.csv',
        dest_key="superstore",
        dest_bucket="superstore-kaggle",
        replace=True,
        aws_conn_id= "aws_default"
    )

    create_schema = RedshiftSQLOperator(
        task_id='create_schema',
        sql='/sql/create_schema.sql',
        params={
            "schema": "stage",
            "table": "sales_records"
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

    # delete_staging_table = RedshiftSQLOperator(
    #     task_id='delete_staging_table',
    #     sql='/sql/delete_staging.sql',
    #     params={
    #         "schema": "public",
    #         "table": "sample"
    #     },
    # )

    # delete_schema = RedshiftSQLOperator(
    #     task_id='delete_schema',
    #     sql='/sql/delete_schema.sql',
    #     params={
    #         "schema": "public",
    #         "table": "sample"
    #     },
    # )

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

    start_operator>>create_local_to_s3_job>>create_schema>>create_staging_table>>s3_to_redshift_stage