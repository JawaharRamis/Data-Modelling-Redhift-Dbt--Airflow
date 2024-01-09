from datetime import datetime, timedelta
import os
import json
import boto3

from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping

from include.functions.get_s3_key import get_s3_key
from include.data_generator.fakeDataOperator import FakeDataToS3Operator

DAG_NAME = 'incremental_superstore_data_pipeline'

DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

TODAY_DATE = datetime.now().strftime('%Y-%m-%d')

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

def read_key(**context):
    message = context['ti'].xcom_pull(key='key', task_ids='get_s3_key_task')
    print(message)
    return message

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


    fake_data_to_s3 = FakeDataToS3Operator(
        task_id='push_fake_data_to_s3',
        aws_conn_id='aws_default',  # Airflow connection ID for AWS credentials
        s3_bucket='superstore-kaggle',
        s3_key='superstore_{{ ds_nodash }}',
        existing_data_key='Superstore',
    )

    sqs_sensor = SqsSensor(
        task_id='sqs_sensor_task',
        sqs_queue='superstore_s3_queue',
        aws_conn_id='aws_sqs_conn',
        max_messages=1,
        poke_interval=5,
        delete_message_on_reception= False
    )

    get_s3_key_task = PythonOperator(
        task_id='get_s3_key',
        python_callable=get_s3_key,
        provide_context=True
    )

    read_key = PythonOperator(
        task_id='read_key',
        python_callable=get_s3_key,
    )



    s3_to_redshift_stage = S3ToRedshiftOperator(
        task_id='s3_to_redshift_stage',
        schema='stage',
        table='stage',
        s3_bucket='superstore-kaggle',
        s3_key="{{ti.xcom_pull(key='s3_key',task_ids='get_s3_key')}}",
        redshift_conn_id='redshift_default',
        aws_conn_id='aws_default',
        copy_options=[
            "TIMEFORMAT 'auto'",
            "IGNOREHEADER 1",
            "csv",
            "ACCEPTINVCHARS"
        ],
    )

    start_operator>>fake_data_to_s3>>sqs_sensor>>get_s3_key_task>>s3_to_redshift_stage