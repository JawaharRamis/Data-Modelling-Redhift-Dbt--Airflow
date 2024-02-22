
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3
import json
import os

def get_s3_key(**context):
    message = context['ti'].xcom_pull(key='messages', task_ids='sqs_sensor_task')
    msg_body= message[0]['Body']
    if isinstance(msg_body, str):
        msg_body = json.loads(msg_body)
    key =msg_body['Records'][0]['s3']['object']['key']
    # delete_sqs_message(message)
    context['ti'].xcom_push(key='s3_key', value=key)
    return {"key": key}

def delete_sqs_message(delete_sqs_message):
    aws_hook = AwsBaseHook(aws_conn_id="aws_sqs_conn", client_type='sqs')
    credentials = aws_hook.get_credentials()
    region_name = aws_hook.region_name

    sqs = boto3.client(
        'sqs',
        region_name=region_name,
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token
    )

    url = os.environ.get("SQS_URL")
    sqs.delete_message(QueueUrl=url, ReceiptHandle=delete_sqs_message[0]['ReceiptHandle'])