from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook
import boto3
from tempfile import NamedTemporaryFile
import csv
import json
from fake_data_generator import FakeDataGenerator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class FakeDataToS3Operator(BaseOperator):
    template_fields = ('s3_key',)
    # template_fields: Sequence[str] = ("s3_key",)

    @apply_defaults
    def __init__(
            self,
            aws_conn_id,
            s3_bucket,
            s3_key,
            existing_data_key,
            num_rows=10,
            min_products=1,
            max_products=5,
            *args, **kwargs):
        super(FakeDataToS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.existing_data_key = existing_data_key
        self.num_rows = num_rows
        self.min_products = min_products
        self.max_products = max_products

    def execute(self, context):
        # aws_hook = BaseHook.get_connection(self.aws_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        existing_data = self.load_existing_data(s3_hook)
        # Generate fake data
        fake_data_generator = FakeDataGenerator(existing_data)
        fake_data = fake_data_generator.generate_fake_data()

        data_str = json.dumps(fake_data)

        s3_hook.write_key(key=self.s3_key, bucket_name=self.s3_bucket, data=data_str)

        # Write fake data to a temporary CSV file
        # with NamedTemporaryFile(mode='w+', newline='', delete=False) as temp_csv:
        #     csv_writer = csv.DictWriter(temp_csv, fieldnames=fake_data[0].keys())
        #     csv_writer.writeheader()
        #     csv_writer.writerows(fake_data)
        # Upload the temporary CSV file to S3
        # s3_hook.upload_file(temp_csv.name, self.s3_bucket, self.s3_key)

        # Clean up the temporary file
        # temp_csv.close()

        # self.log.info(f"Fake data successfully uploaded to s3://{self.s3_bucket}/{self.s3_key}")

    def load_existing_data(self, s3_hook):
        existing_data = []
        existing_data_csv = s3_hook.read_key(self.existing_data_key, bucket_name=self.s3_bucket)
        
        if existing_data_csv:
            reader = csv.DictReader(existing_data_csv.splitlines())
            existing_data = [row for row in reader]

        return existing_data