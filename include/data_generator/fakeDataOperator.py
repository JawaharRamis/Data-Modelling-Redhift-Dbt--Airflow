from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import csv
import json
import datetime
import pandas as pd
from include.data_generator.fake_data_generator import FakeDataGenerator


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
            *args, **kwargs):
        super(FakeDataToS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.existing_data_key = existing_data_key


    def execute(self, context):
        # aws_hook = BaseHook.get_connection(self.aws_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        existing_data = self.load_existing_data(s3_hook)
        # Generate fake data
        fake_data_generator = FakeDataGenerator(existing_data)
        fake_data = fake_data_generator.generate_fake_data()
        for row in fake_data[:3]:
            print(row)

        df = pd.DataFrame(fake_data)
        csv_file = '/usr/local/airflow/include/data/fake_data.csv'
        df.to_csv(csv_file, index=False)

        # Upload the CSV file to S3
        s3_hook.load_file(
            filename=csv_file,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )

        # Remove the temporary file
        # os.remove(csv_file)


    def load_existing_data(self, s3_hook):
        existing_data = []
        existing_data_csv = s3_hook.read_key(self.existing_data_key, bucket_name=self.s3_bucket)
        
        if existing_data_csv:
            reader = csv.DictReader(existing_data_csv.splitlines())
            existing_data = [row for row in reader]

        return existing_data
    
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)