import pandas as pd
from datetime import datetime
import chardet


def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def add_record_date(input_file, output_file):
    file_encoding = detect_encoding(input_file)
    df = pd.read_csv(input_file, encoding=file_encoding)

    df.insert(2, 'Record Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    df=df.drop(['Row ID'], axis=1)
    df.to_csv(output_file, index=False, encoding='utf-8')

