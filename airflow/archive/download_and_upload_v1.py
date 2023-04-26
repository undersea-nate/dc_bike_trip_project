import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook #will communicate with S3 bucket
from datetime import datetime, date

import pyarrow.csv as pv
import pyarrow.parquet as pq

dataset_file = "annual-enterprise-survey-2021-financial-year-provisional-csv.csv"
dataset_url = f"https://stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/{dataset_file}"
parquet_file = dataset_file.replace('.csv', '.parquet')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


#def format_to_parquet(src_file):
#    if not src_file.endswith('.csv'):
#        logging.error("Can only accept source files in CSV format, for the moment")
#        return
#    table = pv.read_csv(src_file)
#    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_file_to_s3(filename: str, key: str, bucket_name: str) -> None:

    hook = S3Hook('s3_conn')

    hook.load_file(filename = filename, key = key, bucket_name = bucket_name)


with DAG(dag_id="download_and_upload", start_date=datetime(2023,3,23), schedule_interval="@daily", catchup=False) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    task_upload_to_s3 = PythonOperator(
        task_id = 'upload_to_s3',
        python_callable = upload_file_to_s3,
        op_kwargs={
            'filename': f"{path_to_local_home}/{dataset_file}",
            'key': 'text2.csv',
            'bucket_name': 'shiny-head-aquamarine'
        }
    )

    download_dataset_task >> task_upload_to_s3