import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator #moves data from S3 to Redshift
from airflow.hooks.S3_hook import S3Hook #will communicate with S3 bucket
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook #might not work because it says redshift_cluster?
from datetime import datetime, date

import pyarrow.csv as pv
import pyarrow.parquet as pq

dataset_file = "2010-capitalbikeshare-tripdata.csv"
dataset_zip = "2010-capitalbikeshare-tripdata.zip"
dataset_url = f"https://s3.amazonaws.com/capitalbikeshare-data/{dataset_zip}"
parquet_file = dataset_file.replace('.csv', '.parquet')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")



def upload_file_to_s3(filename: str, key: str, bucket_name: str) -> None:

    hook = S3Hook('s3_conn')

    hook.load_file(filename = filename, key = key, bucket_name = bucket_name, replace = True)


with DAG(dag_id="download_and_upload", start_date=datetime(2023,3,23), schedule_interval="@daily", catchup=False) as dag:

    transfer_s3_to_redshift = S3ToRedshiftOperator( #fill out these values later
        task_id="transfer_s3_to_redshift",
        redshift_conn_id='redshift_default',
        s3_bucket='shiny-head-aquamarine',
        s3_key='text2.csv', #can use a key prefix instead to upload multiple tables - look up later
        schema="PUBLIC",
        table='test',
        copy_options=["csv"],
    )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_zip}"
    )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip -o {path_to_local_home}/{dataset_zip} -d {path_to_local_home}",
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

    download_dataset_task >> unzip_dataset_task >> task_upload_to_s3 >> transfer_s3_to_redshift