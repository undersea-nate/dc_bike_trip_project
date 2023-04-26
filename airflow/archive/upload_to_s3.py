from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.S3_hook import S3Hook #will communicate with S3 bucket
from datetime import datetime, date
import os
import boto3
from zipfile import ZipFile


def upload_file_to_s3(filename: str, key: str, bucket_name: str) -> None:

    hook = S3Hook('s3_conn')

    hook.load_file(filename = filename, key = key, bucket_name = bucket_name)



with DAG("upload_to_s3", start_date=datetime(2023,3,21), schedule_interval="@daily", catchup=False) as dag:

    def upload_file_to_s3(filename: str, key: str, bucket_name: str) -> None:

        hook = S3Hook('s3_conn')

        hook.load_file(filename = filename, key = key, bucket_name = bucket_name)


    task_upload_to_s3 = PythonOperator(
        task_id = 'upload_to_s3',
        python_callable = upload_file_to_s3,
        op_kwargs={
            'filename': 'data/NetflixBestOf_reddit.csv',
            'key': 'netflix_data.csv',
            'bucket_name': 'shiny-head-aquamarine'
        }
    )

    [task_upload_to_s3]

# 'filename': r'C:\Users\mccli\OneDrive\Documents\GitHub\DEZ-final-project\airflow\data\NetflixBestOf_reddit.csv'