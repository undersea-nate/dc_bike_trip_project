import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, date

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,  BigQueryCreateEmptyTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET_RAW = os.environ.get("BIGQUERY_DATASET", 'bike_data_raw')
BIGQUERY_DATASET_DEV = os.environ.get("BIGQUERY_DATASET", 'bike_data_dev')

year = "2023"
month = "03"
dataset = f"{year}{month}-capitalbikeshare-tripdata"
dataset_zip = f"{dataset}.zip"
dataset_file = f"{dataset}.csv"
dataset_parquet = dataset_file.replace('.csv', '.parquet')
dataset_url = f"https://s3.amazonaws.com/capitalbikeshare-data/{dataset_zip}"
dataset_name = '_' + dataset.replace('-','_')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

if (int(year) == 2020 and int(month) >= 4) or (int(year) >= 2021): #raw data format changes based on month and year
    format = "new"
else:
    format = "old"
#TO DO:
#Create "master" dataset where I can join all the individual datasets into one big one
    #Create a new task to drop current month's table BEFORE creating it
    #drop rows from "master" dataset from current month (to ensure that data isn't entered twice)
    #OPTIONAL: drop rows from current month's dataset that are not from the current time range
    #insert current month's table into "master" dataset
#use dbt core or spark (or both?) to transform the data - start and end dates especially need to be separated
    #OPTIONAL: Also create morning commute and evening commute flags
    #OPTIONAL: Can use Longitude and Latitude file as a "seed" in DBT (only 107 distinct values) 
    #Should also trim the station names to remove spaces
    #OPTIONAL: split into dev, stg, and prd
        #dev - local place where you can mess around with data
        #staging - as similar to production environment as possible
        #production - where your live data is
    #OPTIONAL: Testing in DBT - look up how to do this!
#create dashboard in PBI, maybe 
#create diagram here: https://www.diagrams.net/

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))



###### def upload_file_to_gcs()
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

#def evaluate_dataset_format(year: str, month:str) -> None:
#    if (int(year) == 2020 and int(month) >= 4) or (int(year) >= 2021):
#        return "create_external_bq_table_new_format"
#    else:
#        return "create_external_bq_table_old_format"


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023,3,30),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(dag_id="download_and_upload_gcs", 
         schedule_interval="@daily", 
         max_active_runs = 1,
         default_args=default_args,
         catchup=False) as dag:
    
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_zip}"
    )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip -o {path_to_local_home}/{dataset_zip} -d {path_to_local_home}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            #"src_file": f"{path_to_local_home}/{year}{month}-captialbikeshare-tripdata.csv",
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"{format}/{dataset_parquet}",
            #"local_file": f"{path_to_local_home}/{year}{month}-captialbikeshare-tripdata.parquet",
            "local_file": f"{path_to_local_home}/{dataset_parquet}",
        },
    )

    #evaluate_dataset_format_task = BranchPythonOperator(
    #    task_id = 'evaluate_dataset_format',
    #    python_callable = evaluate_dataset_format,
    #    op_kwargs={
    #        'year': f"{year}",
    #        'month': f"{month}"
    #    }
    #)


    download_dataset_task >> unzip_dataset_task >> format_to_parquet_task >> local_to_gcs_task

    #download_dataset_task >> unzip_dataset_task >> upload_to_s3_task >> evaluate_dataset_format_task >> [create_redshift_table_new_format_task, create_redshift_table_old_format_task]
    #drop_redshift_table_task >> evaluate_dataset_format_task
    #create_redshift_table_old_format_task >> transfer_s3_to_redshift_old_format_task >> append_bike_data_old_format_task
    #create_redshift_table_new_format_task >> transfer_s3_to_redshift_new_format_task >> append_bike_data_new_format_task