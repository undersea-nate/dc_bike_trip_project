from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, date
from zipfile import ZipFile
import os
import pyarrow.csv as pv
import pyarrow.parquet as pq

today = date.today()

#dataset_file = f"{today.year}{today.month:02}-capitalbikeshare-tripdata"
#dataset_url = f"https://s3.amazonaws.com/capitalbikeshare-data/{dataset_file}.zip"
#path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

#dataset_file = f"2010-Q4-cabi-trip-history-data%2F2010-Q4-cabi-trip-history-data.csv"
#dataset_url = f"https://data.world/sya/capital-bikeshare-trip-data/workspace/file?filename={dataset_file}"
dataset_file = "yellow_tripdata_2021-01.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

print(today)

with DAG("download_csv", start_date=datetime(2023,3,21), schedule_interval="@daily", catchup=False) as dag:


    
    def explore_dataset(src_file) -> None:

        table = pv.read_csv(src_file)

        print("is this working??")

        print(table.column(0))

    explore_dataset_task = PythonOperator(
        task_id="explore_dataset_task",
        python_callable = explore_dataset,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}"
        }
    )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    #download_dataset_task = PythonOperator( #BashOperator(
    #    task_id="download_dataset_task",
    #    python_callable = download_dataset,
    #    op_kwargs={
    #        "url": dataset_url
    #    }
    #    #bash_command=f"#!/bin/sh curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file} unzip {dataset_file}" #rm {dataset_file}
    #)

    download_dataset_task >> explore_dataset_task

