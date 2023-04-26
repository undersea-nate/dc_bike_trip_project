import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator #moves data from S3 to Redshift
from airflow.hooks.S3_hook import S3Hook #will communicate with S3 bucket
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook 
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator #allows us to use SQL queries in Redshift
from datetime import datetime, date

import pyarrow.csv as pv
import pyarrow.parquet as pq

year = "2023"
month = "03"
dataset = f"{year}{month}-capitalbikeshare-tripdata"
dataset_file = f"{dataset}.csv"
dataset_zip = f"{dataset}.zip"
dataset_url = f"https://s3.amazonaws.com/capitalbikeshare-data/{dataset_zip}"
dataset_name = '_' + dataset.replace('-','_')
#parquet_file = dataset_file.replace('.csv', '.parquet')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

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
    #OPTIONA: Testing in DBT - look up how to do this!
#create dashboard in PBI, maybe 
#create diagram here: https://www.diagrams.net/

#def initialize_variables(year: str, month: str) -> None:
#    dataset = f"{year}{month}-capitalbikeshare-tripdata"
#    dataset_file = f"{dataset}.csv"
#    dataset_zip = f"{dataset}.zip"
#    dataset_url = f"https://s3.amazonaws.com/capitalbikeshare-data/{dataset_zip}"
#    dataset_name = '_' + dataset.replace('-','_')

def upload_file_to_s3(filename: str, key: str, bucket_name: str) -> None:

    hook = S3Hook('s3_conn')

    hook.load_file(filename = filename, key = key, bucket_name = bucket_name, replace = True)

def evaluate_dataset_format(year: str, month:str) -> None:
    if (int(year) == 2020 and int(month) >= 4) or (int(year) >= 2021):
        return "create_redshift_table_new_format"
    else:
        return "create_redshift_table_old_format"

#default_params = {
#    "year": "2023",
#    "month": "03"
#}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023,3,30),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(dag_id="download_and_upload_aws", 
         schedule_interval="@daily", 
         max_active_runs = 1,
         default_args=default_args,
         #params=default_params,
         catchup=False) as dag:
    
    #initialize_variables_task = PythonOperator(
    #    task_id = 'initialize_variables_task',
    #    python_callable = initialize_variables,
    #    op_kwargs={
    #        'year': f"{year}",
    #        'month': f"{month}"
    #    }
    #)
    
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_zip}"
    )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip -o {path_to_local_home}/{dataset_zip} -d {path_to_local_home}",
    )

    upload_to_s3_task = PythonOperator(
        task_id = 'upload_to_s3',
        python_callable = upload_file_to_s3,
        op_kwargs={
            #'filename': f"{path_to_local_home}/{year}{month}-captialbikeshare-tripdata.csv",
            'filename': f"{path_to_local_home}/{dataset_file}",
            'key': f'{dataset_file}',
            'bucket_name': 'shiny-head-aquamarine'
        }
    )

    transfer_s3_to_redshift_old_format_task = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift_old_format",
        redshift_conn_id='redshift_default',
        s3_bucket='shiny-head-aquamarine',
        s3_key=f'{dataset_file}',
        schema='public',
        table=f'{dataset_name}',
        copy_options=["csv"],
    )

    transfer_s3_to_redshift_new_format_task = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift_new_format",
        redshift_conn_id='redshift_default',
        s3_bucket='shiny-head-aquamarine',
        s3_key=f'{dataset_file}',
        schema='public',
        table=f'{dataset_name}',
        copy_options=["csv"],
    )
    
    drop_redshift_table_task = RedshiftSQLOperator(
        task_id='drop_redshift_table',
        sql=f"""
        drop table if exists {dataset_name};
    """
    )

    evaluate_dataset_format_task = BranchPythonOperator(
        task_id = 'evaluate_dataset_format',
        python_callable = evaluate_dataset_format,
        op_kwargs={
            'year': f"{year}",
            'month': f"{month}"
        }
    )

    create_redshift_table_old_format_task = RedshiftSQLOperator(
        task_id='create_redshift_table_old_format',
        sql=f""" 
            create table if not exists {dataset_name} (
              "Duration" varchar,
              "Start date" varchar,
              "End date" varchar,
              "Start station number" varchar,
              "Start station" varchar,
              "End station number" varchar,
              "End station" varchar,
              "Bike number" varchar,
              "Member type" varchar
            ); 
        """
    )

    ## ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual

    create_redshift_table_new_format_task = RedshiftSQLOperator(
        task_id='create_redshift_table_new_format',
        sql=f""" 
            create table if not exists {dataset_name} (
              "ride_id" varchar,
              "rideable_type" varchar,
              "started_at" varchar,
              "ended_at" varchar,
              "start_station_name" varchar,
              "start_station_id" varchar,
              "end_station_Name" varchar,
              "end_station_id" varchar,
              "start_lat" varchar,
              "start_lng" varchar,
              "end_lat" varchar,
              "end_lng" varchar,
              "member_casual" varchar
            ); 
        """
    )

    #create_all_bike_data_old = RedshiftSQLOperator(
    #    task_id=f'create_all_bike_data_old', 
    #    sql=f""" 
    #        create table if not exists all_bike_data_old (
    #          "Duration" varchar,
    #          "Start date" varchar,
    #          "End date" varchar,
    #          "Start station number" varchar,
    #          "Start station" varchar,
    #          "End station number" varchar,
    #          "End station" varchar,
    #          "Bike number" varchar,
    #          "Member type" varchar
    #        ); 
    #    """
    #)

    #create_all_bike_data_new = RedshiftSQLOperator(
    #    task_id=f'create_all_bike_data_new', 
    #    sql=f""" 
    #        create table if not exists all_bike_data_new (
    #          "ride_id" varchar,
    #          "rideable_type" varchar,
    #          "started_at" varchar,
    #          "ended_at" varchar,
    #          "start_station_name" varchar,
    #          "start_station_id" varchar,
    #          "end_station_Name" varchar,
    #          "end_station_id" varchar,
    #          "start_lat" varchar,
    #          "start_lng" varchar,
    #          "end_lat" varchar,
    #          "end_lng" varchar,
    #          "member_casual" varchar
    #        ); 
    #    """
    #)

    append_bike_data_old_format_task = RedshiftSQLOperator(
        task_id='append_bike_data_old_format', 
        sql=f""" 
            insert into all_bike_data_old
            select * from {dataset_name}
            ; 
        """
    )

    append_bike_data_new_format_task = RedshiftSQLOperator(
        task_id='append_bike_data_new_format', 
        sql=f""" 
            insert into all_bike_data_new
            select * from {dataset_name}
            ; 
        """
    )

    #### CREATE TWO SEPARATE UPLOAD TO S3 TASKS

    # initialize_variables_task >>

    download_dataset_task >> unzip_dataset_task >> upload_to_s3_task >> evaluate_dataset_format_task >> [create_redshift_table_new_format_task, create_redshift_table_old_format_task]

    drop_redshift_table_task >> evaluate_dataset_format_task

    create_redshift_table_old_format_task >> transfer_s3_to_redshift_old_format_task >> append_bike_data_old_format_task

    create_redshift_table_new_format_task >> transfer_s3_to_redshift_new_format_task >> append_bike_data_new_format_task

    #initialize_parameters_task >> download_dataset_task >> unzip_dataset_task >> task_upload_to_s3 >> transfer_s3_to_redshift

    #initialize_parameters_task >> drop_redshift_table >> create_redshift_table >> transfer_s3_to_redshift >> append_bike_data