import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'bike_data_dev')

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

FORMAT = "new"
INPUT_FILETYPE = "parquet"
DATASET = "bike_data"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(dag_id="gcs_to_bq_new_format",
        schedule_interval="@daily",
        max_active_runs=1,
        default_args=default_args,
        catchup=False) as dag:

    #move_files_gcs_task = GCSToGCSOperator(
    #    task_id=f'move_{FORMAT}_files_task',
    #    source_bucket=BUCKET,
    #    source_object=f'{FORMAT}/*.{INPUT_FILETYPE}',
    #    destination_bucket=BUCKET,
    #    destination_object=f'{FORMAT}/{FORMAT}_{DATASET}.{INPUT_FILETYPE}',
    #    move_object=True
    #)
    
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{FORMAT}_{DATASET}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{FORMAT}_{DATASET}_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                "sourceUris": [f"gs://{BUCKET}/{FORMAT}/*"],
            },
        },
    )

    CREATE_BQ_TBL_QUERY = ( #removed start_station_id and end_station_id because the columsn were corrupted
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{FORMAT}_{DATASET} \
        PARTITION BY date\
        AS \
        SELECT \
        EXTRACT(date FROM started_at) as date, \
        EXTRACT(year FROM started_at) as year, \
        EXTRACT(month FROM started_at) as month, \
        rideable_type, \
        started_at, \
        ended_at, \
        start_station_name, \
        end_station_name, \
        start_lat, \
        start_lng, \
        end_lat, \
        end_lng, \
        member_casual, \
        FROM {BIGQUERY_DATASET}.{FORMAT}_{DATASET}_external_table as table;"
    )

    #CREATE_BQ_TBL_QUERY = (
    #    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{FORMAT}_{DATASET} \
    #    PARTITION BY date\
    #    AS \
    #    SELECT \
    #    EXTRACT(date FROM started_at) as date, \
    #    EXTRACT(year FROM started_at) as year, \
    #    EXTRACT(month FROM started_at) as month, \
    #    CAST(rideable_type as STRING) as rideable_type, \
    #    CAST(started_at as STRING) as started_at, \
    #    CAST(ended_at as STRING) as ended_at, \
    #    CAST(start_station_name as STRING) as start_station_name, \
    #    CAST(end_station_name as STRING) as end_station_name, \
    #    CAST(start_lat as STRING) as start_lat, \
    #    CAST(start_lng as STRING) as start_lng, \
    #    CAST(end_lat as STRING) as end_lat, \
    #    CAST(end_lng as STRING) as end_lng, \
    #    CAST(member_casual as STRING) as member_casual, \
    #    FROM {BIGQUERY_DATASET}.{FORMAT}_{DATASET}_external_table as table;"
    #)

    #CREATE_BQ_TBL_QUERY = (
    #    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{FORMAT}_{DATASET} \
    #    PARTITION BY date\
    #    AS \
    #    SELECT \
    #    EXTRACT(date FROM started_at) as date, \
    #    EXTRACT(year FROM started_at) as year, \
    #    EXTRACT(month FROM started_at) as month, \
    #    table.* \
    #    FROM {BIGQUERY_DATASET}.{FORMAT}_{DATASET}_external_table as table;"
    #)



    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{FORMAT}_{DATASET}_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    #move_files_gcs_task 
    bigquery_external_table_task >> bq_create_partitioned_table_job