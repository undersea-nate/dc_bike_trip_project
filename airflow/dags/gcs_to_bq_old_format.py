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

FORMAT = "old"
INPUT_FILETYPE = "parquet"
DATASET = "bike_data"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(dag_id="gcs_to_bq_old_format",
        schedule_interval="@daily",
        max_active_runs=1,
        default_args=default_args,
        catchup=False) as dag:

    move_files_gcs_task = GCSToGCSOperator(
        task_id=f'move_{FORMAT}_files_task',
        source_bucket=BUCKET,
        source_object=f'{FORMAT}/*.{INPUT_FILETYPE}',
        destination_bucket=BUCKET,
        destination_object=f'{FORMAT}/{FORMAT}_{DATASET}.{INPUT_FILETYPE}',
        move_object=True
    )
    
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

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{FORMAT}_{DATASET} \
        PARTITION BY date\
        AS \
        SELECT \
        EXTRACT(date FROM start_date) as date, \
        EXTRACT(year FROM start_date) as year, \
        EXTRACT(month FROM start_date) as month, \
        table.* \
        FROM {BIGQUERY_DATASET}.{FORMAT}_{DATASET}_external_table as table;"
    )


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

    bigquery_external_table_task >> bq_create_partitioned_table_job
