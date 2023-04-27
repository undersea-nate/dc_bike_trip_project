# Airflow Documentation

## Docker-Compose

The docker-compose file is adapted from the [official template provided by Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). The environment includes additional parameters for project and bucket IDs for GCS.

The airflow image required additional parameters, so the docker-compose image referenced a custom DockerFile created in the same directory. 

## DockerFile

The DockerFile has a base image of Apache Airflow 2.5.2 with additional dependencies specified. 

The requirements.txt file is installed upon initialized to include required Python packages, most noteably apache-airflow-providers-google which allows Airflow to connect to GCP.

In addition to downloading Google Cloud SDK, which provides a software framework to develop and connect to GCP, the DockerFile also specifies the installation of unzip as a bash command which is required to unzip the Capital Bikeshare datasets from their website.

## Download_And_Upload_GCS DAG

The Download_And_Upload_GCS DAG fascilitates the process of downloading the individual folders from the Capital Bikeshare website, unzipping them, and uploading each month's dataset as a parquet file to GCS. This DAG is composed of four tasks:

### download_dataset_task 

```python
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_zip}"
    )
```

This task is a simple bash operator which downloads the dataset zip file into local home. 

### unzip_dataset_task 

```python
    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip -o {path_to_local_home}/{dataset_zip} -d {path_to_local_home}",
    )
```

This task is another simple bash operator - its sole purpose is to unzip the folder so the program can access the dataset (which is stored as a CSV)

### format_to_parquet_task 

```python
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )
```

This task is a python operator, which calls a previously defined Python function with parameters defined as op_kwargs. This is the callable python function which is named format_to_parquet:

```python
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))
```

This function simply takes the CSV file and converts it to Parquet. Parquet files are uploaded to GCS faster than CSV files, so this task should improve the speed of this task.

### local_to_gcs_task

```python
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
```

This task is another python operator that is used to upload the Parquet file to GCS. 

```python
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
```

This function references the GCP project and bucket IDs that were referenced within the DockerFile and defined earlier in this DAG:

```python
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
```

## GCS_To_BQ_Old_Format DAG

This DAG is responsable for consolidating all the datasets created before April 2020 into a single external table in BigQuery. 

### bigquery_external_table_task

This task creates an external table for all the datasets uploaded to the GCS bucket with the Download_and_Upload_GCS DAG with the older format (which were all uploaded to a separate folder in GCS).

```python
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
```

BigQuery can automatically detect the correct column type without having to be explicitely defined. This can be changed later but is very helpful at this stage of data processing.

### bq_create_partitioned_table_job

This task uses a BigQuery job operator which can be used for [several different purposes](https://cloud.google.com/bigquery/docs/jobs-overview). The purpose of this specific job is to create a new, partitioned table based upon the previously created external table. This table will be transformed in DBT, but the partitions should enable faster SQL queries, which is helpful both in analyses and manipulating data for visualizations.

```python
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{FORMAT}_{DATASET}_partitioned_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )
```

CREATE_BQ_TBL_QUERY is defined earlier within the DAG:

```python
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
```

## GCS_To_BQ_New_Format DAG

This DAG is almost identical to the GCS_To_BQ_Old_Format DAG, with the exception of the CREATE_BQ_TBL_QUERY, which is defined as this:

```python
    CREATE_BQ_TBL_QUERY = ( #removed start_station_id and end_station_id because the columns were corrupted
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
```

Two columns (start_station_id and end_station_id) were corrupted and caused issues when querying the dataset later in DBT, so they were removed from the data in this step. 

