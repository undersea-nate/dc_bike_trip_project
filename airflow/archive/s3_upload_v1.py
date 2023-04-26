from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def upload_to_s3(filename: str, key:str, bucket_name.str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

with DAG(
    dag_id = "s3_dag",
    schedule_interval = "@daily",
    start_date=datetime(2023,3,19),
    catchup=False
) as dag:
    
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3'
        python_callable=upload_to_s3,
        op_kwargs={
            'filename':'C:\Users\mccli\OneDrive\Documents\GitHub\DEZ-final-project\airflow\data\NetflixBestOf_reddit.csv',
            'key': 'NetflixBestOf_reddit.csv',
            'bucket_name': 'bds-airflow-bucket'
        }

    )

