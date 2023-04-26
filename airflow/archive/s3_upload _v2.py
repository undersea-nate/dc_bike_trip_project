from datetime import datetime, timedelta
from airflow import DAG
# Import below module to work with S3 operator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

with dag:
 
    Upload_to_s3 = S3CreateObjectOperator(
        task_id="Upload-to-S3",
        aws_conn_id= 's3_conn',
        s3_bucket='shiny-head-aquamarine',
        s3_key='data/NetflixBestOf_reddit.csv',
        data="{{ ti.xcom_pull(key='final_data') }}",    
    )

    api_hit_task >> Upload_to_s3