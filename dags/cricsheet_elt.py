import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)


default_args = {    
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'cricsheet_elt',
    default_args=default_args,
    description="Loads Cricsheet data into S3 and transforms with Spark \
        for ball-by-ball analysis",
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    
    players_url = 'https://cricsheet.org/register/people.csv'
    players_file_path = f'{os.getenv("AIRFLOW_HOME")}/data/people.csv'

    download_players_file = BashOperator(
        task_id='download_players_file',
        bash_command=f'curl -o {players_file_path} {players_url}'
    )

    matches_url = 'https://cricsheet.org/downloads/t20s_csv2.zip'
    data_folder_path = f'{os.getenv("AIRFLOW_HOME")}/data'
    matches_file_path = f'{data_folder_path}/matches.zip'
    matches_folder_path = f'{data_folder_path}/matches'
    matches_info_path = f'{data_folder_path}/matches_info'

    download_matches_file = BashOperator(
        task_id='download_matches_file',
        bash_command=f'curl -o {matches_file_path} {matches_url}'
    )

    unzip_matches_file = BashOperator(
        task_id='extract_matches_file',
        bash_command=f'unzip -oX {matches_file_path} -d {matches_folder_path} \
        && mkdir -p {matches_info_path} \
        && mv {matches_folder_path}/*_info.csv {matches_info_path}'
    )

    s3_bucket_name = "cricsheet-raw-data"

    create_s3_bucket = S3CreateBucketOperator(
        task_id='create_s3_bucket',
        bucket_name=s3_bucket_name
    )

    def upload_files_to_s3(local_folder, s3_bucket, s3_folder):

        s3_hook = S3Hook(aws_conn_id='aws_default')

        for root, dirs, files in os.walk(local_folder):
            for file_name in files:
                if file_name.endswith(".csv"):
                    full_path = os.path.join(root, file_name)
                    s3_key = f"{s3_folder}/{file_name}"
                    
                    s3_hook.load_file(full_path, s3_key, bucket_name=s3_bucket, replace=True)
                    print(f"Uploaded {file_name} to s3://{s3_bucket}/{s3_key}")

    upload_innings_to_s3 = PythonOperator(
        task_id='upload_innings_to_s3',
        python_callable=upload_files_to_s3,
        op_kwargs={
            'local_folder': matches_folder_path,
            's3_bucket': s3_bucket_name,
            's3_folder' : 'innings'
        }
    )

    upload_info_to_s3 = PythonOperator(
        task_id='upload_info_to_s3',
        python_callable=upload_files_to_s3,
        op_kwargs={
            'local_folder': matches_info_path,
            's3_bucket': s3_bucket_name,
            's3_folder' : 'info'
        }
    )


    upload_players_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_players_to_s3',
        filename=players_file_path,
        dest_key='people/people.csv',
        dest_bucket=s3_bucket_name,
        replace=True
    )

    download_matches_file  >> unzip_matches_file

    (
        [download_players_file, unzip_matches_file] 
        >> create_s3_bucket 
        >> [upload_innings_to_s3, upload_players_to_s3, upload_info_to_s3]
    )
