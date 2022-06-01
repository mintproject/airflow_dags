import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator

# Change these to your identifiers, if needed.
AWS_S3_CONN_ID = "mint2"



def s3_extract():
    with open("/tmp/test.txt", "w") as f:
        f.write("Hello World")
    

    key_path = "test.txt"
    bucket_name = "components"
    #write file
    source_s3 = S3Hook(AWS_S3_CONN_ID)
    source_s3.load_file("/tmp/test.txt", key_path, bucket_name)

        
with DAG(
    dag_id="s3_extract",
    start_date=datetime(2022, 2, 12),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="s3_extract_task",
        python_callable=s3_extract
    )
    t1