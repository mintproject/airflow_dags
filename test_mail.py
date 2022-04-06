import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator


with DAG(
    dag_id="test_email",
    start_date=datetime(2022, 2, 12),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    import os
    from jinja2 import Template
    dir = os.path.abspath(os.path.dirname(__file__))
    with open(f'{dir}/template_email.html.j2') as file:
        template = Template(file.read())

    output = template.render(size='2', link='Stuff')

    send_email = EmailOperator( 
        task_id='send_email', 
        to='mosorio@inf.utfsm.cl', 
        subject='ingestion complete', 
        html_content=output
        
    )
