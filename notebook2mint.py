
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import Param, Variable
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator


def login_push_function(task_instance, **kwargs):
    DOCKER_REPO = "mintcomponents"
    tag = kwargs['ds_nodash']
    params = kwargs['params']
    name = params['component_name']
    import docker
    client = docker.from_env()
    image = f"mintcomponents/{name}"
    USERNAME =  Variable.get("docker_username")
    PASSWORD = Variable.get("docker_password")
    client.login(USERNAME, PASSWORD)
    return client.api.push(image, tag)    
    
    
with DAG(
    'notebook2mint',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['maxiosorio@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    params={
        "component_name": Param(
            default='name',
            type="string"
        ),
        "url": Param(
            default="https://github.com/mosoriob/MIC_model",
            type="string"
        ),
    },
    description='List notebook from a git repository',
    catchup=False,
    tags=['mic'],
    start_date=datetime(2021, 1, 1),
) as dag:
    
    import os

    t1 = BashOperator(
        task_id="bash_task",
        bash_command="/home/airflow/.local/bin/jupyter-repo2docker  --no-run --image-name mintcomponents/{{ params.component_name }}:{{ ds_nodash }} {{ params.url }} ",
        env={"git_url": '{{ params.url if dag_run else "" }}',
             "DOCKER_HOST": 'http://socat:8375',
             "tag": '{{ ds_nodash }}',
             "component_name": '{{ params.component_name }}',
             "DOCKER_USERNAME":  Variable.get("docker_username"),
             "DOCKER_PASSWORD": Variable.get("docker_password"),
             
            },
        dag=dag,
    )
    
    login_push = PythonOperator(
        task_id='login_push_dockerhub',
        python_callable=login_push_function,
        provide_context=True,
        dag=dag
    )
    
    t1 >> login_push
