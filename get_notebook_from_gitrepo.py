
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import Param

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    'find_notebooks',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email': ['maxiosorio@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    params={
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
    
     
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="bash_task",
        bash_command="mkdir -p repos && git clone $git_url git_{{ run_id }} && find git_{{ run_id }} -name *.ipynb ",
        env={"git_url": '{{ params.url if dag_run else "" }}'},
        dag=dag,
    )
    

    t1
