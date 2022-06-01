#a comment
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task

with DAG(
    'bikes_rent',
    params={
        "files": Param(
            default="",
            type="string"
        ),
    },
    default_args={
        'depends_on_past': False,
        'email': ['maxiosorio@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Download the files from a thread',
    catchup=False,
    tags=['mint'],
    start_date=datetime(2021, 1, 1),
) as dag:

    @task.virtualenv(
        task_id="virtualenv_python", requirements=["colorama==0.4.0"], system_site_packages=False
    )
    def callable_virtualenv():
        """
        Example function that will be performed in a virtual environment.
        Importing at the module level ensures that it will not attempt to import the
        library before it is installed.
        """
        from time import sleep

        from colorama import Back, Fore, Style

        print(Fore.RED + 'some red text')
        print(Back.GREEN + 'and with a green background')
        print(Style.DIM + 'and in dim text')
        print(Style.RESET_ALL)
        for _ in range(10):
            print(Style.DIM + 'Please wait...', flush=True)
            sleep(10)
        print('Finished')
    virtualenv_task = callable_virtualenv()
