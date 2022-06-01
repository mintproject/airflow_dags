from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import Param, Variable

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG, AirflowException
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator


# Operators; we need this to operate!

def cwltool_function(params: dict):
    """Run a CWL workflow using cwltool

    Args:
        params (dict): A dict that contains two keys: 'values' and 'url'
    """
    import os
    import json
    import requests
    import logging
    import uuid
    from cwltool import main
    from utils import create_directory
    #obtain the parameters
    values = json.loads(params["values"])
    url = params["url"]
    
    #create the directory to store the outputs
    out_dir = create_directory("run_cwl", str(uuid.uuid1()))
    cwl_file = os.path.join(out_dir, "cwl.json")
    values_file = os.path.join(out_dir, "values.json")

    #download file and save it as cwl.yml file
    r = requests.get(url)
    with open(cwl_file, "wb") as f:
        f.write(r.content)

    #write values to json file
    with open(values_file, "w") as f:
        json.dump(values, f)
        
    logger = logging.getLogger("airflow.task")
    print(logger.handlers)
    #run cwltool
    main.main(["--enable-pull", "--no-read-only", "--debug",  "--outdir", out_dir, cwl_file, values_file], logger_handler=logger.handlers[0])
    response =  {"image_name": "true"}
    return response    

with DAG(
    "cwltool",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["maxiosorio@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "url": Param(
            default="https://raw.githubusercontent.com/mintproject/airflow/master/tests/cwl/simple/cwl.yml",
            type="string",
        ),
        "values": Param(
            type="string",
            default='{"tarfile":{"class":"File","path":"https://raw.githubusercontent.com/mintproject/airflow/master/tests/cwl/simple/hello.tar"}}'
        ),
    },
    description="Run CWL specification",
    catchup=False,
    tags=["mic", "production"],
    start_date=datetime(2021, 1, 1),
) as dag:
    now = "{{ ds_nodash }}"
    cwltool_task = PythonVirtualenvOperator(
        task_id="cwltool_task",
        requirements=["cwl-runner", "cwltool"],
        python_callable=cwltool_function,
        op_kwargs={"values": "{{ params.values }}", "url": "{{ params.url }}"},
        dag=dag,
    )

    cwltool_task
