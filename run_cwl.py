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
    from cwltool import main
    #obtain the parameters
    values = json.loads(params["values"])
    url = params["url"]
    ds_nodash = "test"

    #create directory for the output
    out_dir = os.path.join(os.getcwd(), "output")
    os.makedirs(out_dir, exist_ok=True)
    print("out_dir: ", out_dir)

    #download file and save it as cwl.yml file
    r = requests.get(url)
    with open("./cwl.yml", "wb") as f:
        f.write(r.content)

    #write values to json file
    with open("./values.json", "w") as f:
        json.dump(values, f)
        
    logger = logging.getLogger("airflow.task")
    print(logger.handlers)
    #run cwltool
    main.run(["--enable-pull", "--leave-tmpdir", "--debug",  "--log-dir", out_dir, "--outdir", out_dir, "./cwl.yml", "./values.json"], logger_handler=logger.handlers[0])
    
    #print files
    for file in os.listdir(out_dir):
        print(file)

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
            default="https://raw.githubusercontent.com/mintproject/airflow/master/tests/cwl/aquifer/cwl.yml",
            type="string",
        ),
        "values": Param(
            type="string",
            default='{"case":2,"conc":30,"contaminant_transport_list":{"class":"File","path":"file1.png"},"aquifer_file":{"class":"File","path":"https://raw.githubusercontent.com/mintproject/aquifer/master/aquifer.ftl"},"degr":"0.5","aquifer_file_case2":{"class":"File","path":"https://raw.githubusercontent.com/mintproject/aquifer/master/aquifer2"},"aquifer_file_case1":{"class":"File","path":"https://raw.githubusercontent.com/mintproject/aquifer/master/aquifer1"},"hydr":"2","inic":"1","rech":"1","arrival_time_viz":"arrival_time.png","break_through_curve_viz":"break_through_curve.png","ground_water_flow_field_viz":"groundwaterflowfield.png"}'
        ),
    },
    description="Run CWL specification",
    catchup=False,
    tags=["mic"],
    start_date=datetime(2021, 1, 1),
) as dag:

    cwltool_task = PythonVirtualenvOperator(
        task_id="cwltool",
        requirements=["cwl-runner", "cwltool"],
        python_callable=cwltool_function,
        op_kwargs={"values": "{{ params.values }}", "url": "{{ params.url }}", "ds_nodash": "{{ ds_nodash }}"},
        dag=dag,
    )

    cwltool_task