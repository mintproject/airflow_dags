from concurrent.futures import thread
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models.variable import Variable
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
# Operators; we need this to operate!
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3_bucket import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
import json
from typing import Tuple, List


# Change these to your identifiers, if needed.
AWS_S3_CONN_ID = "s3_prod"

def generate_report(response : dict) -> Tuple[List[str], List[dict]] :
    """Prepare the report from the response to be wrriten as a CSV file

    Args:
        response (dict): GraphQL reponse from the server

    Returns:
        Tuple[List[str], List[dict]]:  List of headers and list of values
    """
    executions = response["data"]["execution"]

    #generate headers
    headers = ["directory_mint_id"]
    values = []

    for execution in executions:
        value_execution = {}
        value_execution["directory_mint_id"] = execution["id"]
        for parameter_item in execution["parameter_bindings"]:
            parameter = parameter_item
            value = parameter["parameter_value"]
            name = parameter["model_parameter"]["name"]
            if name not in headers:
                headers.append(name)
            value_execution[name] = value

        for data_item in execution["data_bindings"]:
            data = data_item["model_io"]
            name = data["name"]
            value = data_item["resource"]["name"]
            if name not in headers:
                headers.append(name)
            value_execution[name] = value
        
        values.append(value_execution)

    return headers, values

def convert_csv_to_html(headers, values):
    from prettytable import PrettyTable
    table = PrettyTable(headers)
    for value in values:
        print(list(value.values()))
        table.add_row(list(value.values()))
    code = table.get_html_string()
    with open("report.html", "w") as f:
        f.write(code)

def write_csv(headers: List[str], values: List[dict], directory: str, filename: str) -> str:
    """Create a CSV file from the headers and values

    Args:
        headers (List[str]): List of headers
        values (List[dict]): List of values
        dir (str): Directory to write the file to
        filename (str): Name of the file to write

    Returns:
        str: Path to the file
    """
    import csv
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(os.path.join(directory, filename), 'w') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(values)
    return os.path.join(directory, filename)

def convert_string(str):
    import re
    FLOAT_PATTERN = re.compile(r'^-?\d+.{1}\d+$')
    INT_PATTERN = re.compile(r'^-?\d+$')
    if FLOAT_PATTERN.match(str):
        return float(str)
    if INT_PATTERN.match(str):
        return int(str)
    return str

def post_query(query, endpoint, secret, variables = {}):
    import requests
    print(endpoint)
    if not endpoint.startswith('https://'):
        endpoint = f'https://{endpoint}'
    response = requests.post(endpoint, json = {
        "query": query,
        "variables": variables
    }, headers = {"X-Hasura-Admin-Secret": secret})
    print(response.status_code)
    return response.json()

def query_execution_function(ti, **kwargs):
    """
    Print out the "foo" param passed in via
    `airflow tasks test example_passing_params_via_test_command run_this <date>
    -t '{"foo":"bar"}'`
    """
    import re
    params = kwargs['params']
    query = """
    query executions_for_thread_model($threadId: String!) {
      execution(where: {thread_model_executions: {thread_model: { thread_id: {_eq: $threadId}}}}) {
        id
        parameter_bindings {
          model_parameter {
            name
          }
          parameter_value
        }
        data_bindings {
          model_io {
            name
          }
          resource {
            id
            name
            url
            spatial_coverage
          }
        }
        results {
          model_output {
            name
          }
          resource {
            id
            name
            url
          }
        }
      }
    }
    """

    executions = []
    response = post_query(query, params["graphql_endpoint"], Variable.get("graphql_dev_secret"), {"threadId": params["thread_id"]})
    ti.xcom_push(key="response", value=response)
    return response

def download_file(url, directory):
    """Download a file from a url and save it to a directory

    Args:
        url (str): The URL to download the file from
        directory (str): The directory to save the file to
    """
    import requests
    filename = url.split('/')[-1]
    print(url)
    r = requests.get(url, stream=True)
    with open(directory + '/' + filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk:
                f.write(chunk)
                f.flush()

def download_data_function(task_instance, **kwargs):
    import os
    params = kwargs['params']
    response = task_instance.xcom_pull(key="response")
    executions = []
    
    for ex in response["data"]["execution"]:
        execution = {
            "id": ex["id"],
            "location": {
                "lat": None,
                "lon": None
            },
            "parameters": {},
            "inputs": {},
            "outputs": {}
        }
        for p in ex["parameter_bindings"]:
            execution["parameters"][p["model_parameter"]["name"]] = convert_string(p["parameter_value"])
        for d in ex["data_bindings"]:
            execution["inputs"][d["model_io"]["name"]] = d["resource"]["url"]
            if d["resource"]["spatial_coverage"] is not None:
                coords = d["resource"]["spatial_coverage"]["coordinates"]
                execution["location"]["lat"] = coords[1]
                execution["location"]["lon"] = coords[0]
        for d in ex["results"]:
            opurl = d["resource"]["url"]
            execution["outputs"][d["model_output"]["name"]] = opurl
        executions.append(execution)    
    
    
    directory_thread_id = params['thread_id']
    
    headers, values = generate_report(response)
    
    if not os.path.exists(directory_thread_id):
        os.mkdir(directory_thread_id)
    else:
        import shutil
        shutil.rmtree(directory_thread_id, ignore_errors=True)
    write_csv(headers, values, directory_thread_id, "report.csv")

    for execution in executions:
        #download files
        urls = []
        execution_id = execution['id']
        #create a directory
        execution_directory = os.path.join(directory_thread_id, execution_id)
        if not os.path.exists(execution_directory):
            os.makedirs(execution_directory)
        os.mkdir(os.path.join(execution_directory, "inputs"))
        os.mkdir(os.path.join(execution_directory, "outputs"))

        for input in execution['inputs'].values():
            download_file(input, os.path.join(execution_directory, "inputs"))
        for output in execution['outputs'].values():
            download_file(output, os.path.join(execution_directory, "outputs"))

    #compress directory into a zip file
    import shutil
    shutil.make_archive(directory_thread_id, 'zip', directory_thread_id)
    key_path = f"{directory_thread_id}.zip"
    print(key_path)
    bucket_name = "compressfiles"
    #write file
    source_s3 = S3Hook(AWS_S3_CONN_ID)
    source_s3.load_file(key_path, key_path, bucket_name, replace=True)
    
    link = f"https://s3.mint.isi.edu/{bucket_name}/{key_path}"
    size = os.path.getsize(key_path) / 1024 / 1024
    size = "{:.2f}".format(size)
    task_instance.xcom_push(key="link", value=link)
    task_instance.xcom_push(key="size", value=size)

    return link

with DAG(
    'download_thread_dev_v3',
    params={
        "thread_id": Param(
            default="fSLqbaAeoGyqYuGZehbF",
            type="string"
        ),
        "graphql_endpoint": Param(
            default="https://graphql.dev.mint.isi.edu/v1/graphql",
            type="string"
        ),
        "email": Param(
            default="none@none.none",
            type="string",
            description="Email address to send the link to",
            format='idn-email',
            minLength=1,
            maxLength=255,
        ),
        "problem_statement_name": Param(
            default="",
            type="string",
            description="Problem Statement Name",
            minLength=0,
            maxLength=255,
        ),
        "subtask_name": Param(
            default="",
            type="string",
            description="Sub task Name",
            minLength=0,
            maxLength=255,
        ),
        "subtask_url": Param(
            default="",
            type="string",
            description="The url of the subtask",
            minLength=0,
            maxLength=255,
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

    # TASK 1: Query executions
    query_execution = PythonOperator(
        task_id='query_execution',
        python_callable=query_execution_function,
        provide_context=True,
        dag=dag
    )

    
    # TASK 2: Send email: we are preparing the data
    import os
    from jinja2 import Template
    directory = os.path.abspath(os.path.dirname(__file__))
    
    with open(f'{directory}/template_download_processing.html.j2') as file:
        template_processing_message = Template(file.read())
        processing_message = template_processing_message.render(
            subtask_url="{{ params.subtask_url }}",
            subtask_name="{{ params.subtask_name }}",
            problem_statement_name="{{ params.problem_statement_name }}"
        )

    send_email_processing = EmailOperator( 
        task_id='send_email_processing', 
        to="{{ params.email }}", 
        subject="Your MINT data: {{ params.problem_statement_name }} - {{ params.subtask_name }}", 
        html_content=processing_message,
        dag=dag
    )
        
    # TASK 3: Download data, compress and upload it

    download_link = PythonOperator(
        task_id='download_link',
        python_callable=download_data_function,
        provide_context=True,
        dag=dag
    )

        
    # TASK 4: Send email: we are ready

    with open(f'{directory}/template_download_ready.html.j2') as file:
        template_ready_message = Template(file.read())
        ready_message = template_ready_message.render(
            size="{{ ti.xcom_pull(key='size') }}",
            link="{{ ti.xcom_pull(key='link') }}",
            subtask_url="{{ params.subtask_url }}",
            subtask_name="{{ params.subtask_name }}",
            problem_statement_name="{{ params.problem_statement_name }}"
        )

    send_email_ready = EmailOperator( 
        task_id='send_email_ready', 
        to="{{ params.email }}", 
        subject="Your MINT data: {{ params.problem_statement_name }} - {{ params.subtask_name }}", 
        html_content=ready_message,
        dag=dag
    )
    
    # Workflow

    query_execution >> send_email_processing >> download_link >> send_email_ready
