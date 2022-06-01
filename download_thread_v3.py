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

# Change these to your identifiers, if needed.
AWS_S3_CONN_ID = "s3_prod"

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
    if not os.path.exists(directory_thread_id):
        os.mkdir(directory_thread_id)
    else:
        import shutil
        shutil.rmtree(directory_thread_id, ignore_errors=True)
    for execution in executions:
        print(execution)
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
    bucket_name = "components"
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
    'download_thread_v3',
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
    tags=['mint', 'production'],
    start_date=datetime(2021, 1, 1),
) as dag:

    query_execution = PythonOperator(
        task_id='query_execution',
        python_callable=query_execution_function,
        provide_context=True,
        dag=dag
    )

    download_link = PythonOperator(
        task_id='download_link',
        python_callable=download_data_function,
        provide_context=True,
        dag=dag
    )
    
    import os
    from jinja2 import Template
    dir = os.path.abspath(os.path.dirname(__file__))
    with open(f'{dir}/template_email.html.j2') as file:
        template = Template(file.read())

    output = template.render(size="{{ ti.xcom_pull(key='size') }}", link="{{ ti.xcom_pull(key='link') }}")



    send_email = EmailOperator( 
        task_id='send_email', 
        to="{{ params.email }}", 
        subject="Your MINT data is ready", 
        html_content=output,
        dag=dag
    )

    query_execution >> download_link >> send_email
