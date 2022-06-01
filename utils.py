def create_directory(dag_id: str, _id: str) -> str:
    """Create a directory where the inputs or outputs are download

    Args:
        dag_id (str): The dag name
        _id (str): The id of the task

    Returns:
        str: Full path of the directory
    """
    import os
    airflow_directory = os.path.join("/data", "airflow")
    files_directory = os.path.join(airflow_directory, "files")
    dags_directory = os.path.join(files_directory, "dags")
    if dag_id is not None:
        dag_directory = os.path.join(dags_directory, dag_id)
    else:
        dag_directory = os.path.join(dags_directory, "unknown")

    execution_directory = os.path.join(dag_directory, _id)
    if not os.path.exists(execution_directory):
        os.makedirs(execution_directory)
    else:
        import shutil
        shutil.rmtree(execution_directory, ignore_errors=True)
    return execution_directory
