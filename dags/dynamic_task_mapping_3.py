from airflow.sdk import dag, task
from datetime import datetime
import random
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    'dynamic_task_mapping_3',
    schedule= '@daily',
    start_date=datetime(2025, 11, 5),
    catchup=False
)
def dynamic_task_mapping_3():
    @task
    def get_files():
        return ["file_{}".format(nb) for nb in range(random.randint(3, 5))]
    
    @task
    def download_file(folder: str, file: str):
        return f"ls {folder}/{file}; exit 0"

    files = download_file.partial(folder="/usr/local").expand(file=get_files())
    # Mapping with a non-taskflow operator
    BashOperator.partial(task_id="ls_file").expand(bash_command=files)

dynamic_task_mapping_3()