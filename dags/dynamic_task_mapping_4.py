from airflow.sdk import dag, task
from datetime import datetime, timedelta
import random
from airflow.providers.standard.operators.bash import BashOperator

"""
collect the output of your mapped tasks into a classic operator instead of using expand
to produce the output graph
get_files >> [download_file[0], download_file[1], download_file[2]] >> BashOperator
"""

@dag(
    'dynamic_task_mapping_4',
    schedule= '@daily',
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    doc_md=__doc__
)
def dynamic_task_mapping_4():
    @task
    def get_files():
        return ["file_{}".format(nb) for nb in range(random.randint(3, 5))]
    
    @task
    def download_file(folder: str, file: str):
        return f"{folder}/{file}"

    files = download_file.partial(folder="/usr/local").expand(file=get_files())
    # Mapping with a non-taskflow operator
    print_files = BashOperator(
                task_id="print_files",
                bash_command="echo '{{ ti.xcom_pull(task_ids='download_file', dag_id='dynamic_task_mapping_4', key='return_value') }}'"
                )
    
    files >> print_files

dynamic_task_mapping_4()