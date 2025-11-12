from airflow.sdk import dag, task
from datetime import datetime
import random

@dag(
    'dynamic_task_mapping_1',
    schedule= '@daily',
    start_date=datetime(2025, 11, 5),
    catchup=False
)
def dynamic_task_mapping_1():
    @task
    def get_files():
        return ["file_{}".format(nb) for nb in range(random.randint(3, 5))]
    
    @task
    def download_file(file: str):
        print(file)

    # files = download_file.expand(file=["file_A", "file_B", "file_C"])
    files = download_file.expand(file=get_files())

dynamic_task_mapping_1()