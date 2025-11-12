from airflow.sdk import dag, task
from datetime import datetime
import random

@dag(
    'dynamic_task_mapping_2',
    schedule= '@daily',
    start_date=datetime(2025, 11, 5),
    catchup=False
)
def dynamic_task_mapping_2():
    @task
    def get_files():
        return [f"file_{nb}" for nb in range(random.randint(3, 5))]
    
    @task
    def download_file(folder: str, file: str):
        print(f"{folder}/{file}")

    files = download_file.partial(folder="/usr/local").expand(file=get_files())
    


dynamic_task_mapping_2()