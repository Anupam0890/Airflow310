from airflow import DAG
from airflow.decorators import task
from datetime import datetime

def create_dag(filename):
    with DAG(f"process_{filename}", start_date=datetime(2025, 11, 8), schedule="@daily",catchup=False) as dag:
        @task
        def extract(filename):
            return filename
        
        @task
        def process(filename):
            return filename
        
        @task
        def send_mail(filename):
            print(filename)

        send_mail(process(extract(filename)))

    return dag

for file in ('file_A.csv', 'file_B.csv', 'file_C.csv'):
    globals()[f"dag_{file}"] = create_dag(file)