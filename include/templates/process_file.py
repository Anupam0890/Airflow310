from airflow import DAG
from airflow.sdk import task
from datetime import datetime

with DAG(f"process_DAG_ID_HOLDER", start_date=datetime(2025,11, 11), 
         schedule="SCHEDULE_HOLDER", catchup=False) as dag:
    
    @task
    def extract(filename):
        return filename
        
    @task
    def process(filename):
        return filename
        
    @task
    def send_mail(filename):
        print(filename)

    send_mail(process(extract("INPUT_HOLDER")))