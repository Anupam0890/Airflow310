from airflow import DAG
from airflow.sdk import task
from datetime import datetime

with DAG(f"process_file_a", start_date=datetime(2025,11, 11), 
         schedule="@hourly", catchup=False) as dag:
    
    @task
    def extract(filename):
        return filename
        
    @task
    def process(filename):
        return filename
        
    @task
    def send_mail(filename):
        print(filename)

    send_mail(process(extract("file_a.csv")))