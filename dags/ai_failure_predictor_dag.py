# dags/ai_failure_predictor_dag.py
from airflow import DAG
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import pandas as pd
import joblib
import random

def predict_failure():
    model = joblib.load("/opt/airflow/models/failure_model.pkl")

    # Simulate or fetch new run data
    new_data = pd.DataFrame([{
        "duration": random.randint(100, 400),
        "retries": random.choice([0, 1, 2]),
        "day_of_week": datetime.now().weekday()
    }])

    pred = model.predict_proba(new_data)[0][1]
    print(f"Predicted failure probability: {pred:.2f}")

    if pred > 0.6:
        # Slack or Alert logic here
        print("High risk of failure! Consider checking upstream tasks.")

    return pred
        
    

def trigger_downstream():
    print("Running actual data pipeline (mock task).")

@dag(
    start_date=datetime(2025, 11, 8),
    schedule="@daily",
    catchup=False,
    tags=["AI", "DataOps"]
)
def ai_failure_predictor_dag():

    @task.branch
    def predict_task():
        if predict_failure() > 0.6:
            return "skip_pipeline"
        else:
            return "run_pipeline"

    @task
    def run_pipeline():
        trigger_downstream()

    @task
    def skip_pipeline():
        pass

    predict_task() >> [run_pipeline(), skip_pipeline()]

ai_failure_predictor_dag()