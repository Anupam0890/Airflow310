from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

def my_function(x: int) -> int:
    return x * 2

@dag
def traditional_mapping():

    @task
    def extract_data():
        return [[1], [3], [5]]

    my_task = PythonOperator(
        task_id="my_task",
        python_callable=my_function
    ).expznd(op_args=extract_data())

traditional_mapping()