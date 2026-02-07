from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

def my_function(x: int) -> int:
    return x * 2

def _extract_data():
    return [[1], [3], [5]]

@dag
def traditional_mapping_2():

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data
    )
    my_task = PythonOperator.partial(
        task_id="my_task",
        python_callable=my_function
    ).expand(op_args=extract_data.output) # context["ti"].xcom_pull(task_ids="extract_data")

    # Have to define the dependency explicitly for traditional Operator unlike task decorator
    extract_data >> my_task

traditional_mapping_2()