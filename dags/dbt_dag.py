from datetime import datetime
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.bash import BashOperator

DBT_DIR = '/opt/airflow/dbt'
PROFILE_DIR = '/opt/airflow/dbt'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 4),
}

with DAG(
    dag_id='dbt_airflow_example',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_DIR} && dbt seed --profiles-dir {PROFILE_DIR}',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && dbt run --profiles-dir {PROFILE_DIR}',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && dbt test --profiles-dir {PROFILE_DIR}',
    )

    dbt_seed >> dbt_run >> dbt_test
