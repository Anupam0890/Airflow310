from airflow import DAG
from airflow.operators.python import PythonOperator, BashOperator
from datetime import datetime
import os
import requests

SQL_INPUT_DIR = "/opt/airflow/include/sql_inputs"
DBT_PROJECT_DIR = "/opt/airflow/dbt/models/ai_generated"
OLLAMA_URL = "http://ollama:11434/api/generate"  # Notice: use container name
MODEL_NAME = "codellama"

os.makedirs(DBT_PROJECT_DIR, exist_ok=True)

def call_ollama(prompt: str) -> str:
    """Send prompt to Ollama API inside Docker network"""
    payload = {"model": MODEL_NAME, "prompt": prompt, "stream": False}
    resp = requests.post(OLLAMA_URL, json=payload)
    resp.raise_for_status()
    return resp.json()["response"]

def convert_sql_to_dbt(sql_query: str, model_name: str):
    """Generate dbt model and schema.yml using Ollama"""
    prompt = f"""
    Convert the following SQL into a dbt model.
    - Add ref() for source tables
    - Use materialized='table'
    - Generate schema.yml with basic tests
    Separate SQL and YAML with '### YAML'

    SQL Query:
    {sql_query}
    """
    return call_ollama(prompt)

def process_sql_files(**context):
    """Read .sql files and create dbt models using Ollama"""
    for fname in os.listdir(SQL_INPUT_DIR):
        if not fname.endswith(".sql"): continue
        model_name = os.path.splitext(fname)[0]
        with open(os.path.join(SQL_INPUT_DIR, fname)) as f:
            sql_query = f.read()

        print(f" Processing {fname} via Ollama...")
        ai_output = convert_sql_to_dbt(sql_query, model_name)

        sql_part, yaml_part = ai_output.split("### YAML", 1) if "### YAML" in ai_output else (ai_output, "")
        model_dir = os.path.join(DBT_PROJECT_DIR, model_name)
        os.makedirs(model_dir, exist_ok=True)

        with open(os.path.join(model_dir, f"{model_name}.sql"), "w") as f:
            f.write(sql_part.strip())
        if yaml_part:
            with open(os.path.join(model_dir, "schema.yml"), "w") as f:
                f.write(yaml_part.strip())

        print(f" Created dbt model: {model_name}")

with DAG(
    dag_id="ollama_sql_to_dbt_dag",
    start_date=datetime(2025, 11, 12),
    schedule="@daily",
    catchup=False,
    tags=["ollama", "dbt", "ai"],
) as dag:

    generate_dbt = PythonOperator(
        task_id="generate_dbt_models",
        python_callable=process_sql_files,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --select ai_generated",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --select ai_generated",
    )

    generate_dbt >> dbt_run >> dbt_test
