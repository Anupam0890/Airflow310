from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="docker_curl_test",
    start_date=datetime(2025, 11, 13),
    schedule=None,
    catchup=False,
) as dag:

    curl_test = DockerOperator(
        task_id="curl_test",
        image="curlimages/curl:latest",
        api_version="auto",
        auto_remove=True,
        command="curl -v http://example.com",
        docker_url="unix://var/run/docker.sock",   # Host Docker daemon
        network_mode="airflow_default",            # IMPORTANT!
    )

    curl_test
