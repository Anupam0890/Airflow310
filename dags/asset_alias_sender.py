"""
Dynamically create an asset at runtime using the asset alias feature.
This DAG creates a file and registers it as an asset with an alias.
"""

from airflow.sdk import dag, task, AssetAlias, Metadata, Asset
from pendulum import datetime

alias_name = "alias_file_a"


@dag(
    start_date=datetime(2026, 1, 26),
    schedule='@daily',
    catchup=False,
    max_active_runs=1
)
def asset_alias_sender():
    @task
    def get_path() -> str:
        return "/tmp/file_a.txt"
    
    @task(outlets=[AssetAlias(alias_name)])
    def create_file(path: str):
        # Simulate file creation logic
        with open(path, 'w') as f:
            f.write('This is my asset file content.\n')
        yield Metadata(
            asset=Asset(
                name="file_a",
                #uri=f"file://{path}",
                uri = path
            ),
            alias=AssetAlias(alias_name),
            extra={"description": "An example asset file."}
        )

    create_file(get_path())

asset_alias_sender()
