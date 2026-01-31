from airflow.sdk import dag, task, AssetAlias, Asset, Metadata
from pendulum import datetime

alias_name = "alias_file_a"

@dag(
    start_date=datetime(2026, 1, 26),
    schedule=[AssetAlias(alias_name)]
)
def asset_alias_consumer():
    @task
    def consume_file_a():
        pass

    consume_file_a()

asset_alias_consumer()
