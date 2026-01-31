from airflow.sdk import dag, task, AssetAlias, Asset, Metadata
from pendulum import datetime

alias_name = "alias_file_a"

@dag(
    start_date=datetime(2026, 1, 26),
    schedule=[AssetAlias(alias_name)]
)
def asset_alias_consumer_2():
    @task(inlets=[AssetAlias(alias_name)])
    def consume_file_a(inlet_events=None):
        events = inlet_events[AssetAlias(alias_name)]
        print(events[-1])
        print(events[-1].asset.uri)
        with open(events[-1].asset.uri, 'r') as f:
            content = f.read()
            print("File content:")
            print(content)

    consume_file_a()

asset_alias_consumer_2()
