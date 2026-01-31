from airflow.sdk import asset, Metadata

@asset(uri="/tmp/file_b.txt", schedule="@daily")
def asset_ex_2(self, ds=None):
    with open(self.uri, "w") as f:
        f.write("Content of File B")

    yield Metadata(
        self,
        extra={
            "description": "File B contains data to be sent",
            "created_at": ds
        }
    )

@asset(name="report", schedule=file_b)
def report_file_b(context, file_b):
    events = context["inlet_events"][file_b]
    print(events[-1])