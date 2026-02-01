from airflow.sdk import dag, task

@dag
def tranforming_and_filtering():

    @task
    def extract_files() -> list[str]:
        return ['a.txt', 'b.csv', 'c.zip', 'd.txt']

    @task
    def add_path(file: str, path: str) -> str:
        return f"{path}/{file}"

    add_path.partial(path="/tmp").expand(file=extract_files())