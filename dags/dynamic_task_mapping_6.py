from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException 


def filter_file_extension(file: str) -> str:
    if file.rsplit('.', 1)[1] == 'zip':
        raise AirflowSkipException(f"Skipping file {file} as it has zip extension")
    return file

@dag
def transforming_and_filtering():

    @task
    def extract_files() -> list[str]:
        return ['a.txt', 'b.csv', 'c.zip', 'd.txt']

    @task
    def add_path(file: str, path: str) -> str:
        return f"{path}/{file}"

    @task
    def print_valid_files(files: list[str]) -> None:
        for file in files:
            print(file)

    filtered_files = extract_files().map(filter_file_extension)
    valid_files =  add_path.partial(path="/tmp").expand(file=filtered_files)
    print_valid_files(valid_files)

transforming_and_filtering()