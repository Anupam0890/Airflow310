from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException 


def filter_file_extension(filepath: tuple[str, str]) -> dict[str, str]:
    if filepath[0].rsplit('.', 1)[1] == 'zip':
        raise AirflowSkipException(f"Skipping file {filepath[0]} as it has zip extension")
    return {
        'file': filepath[0],
        'path': filepath[1]
    }

@dag
def transforming_and_filtering_2():

    @task
    def extract_files() -> list[str]:
        return ['a.txt', 'b.csv', 'c.zip', 'd.txt']
    
    @task
    def extract_paths() -> list[str]:
        return ['/tmp', '/var/log', '/home','/home/ds']

    @task
    def add_path(file: str, path: str) -> str:
        return f"{path}/{file}"

    @task(trigger_rule='none_failed_min_one_success')
    def print_valid_files(files: list[str]) -> None:
        for file in files:
            print(file)

    filtered_files = extract_files().zip(extract_paths()).map(filter_file_extension)
    valid_files =  add_path.expand_kwargs(filtered_files)
    print_valid_files(valid_files)

transforming_and_filtering_2()