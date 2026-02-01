from airflow.sdk import dag, task

def filter_file_extension(file: str) -> str:
    if file.rsplit('.', 1)[1] == 'zip':
        return None
    
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

    files_path =  add_path.partial(path="/tmp").expand(file=extract_files())
    filtered_files = files_path.map(filter_file_extension)
    print_valid_files(filtered_files)

transforming_and_filtering()