"""Concatenating outputs of multiple tasks"""

def filter_file_extension(filepath: tuple[str, str]) -> dict[str, str]:
    if filepath[0].rsplit('.', 1)[1] == 'zip':
        raise AirflowSkipException(f"Skipping file {filepath[0]} as it has zip extension")
    return {
        'file': filepath[0],
        'path': filepath[1]
    }

@dag
def concatenation_tasks_output():

    @task
    def extract_files_from_source_a() -> list[str]:
        return ['a.txt', 'b.csv', 'c.zip']
    
    @task
    def extract_files_from_source_b() -> list[str]:
        return ['e.txt', 'f.csv', 'g.zip']

    @task
    def extract_files_from_source_c() -> list[str]:
        return ['i.txt', 'j.parquet']

    @task
    def extract_paths() -> list[str]:
        return ['/var', '/var/log', '/home', '/tmp', '/var1', '/var/log1', '/home1', '/tmp1']

    @task
    def add_path(file: str, path: str) -> str:
        return f"{path}/{file}"

    @task(trigger_rule='none_failed_min_one_success')
    def print_valid_files(files: list[str]) -> None:
        for file in files:
            print(file)

    files = extract_files_from_source_a().concat(extract_files_from_source_b()).concat(extract_files_from_source_c())
    filtered_files = files.zip(extract_paths()).map(filter_file_extension)
    valid_files =  add_path.expand_kwargs(filtered_files)
    print_valid_files(valid_files)

concatenation_tasks_output()