from airflow.sdk import dag, task
import pendulum

@dag(
    dag_id="my_decorated_dag",
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    schedule= "*/10 * * * *",  # Set to None for manual trigger, or use a cron string like "0 0 * * *"
    catchup=False,
    tags=["example", "decorators"],
)
def my_sample_dag():
    """
    This DAG demonstrates the use of Airflow decorators for defining tasks and DAGs.
    """

    @task
    def extract_data(source: str) -> list[int]:
        """
        Simulates extracting data from a source.
        """
        print(f"Extracting data from {source}...")
        return [1, 2, 3, 4, 5]

    @task
    def transform_data(data: list[int]) -> list[int]:
        """
        Simulates transforming the extracted data.
        """
        print(f"Transforming data: {data}...")
        return [x * 2 for x in data]

    @task
    def load_data(transformed_data: list[int]):
        """
        Simulates loading the transformed data.
        """
        print(f"Loading data: {transformed_data}...")
        print("Data loaded successfully!")

    # Define the task dependencies using the functional API
    extracted_values = extract_data(source="my_database")
    transformed_values = transform_data(data=extracted_values)
    load_data(transformed_data=transformed_values)

my_sample_dag()