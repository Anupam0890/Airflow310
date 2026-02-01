from airflow.sdk import dag, task

@dag
def map_and_reduce():

    @task
    def extra_value() -> list[int]:
        from random import randint
        return [i+1 for i in range(randint(1, 8))]

    
    extra_value()

map_and_reduce()