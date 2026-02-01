from airflow.sdk import dag, task

@dag
def map_and_reduce():

    @task
    def extra_value() -> list[int]:
        from random import randint
        return [i+1 for i in range(randint(1, 8))]

    @task
    def add_values(value: int, constant_val: int):
        result = value + constant_val
        return result
    
    add_values.partial(constant_val=10).expand(value=extra_value())

map_and_reduce()