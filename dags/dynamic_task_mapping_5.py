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
    
    @task
    def sum_values(values: list[int]) -> int:
        print(sum(values))
    
    numbers = add_values.partial(constant_val=10).expand(value=extra_value())
    sum_values(numbers)

map_and_reduce()