from airflow.sdk import dag, task, get_current_context, Param
from datetime import datetime


@dag(
    start_date=datetime(2026, 1, 10),
    schedule=None,
    catchup=False,
    params={
          "extra_val": Param(
            10,
            type="integer",
            minimum=1,
            maximum=100
          ),
         "extra_date": Param(
           "2026-01-10T15:52:00",
           type="string",
           format="date-time"
         )
    }
)
def passing_param_to_dag_ex1():

    @task(multiple_outputs=True)
    def extract_data() -> dict[str, int]:
        return {'input_a': 100, 'input_b': 200}
    
    @task
    def transform_a(input_a: int, **context) -> int:
        print(context['params']['extra_val'])  # Do not pass the entire context in production code to get value of params only 
        return input_a + 10

    @task
    def transform_b(input_b: int, params=None) -> int:
        print(params['extra_val'])  # Just pass the context variable params
        return input_b + 20
    
    @task
    def load_data():
        context = get_current_context()
        print(context['params']['extra_val'])  # Get context inside the task

    values = extract_data()

    #transform_b(values['input_b'])
    [transform_a(values['input_a']), transform_b(values['input_b'])] >> load_data()


passing_param_to_dag_ex1()
