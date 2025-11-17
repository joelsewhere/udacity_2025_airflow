from airflow.decorators import dag, task

@task
def pull_data(
    data_interval_start,
    data_interval_end,
    ):
  pass

@task
def transform_data():
  pass

@task
def load_data():
  pass

@dag(schedule=None)
def single_responsibility():
  load_data(transform_data(pull_data()))

single_responsibility()
  
