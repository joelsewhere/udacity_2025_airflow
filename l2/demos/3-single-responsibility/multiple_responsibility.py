from airflow.decorators import dag, task

@task
def pipeline():
  
  # Pull data

  # Transform data

  # Load data
  pass

@dag(schedule=None)
def multiple_responsibility():

  pipeline()

multiple_responsibility()

  
