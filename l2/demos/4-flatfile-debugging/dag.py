from airflow.decorators import dag, task
from airflow.hooks.filesystem import FSHook

@dag(schedule=monthly)
def flatfiles():

  @task
  def collect_data(filename):

    # query from different tables for each month
    data = pd.DataFrame()

    hook = FSHook()

    flatfile_storage = hook.get_path() 
    filepath = flatfile_storage + filename

    file = open(filepath, 'w')
    file.write(data.to_csv(index=False))
    file.close()

  filename = '{{ ds }}_records.csv'
  collect_data(filename)

flatfile()
