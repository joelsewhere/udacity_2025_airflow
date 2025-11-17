from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

def python_task(ds):

  print(ds)

catchup_true = DAG(
  dag_id="catchup_true",
  schedule="@monthly",
  start_date=pendulum.datetime(2025, 1, 1, tz="America/Chicago")
  ) # Catchup defaults to True!

PythonOperator(
  task_id="catchup_true_task",
  dag=catchup_true,
  python_callable=python_task
  )


catchup_false = DAG(
  dag_id="catchup_false",
  schedule="@monthly",
  start_date=pendulum.datetime(2025, 1, 1, tz="America/Chicago")
  catchup=False,
  )

PythonOperator(
  task_id="catchup_false_task",
  dag=catchup_false,
  python_callable=python_task
  )
