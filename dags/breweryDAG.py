from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'retries': 0
}

dag=DAG(
    dag_id='breweryDAG',
    default_args=default_args,
    start_date=datetime(2024,4,28),
    catchup=False,
    schedule_interval='0 0 * * *', #Run once a day at midnight
    )
    
t1 = BashOperator(
    task_id = 'Bash_task',
    bash_command = 'python $AIRFLOW_HOME/dags/scripts/openbrewETL.py',
    dag = dag
    )
    
t1
