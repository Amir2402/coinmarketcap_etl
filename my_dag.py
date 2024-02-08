from airflow import DAG 
from datetime import datetime , timedelta
from airflow.operators.python import PythonOperator
import sys  
from datetime import timedelta 

sys.path.append('/home/amir/firstproj')

from load import load_data
from Transform import transform_data

default_args={
    'owner' : 'Amir' , 
    'retries' : 1 ,
    'retry_delay' : timedelta(minutes=2)  
}

with DAG(
    dag_id='coins_etl' , 
    default_args = default_args ,
    start_date = datetime(2023, 12 , 18) ,
    schedule_interval = timedelta(days=2) ,
    catchup = False
) as dag : 

    extract_transform = PythonOperator(
        task_id = 'transform' , 
        python_callable= transform_data , 
        dag = dag
    )

    load = PythonOperator(
        task_id = 'load' , 
        python_callable = load_data , 
        dag = dag 
    )

    extract_transform >> load 
