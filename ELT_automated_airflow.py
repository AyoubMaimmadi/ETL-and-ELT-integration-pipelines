from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

def run_hadoop_command(command):
    try:
        subprocess.run(command, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")
    except FileNotFoundError as e:
        print(f"Error: {e}. Make sure Hadoop commands are installed and configured properly.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def step_1():
    load_command = "hadoop fs -copyFromLocal C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\sales_csv /sales_data"
    run_hadoop_command(load_command)

def step_2():
    load_mapper = "hadoop fs -put C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\mapper.py /python"
    run_hadoop_command(load_mapper)

def step_3():
    streaming_command = "hadoop jar C:\\Users\\LENOVO\\hadoop-3.3.0\\hadoop-3.3.0\\share\\hadoop\\tools\\lib\\hadoop-streaming-3.3.0.jar -files file:/C:/Users/LENOVO/ETL-and-ELT-integration-pipelines/mapper.py -mapper \"python mapper.py\" -input /sales_data/* -output /output-sales-data"
    run_hadoop_command(streaming_command)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hadoop_etl_pipeline',
    default_args=default_args,
    description='Automated ETL pipeline using Hadoop',
    schedule_interval=timedelta(days=1),
)

task_1 = PythonOperator(
    task_id='step_1_load_data',
    python_callable=step_1,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='step_2_copy_mapper',
    python_callable=step_2,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='step_3_run_hadoop_job',
    python_callable=step_3,
    dag=dag,
)

task_1 >> task_2 >> task_3
