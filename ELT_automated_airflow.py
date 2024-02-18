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
    hdfs_path = '/sales_data'
    local_path = "C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\sales_csv"
    if subprocess.run(f"hadoop fs -test -d {hdfs_path}", shell=True).returncode != 0:
        load_command = f"hadoop fs -copyFromLocal {local_path} {hdfs_path}"
        run_hadoop_command(load_command)
    else:
        print(f"{hdfs_path} already exists in HDFS. Skipping copy.")

def step_2():
    hdfs_path = '/python'
    local_file = "C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\mapper.py"
    if subprocess.run(f"hadoop fs -test -e {hdfs_path}", shell=True).returncode != 0:
        load_mapper = f"hadoop fs -put {local_file} {hdfs_path}"
        run_hadoop_command(load_mapper)
    else:
        print(f"{hdfs_path} already exists in HDFS. Skipping copy.")

def step_3():
    streaming_command = 'hadoop jar C:\\Users\\LENOVO\\hadoop-3.3.0\\hadoop-3.3.0\\share\\hadoop\\tools\\lib\\hadoop-streaming-3.3.0.jar -files file:/C:/Users/LENOVO/ETL-and-ELT-integration-pipelines/mapper.py -mapper "python mapper.py" -input /sales_data/* -output /output-sales-data'
    run_hadoop_command(streaming_command)

def run_elt_automated():
    script_path = "C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\ELT_automated.py"
    try:
        subprocess.run(f"python {script_path}", check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while running ELT_automated.py: {e}")
    except FileNotFoundError as e:
        print(f"Error: {e}. Make sure the script path is correct.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'HADOOP_ELT_pipeline',
    default_args=default_args,
    description='Automated ELT pipeline using Hadoop',
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

task_4 = PythonOperator(
    task_id='run_elt_automated_script',
    python_callable=run_elt_automated,
    dag=dag,
)

task_1 >> task_2 >> task_3 >> task_4
