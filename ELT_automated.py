import subprocess
import os

def run_hadoop_command(command):
    try:
        subprocess.run(command, check=True, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")

def step_1():
    hdfs_path = '/sales_data'
    local_path = "C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\sales_csv"
    # Check if directory exists in HDFS
    if subprocess.run(f"hadoop fs -test -d {hdfs_path}", shell=True).returncode != 0:
        load_command = f"hadoop fs -copyFromLocal {local_path} {hdfs_path}"
        run_hadoop_command(load_command)
    else:
        print(f"{hdfs_path} already exists in HDFS. Skipping copy.")

def step_2():
    hdfs_path = '/python'
    local_file = "C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\mapper.py"
    # Check if file exists in HDFS
    if subprocess.run(f"hadoop fs -test -e {hdfs_path}", shell=True).returncode != 0:
        load_mapper = f"hadoop fs -put {local_file} {hdfs_path}"
        run_hadoop_command(load_mapper)
    else:
        print(f"{hdfs_path} already exists in HDFS. Skipping copy.")

def step_3():
    streaming_command = 'hadoop jar C:\\Users\\LENOVO\\hadoop-3.3.0\\hadoop-3.3.0\\share\\hadoop\\tools\\lib\\hadoop-streaming-3.3.0.jar -files file:/C:/Users/LENOVO/ETL-and-ELT-integration-pipelines/mapper.py -mapper "python mapper.py" -input /sales_data/* -output /output-sales-data'
    run_hadoop_command(streaming_command)

step_1()
step_2()
step_3()
