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

# Step 1: Load data into HDFS (example command)
load_command = "hadoop fs -copyFromLocal C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\sales_csv /sales_data"
run_hadoop_command(load_command)

# Step 2: Copying Mapper.py into HDFS
load_mapper = "hadoop fs -put C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\mapper.py /python"
run_hadoop_command(load_mapper)

# Step 3: Run Hadoop Streaming job for transformation
streaming_command = "hadoop jar C:\\Users\\LENOVO\\hadoop-3.3.0\\hadoop-3.3.0\\share\\hadoop\\tools\\lib\\hadoop-streaming-3.3.0.jar -files file:/C:/Users/LENOVO/ETL-and-ELT-integration-pipelines/mapper.py -mapper \"python mapper.py\" -input /sales_data/* -output /output-sales-data"
run_hadoop_command(streaming_command)

