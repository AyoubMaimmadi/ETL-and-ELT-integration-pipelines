import subprocess
import os
import time
import logging
import json

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File to store progress
progress_file = 'progress.json'

def load_progress():
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as file:
            return json.load(file)
    else:
        return {}
def save_progress(step, status):
    progress = load_progress()
    progress[step] = status
    with open(progress_file, 'w') as file:
        json.dump(progress, file)

def run_hadoop_command(command, retries=3, delay=5):
    logging.info(f"Running command: {command}")
    for attempt in range(1, retries + 1):
        try:
            subprocess.run(command, check=True, shell=True)
            logging.info("Command executed successfully.")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Attempt {attempt} failed: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)

    logging.error(f"All attempts failed for command: {command}")
    return False

def step_1(hdfs_path, local_path):
    logging.info("Starting Step 1: Load data to HDFS")
    if not os.path.exists(local_path) or not os.listdir(local_path):
        logging.warning(f"Local directory {local_path} does not exist or is empty. Skipping step 1.")
        return False

    if subprocess.run(f"hadoop fs -test -d {hdfs_path}", shell=True).returncode != 0:
        load_command = f"hadoop fs -copyFromLocal {local_path} {hdfs_path}"
        success = run_hadoop_command(load_command)
    else:
        logging.info(f"{hdfs_path} already exists in HDFS. Skipping copy.")
        success = True
    save_progress('step_1', 'completed' if success else 'failed')
    logging.info("Step 1 completed." if success else "Step 1 failed.")
    return success
    

def step_2(hdfs_path, local_file):
    logging.info("Starting Step 2: Load mapper to HDFS")
    if not os.path.exists(local_file):
        logging.warning(f"Local file {local_file} does not exist. Skipping step 2.")
        return False

    if subprocess.run(f"hadoop fs -test -e {hdfs_path}", shell=True).returncode != 0:
        load_mapper = f"hadoop fs -put {local_file} {hdfs_path}"
        success = run_hadoop_command(load_mapper)
    else:
        logging.info(f"{hdfs_path} already exists in HDFS. Skipping copy.")
        success = True
    save_progress('step_2', 'completed' if success else 'failed')
    logging.info("Step 2 completed." if success else "Step 2 failed.")
    return success

def step_3(streaming_command):
    logging.info("Starting Step 3: Execute Hadoop Streaming")
    success = run_hadoop_command(streaming_command)
    save_progress('step_3', 'completed' if success else 'failed')
    logging.info("Step 3 completed." if success else "Step 3 failed.")
    return success

def main():
    logging.info("Starting ELT Process")
    progress = load_progress()

    # Define paths and commands
    hdfs_path_data = '/sales_data'
    local_path_data = "C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\sales_csv"
    hdfs_path_mapper = '/python'
    local_file_mapper = "C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\mapper.py"
    streaming_command = 'hadoop jar C:\\Users\\LENOVO\\hadoop-3.3.0\\hadoop-3.3.0\\share\\hadoop\\tools\\lib\\hadoop-streaming-3.3.0.jar -files file:/C:/Users/LENOVO/ETL-and-ELT-integration-pipelines/mapper.py -mapper "python mapper.py" -input /sales_data/* -output /output-sales-data'

    # Execute steps based on progress
    if progress.get('step_1') != 'completed':
        step_1(hdfs_path_data, local_path_data)

    if progress.get('step_2') != 'completed':
        step_2(hdfs_path_mapper, local_file_mapper)

    if progress.get('step_3') != 'completed':
        step_3(streaming_command)

    logging.info("ELT Process completed")

if __name__ == "__main__":
    main()