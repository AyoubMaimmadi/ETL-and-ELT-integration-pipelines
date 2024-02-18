import os
import subprocess

def file_exists_in_hdfs(hdfs_path, file_name):
    # Check if a file exists in HDFS
    cmd = f"hadoop fs -test -e {os.path.join(hdfs_path, file_name)}"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.returncode == 0

def upload_to_hdfs(local_path, hdfs_path):
    files = [f for f in os.listdir(local_path) if f.endswith('.csv')]
    for file in files:
        if not file_exists_in_hdfs(hdfs_path, file):
            local_file = os.path.join(local_path, file)
            hdfs_file = os.path.join(hdfs_path, file)
            command = f"hadoop fs -copyFromLocal \"{local_file}\" \"{hdfs_file}\""
            subprocess.run(command, check=True, shell=True)
        else:
            print(f"Skipping {file} as it already exists in HDFS.")

local_csv_path = 'C:\\Users\\LENOVO\\ETL-and-ELT-integration-pipelines\\sales_csv'
hdfs_directory = '/sales_data'

upload_to_hdfs(local_csv_path, hdfs_directory)
print("This process has finished successfully")

