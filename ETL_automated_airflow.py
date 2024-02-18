from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import glob
import pandas as pd
import psycopg2
from psycopg2 import OperationalError, Error
import logging
import time

# Transformation functions
def transform_date(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError as e:
        logging.warning(f"Date transformation error: {e}")
        return date_str

def transform_money(money_value):
    try:
        return "{:.2f}".format(float(money_value))
    except ValueError as e:
        logging.warning(f"Money transformation error: {e}")
        return money_value

# Database interaction functions with retry mechanism
def create_db_connection(attempts=3, delay=5):
    conn = None
    while attempts > 0:
        try:
            conn = psycopg2.connect(dbname=db_name, user=username, password=password)
            break
        except OperationalError as e:
            logging.error(f"Database connection error: {e}. Retrying in {delay} seconds.")
            time.sleep(delay)
            attempts -= 1
            delay *= 2  # Exponential backoff
    return conn

def record_exists(cursor, order_id, table_name):
    try:
        cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE \"Order ID\" = %s)", (order_id,))
        return cursor.fetchone()[0]
    except Error as e:
        logging.error(f"Error checking record existence: {e}")
        return False

def insert_dataframe_to_db(df, table_name, conn):
    cursor = conn.cursor()
    records_to_insert = []

    for _, row in df.iterrows():
        if not record_exists(cursor, row['Order ID'], table_name):
            records_to_insert.append(tuple(row))

    if records_to_insert:
        columns = ', '.join([f'"{column}"' for column in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'

        try:
            cursor.executemany(insert_query, records_to_insert)
            conn.commit()
            logging.info(f"{len(records_to_insert)} records inserted successfully into {table_name}.")
        except Error as e:
            logging.error(f"Error inserting records: {e}")
            conn.rollback()

    cursor.close()

def process_file(file_path, table_name, conn):
    processed_flag = file_path + ".processed"
    processing_flag = file_path + ".processing"

    if not os.path.isfile(processed_flag):
        if not os.path.isfile(processing_flag):
            # File has not been processed or is not currently being processed
            try:
                # Step 2: Attempt to read the file
                df = pd.read_excel(file_path, index_col=0)

                # Step 3: Apply transformations
                date_columns = ['Order Date', 'Ship Date']
                money_columns = ['Unit Price', 'Unit Cost', 'Total Revenue', 'Total Cost', 'Total Profit']
                df[date_columns] = df[date_columns].applymap(transform_date)
                df[money_columns] = df[money_columns].applymap(transform_money)

                # Mark file as processing
                with open(processing_flag, 'w') as f:
                    f.write("Processing")

                # Step 4: Insert data into the database
                insert_dataframe_to_db(df, table_name, conn)

                # Step 5: Rename .processing to .processed upon successful processing
                os.rename(processing_flag, processed_flag)
                logging.info(f"Data from {file_path} processed and inserted successfully.")

            except Exception as e:
                logging.error(f"Error processing file {file_path}: {e}")
                if os.path.isfile(processing_flag):
                    os.remove(processing_flag)
        else:
            # File was partially processed in a previous run, re-attempt processing
            logging.warning(f"File {file_path} was partially processed in a previous run. Re-attempting.")
            os.remove(processing_flag)  # Remove processing flag and re-attempt
            process_file(file_path, table_name, conn)  # Recursive call to reprocess
    else:
        logging.info(f"File {file_path} has already been processed.")



def process_files(directory, table_name):
    conn = create_db_connection()
    if conn is None:
        logging.error("Failed to establish database connection. Exiting.")
        return

    try:
        if not os.path.exists(directory):
            logging.error(f"The directory {directory} does not exist. Exiting.")
            return

        excel_files = glob.glob(os.path.join(directory, '*.xlsx'))

        if not excel_files:
            logging.warning(f"No .xlsx files found in the directory: {directory}. Exiting.")
            return

        for file_path in excel_files:
            processed_flag = file_path + ".processed"
            processing_flag = file_path + ".processing"

            if os.path.isfile(processing_flag):
                logging.warning(f"File {file_path} was partially processed in a previous run. Re-attempting.")
                os.remove(processing_flag)

            if not os.path.isfile(processed_flag):
                # Mark file as processing
                with open(processing_flag, 'w') as f:
                    f.write("Processing")
                process_file(file_path, table_name, conn)
            else:
                logging.info(f"File {file_path} has already been processed.")

    except Exception as e:
        logging.error(f"An error occurred during file processing: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 16),  # Adjust to your desired start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_pipeline',
    default_args=default_args,
    description='ETL task to process sales data and load into database',
    schedule_interval=timedelta(days=1),  # Adjust to your desired interval
)

def etl_task():
    csv_directory = 'sales_xlsx'  # Ensure this path is accessible by Airflow
    table_name = 'sales_records'
    process_files(csv_directory, table_name)

run_etl = PythonOperator(
    task_id='run_etl_process',
    python_callable=etl_task,
    dag=dag,
)

run_etl
