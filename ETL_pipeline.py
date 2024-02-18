import os
import glob
import pandas as pd
import psycopg2
from psycopg2 import OperationalError, Error
from datetime import datetime
import logging
import time
import json

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database credentials
db_name = "sales-database"
username = "postgres"
password = "Abdi2022"

# Progress file
progress_file = 'progress_etl.json'

def load_progress():
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as file:
            return json.load(file)
    else:
        return {}

def save_progress(file_path, status):
    progress = load_progress()
    progress[file_path] = status
    with open(progress_file, 'w') as file:
        json.dump(progress, file)

# Transformation functions
def transform_date(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError as e:
        logging.warning(f"Date transformation error: {e}")
        return date_str

def transform_money(money_value):
    if isinstance(money_value, float):
        return "{:.2f}".format(money_value)
    try:
        return "{:.2f}".format(float(money_value.replace('$', '').replace(',', '')))
    except ValueError as e:
        logging.warning(f"Money transformation error: {e}")
        return money_value


# Database interaction functions
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

    if os.path.isfile(processed_flag):
        logging.info(f"File {file_path} has already been processed.")
        return

    try:
        if os.path.isfile(processing_flag):
            logging.warning(f"File {file_path} was partially processed in a previous run. Re-attempting.")
            os.remove(processing_flag)
        
        with open(processing_flag, 'w') as f:
            f.write("Processing")

        df = pd.read_excel(file_path, index_col=0)
        date_columns = ['Order Date', 'Ship Date']
        money_columns = ['Unit Price', 'Unit Cost', 'Total Revenue', 'Total Cost', 'Total Profit']

        # Apply transformations using map for each column
        for col in date_columns:
            df[col] = df[col].map(transform_date)

        for col in money_columns:
            df[col] = df[col].map(transform_money)

        insert_dataframe_to_db(df, table_name, conn)

        os.rename(processing_flag, processed_flag)
        save_progress(file_path, 'completed')

    except pd.errors.EmptyDataError as e:
        logging.error(f"Empty data in file {file_path}: {e}")
        save_progress(file_path, 'failed')
        if os.path.isfile(processing_flag):
            os.remove(processing_flag)

    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        save_progress(file_path, 'failed')
        if os.path.isfile(processing_flag):
            os.remove(processing_flag)

def process_files(directory, table_name):
    conn = create_db_connection()
    if conn is None:
        logging.error("Failed to establish database connection. Exiting.")
        return

    progress = load_progress()

    try:
        if not os.path.exists(directory):
            logging.error(f"The directory {directory} does not exist. Exiting.")
            return

        excel_files = glob.glob(os.path.join(directory, '*.xlsx'))

        if not excel_files:
            logging.warning(f"No .xlsx files found in the directory: {directory}. Exiting.")
            return

        for file_path in excel_files:
            if progress.get(file_path) != 'completed':
                process_file(file_path, table_name, conn)

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    csv_directory = 'sales_xlsx'
    table_name = 'sales_records'
    process_files(csv_directory, table_name)
    logging.info("ETL Process Completed Successfully")
