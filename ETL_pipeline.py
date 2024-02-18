import os
import glob
import pandas as pd
import psycopg2
from psycopg2 import OperationalError, Error
from datetime import datetime

# Database credentials
db_name = "sales-database"
username = "postgres"
password = "Abdi2022"

# Function to transform date to ISO format (YYYY-MM-DD)
def transform_date(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return date_str

# Function to standardize money values to two decimal places
def transform_money(money_value):
    try:
        return "{:.2f}".format(float(money_value))
    except ValueError:
        return money_value

# Function to check if the record already exists in the database
def record_exists(cursor, order_id, table_name):
    cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE \"Order ID\" = %s)", (order_id,))
    return cursor.fetchone()[0]

# Function to insert DataFrame into the database
def insert_dataframe_to_db(df, table_name, conn):
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        if not record_exists(cursor, row['Order ID'], table_name):
            columns = ', '.join([f'"{column}"' for column in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
            try:
                cursor.execute(insert_query, tuple(row))
                conn.commit()
            except Error as e:
                print(f"Error inserting record: {e}")
                conn.rollback()
        else:
            print(f"Record with Order ID {row['Order ID']} already exists. Skipping.")

    cursor.close()

# Function to process each Excel file and load into the database
def process_files(directory, table_name):
    try:
        conn = psycopg2.connect(dbname=db_name, user=username, password=password)

        # Get all Excel files in the directory
        excel_files = glob.glob(os.path.join(directory, '*.xlsx'))
        
        for file_path in excel_files:
            if not os.path.isfile(file_path + ".processed"):
                try:
                    df = pd.read_excel(file_path, index_col=0)

                    # Apply transformations
                    date_columns = ['Order Date', 'Ship Date']
                    money_columns = ['Unit Price', 'Unit Cost', 'Total Revenue', 'Total Cost', 'Total Profit']

                    df[date_columns] = df[date_columns].applymap(transform_date)
                    df[money_columns] = df[money_columns].applymap(transform_money)

                    insert_dataframe_to_db(df, table_name, conn)
                    print(f"Data from {file_path} inserted successfully into {table_name}.")

                    # Mark file as processed
                    with open(file_path + ".processed", 'w') as f:
                        f.write("Processed")
                except Exception as e:
                    print(f"An error occurred processing file {file_path}: {e}")
            else:
                print(f"File {file_path} has already been processed.")
    
    except OperationalError as e:
        print(f"An operational error occurred: {e}")
    except Error as e:
        print(f"A database error occurred: {e}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()

csv_directory = 'sales_xlsx'
table_name = 'sales_records'

process_files(csv_directory, table_name)
print("This Extraction Process Has Successfully Finished")
