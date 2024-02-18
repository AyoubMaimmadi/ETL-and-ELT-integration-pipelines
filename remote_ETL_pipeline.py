import requests
import pandas as pd
from io import BytesIO
import psycopg2
from psycopg2 import OperationalError, Error  
from datetime import datetime


# Define the URL of the Excel files on GitHub
base_url = "https://raw.githubusercontent.com/anbento0490/tutorials/master/sales_xlsx/"
file_names = [f"sales_records_n{i}.xlsx" for i in range(1, 6)]

# Database credentials
db_name = "remote-sales-database"
username = "postgres"
password = "Abdi2022"

# Function to transform date to ISO format (YYYY-MM-DD)
def transform_date(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        # If there's an error, return the original string
        return date_str

# Function to standardize money values to two decimal places
def transform_money(money_value):
    try:
        return "{:.2f}".format(float(money_value))
    except ValueError:
        # If there's an error, return the original value
        return money_value

# Function to insert DataFrame into the database
def insert_dataframe_to_db(df, table_name, conn):
    cursor = conn.cursor()
    try:
        # Apply transformations to date and money columns
        date_columns = ['Order Date', 'Ship Date']
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(transform_date)

        money_columns = ['Unit Price', 'Unit Cost', 'Total Revenue', 'Total Cost', 'Total Profit']
        for col in money_columns:
            if col in df.columns:
                df[col] = df[col].apply(transform_money)

        # Convert DataFrame to list of tuples
        records = df.to_dict(orient='records')
        columns = df.columns.tolist()

        # Generate the INSERT INTO SQL query
        placeholders = ', '.join(['%s'] * len(columns))
        columns = ', '.join([f'"{column}"' for column in columns])
        insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'

        cursor = conn.cursor()

        for record in records:
            try:
                cursor.execute(insert_query, list(record.values()))
            except Error as e:
                print(f"Error inserting record: {e}")
                conn.rollback()
            else:
                conn.commit()
    except Exception as e:
        print(f"Error inserting data into the database: {e}")
        conn.rollback()
    finally:
        cursor.close()

# Function to process and insert data into the database
def process_and_insert_data(file_name, table_name, conn):
    try:
        url = base_url + file_name
        response = requests.get(url)
        if response.status_code == 200:
            df = pd.read_excel(BytesIO(response.content))
            # Insert the DataFrame into the database
            insert_dataframe_to_db(df, table_name, conn)
            print(f"Data from {file_name} inserted successfully into {table_name}.")
        else:
            print(f"Failed to download {file_name}. Status code: {response.status_code}")
    except Exception as e:
        print(f"An error occurred processing {file_name}: {e}")

# Process each file and insert its data into the database
def process_files_and_insert_into_db(table_name):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(dbname=db_name, user=username, password=password)
        for file_name in file_names:
            process_and_insert_data(file_name, table_name, conn)
    except OperationalError as e:
        print(f"An operational error occurred: {e}")
    finally:
        # Close communication with the database
        if 'conn' in locals() and conn is not None:
            conn.close()

# Define the PostgreSQL table name
table_name = 'sales_records'

# Process the Excel files from the GitHub repository and insert into the database
process_files_and_insert_into_db(table_name)
