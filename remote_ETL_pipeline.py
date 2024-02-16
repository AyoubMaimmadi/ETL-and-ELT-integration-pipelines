import pandas as pd
import psycopg2
from psycopg2 import OperationalError, Error
from datetime import datetime
import requests 
from io import BytesIO

# Database credentials
db_name = "sales-database"
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
    
    cursor.close()

# Function to process Excel files from the GitHub repository and load into the database
def process_files_from_github(table_name):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(dbname=db_name, user=username, password=password)

        # Define the GitHub repository URL
        github_repo_url = 'https://github.com/anbento0490/tutorials/raw/master/sales_xlsx/'

        # Fetch the list of Excel files from the GitHub repository
        response = requests.get(github_repo_url)
        if response.status_code == 200:
            excel_files = response.text.splitlines()
        else:
            print(f"Failed to fetch file list from GitHub repository. Status code: {response.status_code}")
            return

        for file_name in excel_files:
            try:
                # Fetch the Excel file content
                file_url = f'{github_repo_url}{file_name}'
                excel_content = requests.get(file_url).content

                # Read the Excel file content into a DataFrame, skipping the first column
                df = pd.read_excel(BytesIO(excel_content), engine='openpyxl', index_col=0)

                # Apply transformations
                date_columns = ['Order Date', 'Ship Date']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = df[col].apply(transform_date)

                money_columns = ['Unit Price', 'Unit Cost', 'Total Revenue', 'Total Cost', 'Total Profit']
                for col in money_columns:
                    if col in df.columns:
                        df[col] = df[col].apply(transform_money)

                # Insert DataFrame into the database
                insert_dataframe_to_db(df, table_name, conn)
                print(f"Data from {file_name} inserted successfully into {table_name}.")
            except pd.errors.EmptyDataError as e:
                print(f"No data in file {file_name}: {e}")
            except pd.errors.ParserError as e:
                print(f"Error parsing file {file_name}: {e}")
            except Exception as e:
                print(f"An error occurred processing file {file_name}: {e}")
    
    except OperationalError as e:
        print(f"An operational error occurred: {e}")
    except Error as e:
        print(f"A database error occurred: {e}")
    finally:
        # Close communication with the database
        if 'conn' in locals() and conn is not None:
            conn.close()

# Define the PostgreSQL table name
table_name = 'sales_records'

# Process the Excel files from the GitHub repository and load into the database
process_files_from_github(table_name)
