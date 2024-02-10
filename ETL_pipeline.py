
import os
import glob
import pandas as pd
import psycopg2
from psycopg2 import OperationalError, Error

# Database credentials
db_name = "sales-database"
username = "postgres"
password = "lina2015"

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

# Function to process each CSV file and load into the database
def process_files(directory, table_name):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(dbname=db_name, user=username, password=password)

        # Get all CSV files in the directory
        csv_files = glob.glob(os.path.join(directory, '*.csv'))
        
        for file_path in csv_files:
            try:
                # Read the CSV file into a DataFrame, skipping the first column
                df = pd.read_csv(file_path, index_col=0)
                
                # Insert DataFrame into the database
                insert_dataframe_to_db(df, table_name, conn)
                print(f"Data from {file_path} inserted successfully into {table_name}.")
            except pd.errors.EmptyDataError as e:
                print(f"No data in file {file_path}: {e}")
            except pd.errors.ParserError as e:
                print(f"Error parsing file {file_path}: {e}")
            except Exception as e:
                print(f"An error occurred processing file {file_path}: {e}")
    
    except OperationalError as e:
        print(f"An operational error occurred: {e}")
    except Error as e:
        print(f"A database error occurred: {e}")
    finally:
        # Close communication with the database
        if 'conn' in locals() and conn is not None:
            conn.close()
        # Close communication with the database
        if 'conn' in locals() and conn is not None:
            conn.close()

# Define the directory where the CSV files are stored
csv_directory = 'sales_csv'
# Define the PostgreSQL table name
table_name = 'sales_records'

# Process the CSV files and load into the database
process_files(csv_directory, table_name)
