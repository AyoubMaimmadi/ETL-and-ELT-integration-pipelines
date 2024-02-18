import psycopg2
from psycopg2 import OperationalError, Error
import logging

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database credentials (Consider using environment variables for security)
db_name = "sales-database"
username = "postgres"
password = "Abdi2022"

def create_table(conn):
    try:
        with conn.cursor() as cur:
            # Create table schema
            cur.execute('''
                CREATE TABLE IF NOT EXISTS sales_records (
                    "Region" TEXT,
                    "Country" TEXT,
                    "Item Type" TEXT,
                    "Sales Channel" TEXT,
                    "Order Priority" TEXT,
                    "Order Date" DATE,
                    "Order ID" BIGINT PRIMARY KEY,
                    "Ship Date" DATE,
                    "Units Sold" INTEGER,
                    "Unit Price" NUMERIC,
                    "Unit Cost" NUMERIC,
                    "Total Revenue" NUMERIC,
                    "Total Cost" NUMERIC,
                    "Total Profit" NUMERIC
                );
            ''')
            conn.commit()
            logging.info("Table created successfully")
    except Error as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()

def connect_to_database(db_name, username, password):
    try:
        return psycopg2.connect(dbname=db_name, user=username, password=password)
    except OperationalError as e:
        logging.error(f"Database connection error: {e}")
        return None

def main():
    conn = connect_to_database(db_name, username, password)
    if conn is None:
        logging.error("Failed to connect to the database. Exiting.")
        return

    create_table(conn)

    if conn:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
