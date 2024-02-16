import psycopg2
from psycopg2 import OperationalError, Error

# Database credentials
db_name = "sales-database"
username = "postgres"
password = "Abdi2022"

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(dbname=db_name, user=username, password=password)
    cur = conn.cursor()

    # Create table schema
    # The following SQL command creates a table with various columns.
    cur.execute('''
            CREATE TABLE IF NOT EXISTS sales_records (
                "Region" TEXT,
                "Country" TEXT,
                "Item Type" TEXT,
                "Sales Channel" TEXT,
                "Order Priority" TEXT,
                "Order Date" TEXT,
                "Order ID" BIGINT,
                "Ship Date" TEXT,
                "Units Sold" BIGINT,
                "Unit Price" DOUBLE PRECISION,
                "Unit Cost" DOUBLE PRECISION,
                "Total Revenue" DOUBLE PRECISION,
                "Total Cost" DOUBLE PRECISION,
                "Total Profit" DOUBLE PRECISION
            );
    ''')

    # Commit changes
    conn.commit()
    print("Table created successfully")

except OperationalError as e:
    # Exception block for handling operational errors (e.g., connection issues).
    print(f"An operational error occurred: {e}")
except Error as e:
    # General exception block for handling other database errors.
    print(f"A database error occurred: {e}")
finally:
    # Ensuring that the cursor and connection are closed properly.
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
