if __name__ == "__main__":
    csv_directory = 'sales_xlsx'
    table_name = 'sales_records'
    process_files(csv_directory, table_name)
    logging.info("ETL Process Completed Successfully")