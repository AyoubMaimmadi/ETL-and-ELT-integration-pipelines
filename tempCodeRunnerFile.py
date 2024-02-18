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