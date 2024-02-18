#!/usr/bin/env python
import sys
import pandas as pd

# Function to convert date format
def convert_date_format(date_str):
    return pd.to_datetime(date_str).strftime('%Y-%m-%d')

# Function to format money values
def format_money(value):
    return '{:.2f}'.format(float(value))

# Process each line in the input
for line in sys.stdin:
    # Strip and split the line into columns
    line = line.strip()
    columns = line.split(',')

    # Check if the line is not the header and has the correct number of columns
    if len(columns) == 14 and 'Order Date' not in line:
        # Transform dates
        columns[5] = convert_date_format(columns[5])  # Order Date
        columns[7] = convert_date_format(columns[7])  # Ship Date

        # Format money fields
        for i in [9, 10, 11, 12, 13]:
            columns[i] = format_money(columns[i])

    # Output the transformed line
    print(','.join(columns))


