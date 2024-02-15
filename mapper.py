#!/usr/bin/env python3
import sys

print("Mapper started", file=sys.stderr)

# Function to convert date format from MM/DD/YYYY to YYYY-MM-DD
def transform_date(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        # Return the original string if the transformation fails
        return date_str

# Function to format money values to ensure two decimal places
def transform_money(value):
    try:
        # Ensure two decimal places for monetary values
        return "{:.2f}".format(float(value))
    except ValueError:
        # Return the original value if the transformation fails
        return value

# Attempt to skip the header line if it exists
try:
    next(sys.stdin)
    print("Header skipped", file=sys.stderr)
except StopIteration:
    # Exit the script if there is no input to process
    print("No input provided", file=sys.stderr)
    sys.exit()

for line in sys.stdin:
    line = line.strip()
    parts = line.split(',')  # Assuming CSV format. Change to '\t' if your input is tab-delimited
    
    # Ensure we have enough parts to avoid IndexError
    if len(parts) >= 14:
        parts[5] = transform_date(parts[5])  # Order Date column
        parts[7] = transform_date(parts[7])  # Ship Date column
        parts[9] = transform_money(parts[9])  # Unit Price column
        parts[10] = transform_money(parts[10])  # Unit Cost column
        parts[11] = transform_money(parts[11])  # Total Revenue column
        parts[12] = transform_money(parts[12])  # Total Cost column
        parts[13] = transform_money(parts[13])  # Total Profit column

        print(','.join(parts))  # Change to '\t'.join(parts) if your output should be tab-delimited
    else:
        print("Invalid line: not enough parts", file=sys.stderr)

print("Mapper completed", file=sys.stderr)
