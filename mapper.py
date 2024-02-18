#!/usr/bin/env python
import sys
from datetime import datetime

def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return 'Invalid-Date'

def capitalize_region_all_upper(region_name):
    return region_name.upper()

def int_(units):
    try:
        return str(round(float(units)))
    except ValueError:
        return 'Invalid-Units'

def process_line(line):
    columns = line.strip().split(',')
    if len(columns) == 15 :
        columns[1] = capitalize_region_all_upper(columns[1])
        columns[6] = convert_date_format(columns[6])
        columns[8] = convert_date_format(columns[8])
        columns[9] = int_(columns[9])
        for i in [10, 11, 12, 13, 14]:
            columns[i] = int_(columns[i])
        return ','.join(columns)
    return line



def main():
    for line in sys.stdin:
        processed_line = process_line(line)
        print(processed_line)

if __name__ == '__main__':
    main()



