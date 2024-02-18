import csv
from datetime import datetime

sample_data = "0,Australia and Oceania,Palau,Office Supplies,Online,H,3/6/2016,517073523,3/26/2016,2401,651.21,524.96,1563555.21,1260428.96,303126.25"

def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return 'Invalid-Date'

def capitalize_country_all_upper(country_name):
    return country_name.upper()

def int_(units):
    try:
        return str(round(float(units)))
    except ValueError:
        return 'Invalid-Units'

def process_line(line):
    columns = line.strip().split(',')
    if len(columns) == 15 :
        columns[1] = capitalize_country_all_upper(columns[1])
        columns[6] = convert_date_format(columns[6])
        columns[8] = convert_date_format(columns[8])
        columns[9] = int_(columns[9])
        for i in [10, 11, 12, 13, 14]:
            columns[i] = int_(columns[i])
        return ','.join(columns)
    return line

# Applying the transformation
transformed_line = process_line(sample_data)

# Appending transformed line to sample_data.csv
with open('sample_data.csv', 'a', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(transformed_line.split(','))

print("Transformed line has been appended to sample_data.csv")

