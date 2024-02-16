import sys
import csv
from datetime import datetime

def transform_date(date_str):
    # Transform the date as required
    return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")

def transform_money(money_str):
    # Transform the money as required
    return float(money_str)

for line in sys.stdin:
    reader = csv.reader([line])
    for row in reader:
        # Assuming date is in the third column and money value in the sixth
        row[2] = transform_date(row[2])
        row[5] = transform_money(row[5])
        print(','.join(map(str, row)))

