#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

print('Ayoub Was her')
# Transformation function
def transform_data(line):
    # Example transformation: Convert text to uppercase
    return line.upper()

# Read input from standard input, transform, and write to standard output
for line in sys.stdin:
    transformed_line = transform_data(line.strip())
    print(transformed_line)
