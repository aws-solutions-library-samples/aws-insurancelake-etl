#!/usr/bin/env python3
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
"""
Script to convert 2 column CSV lookup data to JSON structure suitable for loading into
DynamoDB lookup_value table with load_dynamodb_lookup_table.py. Expects CSV input from stdin,
outputs JSON to stdout.

Usage: csv_to_json.py column_name < input.csv > output.json

Options:
  -h, --help           show this help message and exit
  column_name          Value to use for column_name in the DynamoDB JSON
  input.csv            2 column CSV input file (other columns will be ignored)
  output.json          JSON output suitable for load_dynamodb_lookup_table.py

"""
import sys
import csv
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('column_name',
	help="Value to use for column_name in the DynamoDB JSON")
args = parser.parse_args()

data = sys.stdin.readlines()
csvreader = csv.reader(data)
next(csvreader)
data_dict = { args.column_name: { row[0]: row[1] for row in csvreader } }
print(json.dumps(data_dict))