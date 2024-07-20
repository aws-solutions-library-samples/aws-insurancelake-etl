#!/usr/bin/env python3
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
"""
Script to populate the DynamoDB table for the multilookup transform
with lookup data. Requires a CSV file with lookup data and return
values. Script requires lookup columns to be specified. All other
columns will assumed to be return values. Only return value
columns will be written to the DynamoDB table separately. The lookup
columns will be combined to a single lookup_item key.

Example usage:
	./load_dynamodb_multilookup_table.py dev-insurancelake-etl-multi-lookup lookups.csv PolicyData-LOBCoverage originalprogram originalcoverage
"""
import boto3
import argparse
import csv

parser = argparse.ArgumentParser()
parser.add_argument('table_name',
	help='Name of DynamoDB multilookup table to be populated by the script')
parser.add_argument('data_file',
	help='Path/file name of CSV file to use for populating the DynamoDB table')
parser.add_argument('lookup_group',
	help='Value to use for lookup_group in DynamoDB table (grouping of lookup_items)')
parser.add_argument('lookup_columns', nargs='+',
	help='Lookup columns to use for combined lookup_item value')
args = parser.parse_args()

with open(args.data_file, mode='r', encoding='utf-8') as lookup_data_file:
	lookup_data_file = csv.DictReader(lookup_data_file)

	dynamodb_write_data = []
	for row in lookup_data_file:
		# lookup_columns are used to create the single lookup key and removed from the row
		lookup_item = '-'.join([row.pop(lookup_column) for lookup_column in args.lookup_columns])

		# Remaining columns are considered all return columns
		row_to_write = { return_column: row[return_column] for return_column in row }

		# Set partition key and sort key
		row_to_write.update({
			'lookup_group': args.lookup_group,
			'lookup_item': lookup_item,
			
		})

		dynamodb_write_data.append(row_to_write)

# Write data to DynamoDB table
dynamodb = boto3.resource('dynamodb')
lookup_table = dynamodb.Table(args.table_name)

with lookup_table.batch_writer() as batch:
	for data_row in dynamodb_write_data:
		response = batch.put_item(data_row)

print(f'{len(dynamodb_write_data)} rows written to {args.table_name} for lookup_group {args.lookup_group}')