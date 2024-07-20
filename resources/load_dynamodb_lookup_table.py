#!/usr/bin/env python3
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
"""
Script to populate the DynamoDB table for the lookup transform with
lookup data. Requires a JSON file with lookup data in key: value
format for each column on which the transform is being applied.
"""
import boto3
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument('source_system',
	help="Name of source data system that will be used to match against uploaded file path to the Collect bucket")
parser.add_argument('table_name',
	help="Name of DynamoDB lookup table to be populated by the script")
parser.add_argument('data_file',
	help="Path/file name of JSON file to use for populating the DynamoDB table")
args = parser.parse_args()
 
# Read data from JSON in the format:
# {
#	"column_name1": { "lookup_value1": "lookedup_value1", "lookup_value2": "lookedup_value2", ... },
#	"column_name2": { ... }
# }
with open(args.data_file, encoding='utf-8') as lookup_data_file:
	lookup_data = json.load(lookup_data_file)

# Prepare DynamoDB data in the form of the table schema
dynamodb_write_data = [
	{
		'source_system': args.source_system,
		'column_name': lookup,
		'lookup_data': json.dumps(lookup_data[lookup]),
	}
	for lookup in lookup_data
]

# Write data to DynamoDB table
dynamodb = boto3.resource('dynamodb')
lookup_table = dynamodb.Table(args.table_name)

with lookup_table.batch_writer() as batch:
	for data_row in dynamodb_write_data:
		response = batch.put_item(data_row)