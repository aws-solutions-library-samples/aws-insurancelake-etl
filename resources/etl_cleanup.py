#!/usr/bin/env python3
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
"""
Script to clean-up artifacts left from running the pipeline and leave anything deployed by CDK

*** THIS SCRIPT DELETES DATA AND RESOURCES THAT CANNOT BE RECOVERED ***

Usage: etl_cleanup.py [-h] [--mode reallydelete|allbuckets|nodynamodb]

Options:
  -h, --help           show this help message and exit
  --mode reallydelete  Must specify to actually delete anything, otherwise runs dry-run
  --mode allbuckets    Also delete objects in the ETL scripts Glue job bucket (assumes reallydelete)
  --mode nodynamodb    Leaves DynamoDB tables intact (assumes reallydelete)

Ensure you have the correct account access and tokens in your environment, as well as the default
region specified in the AWS_DEFAULT_REGION environment variable.

Script is hard-coded to only work on the InsuranceLake Development environment.

Running without one of the --mode options will return a count of objects only (except S3 buckets,
which will report 0).

"""
import boto3
import botocore
import argparse
import re
import sys
import os

sys.path.append(os.path.dirname(__file__) + '/../lib')
from configuration import (
    S3_ACCESS_LOG_BUCKET, S3_CONFORMED_BUCKET, S3_PURPOSE_BUILT_BUCKET, S3_RAW_BUCKET, DEV, TEST, PROD,
    get_environment_configuration, get_resource_name_prefix, get_logical_id_prefix
)

# Strongly recommend only using this on the DEV environment
environment = DEV

parser = argparse.ArgumentParser()
parser.add_argument('--mode', dest='mode', required = False,
	choices=[ 'reallydelete', 'allbuckets', 'nodynamodb' ],
	help='Must specify to actually delete anything, otherwise runs dry-run; allbuckets includes '
	'the ETL scripts Glue job bucket with the Glue job scripts, and all JSON config, mapping, '
	'SQL, DQ rules; nodynamodb skips clearing DynamoDB tables')
args = parser.parse_args()
reallydelete = args.mode in [ 'reallydelete', 'allbuckets', 'nodynamodb' ]

if not reallydelete:
	print('**** DRY RUN MODE ENABLED. NO REAL DELETING WILL OCCUR ****')

mappings = get_environment_configuration(environment)
resource_prefix = get_resource_name_prefix()
logical_id_prefix = get_logical_id_prefix()

bucket_exports = [
   mappings[S3_ACCESS_LOG_BUCKET], 
   mappings[S3_CONFORMED_BUCKET], 
   mappings[S3_PURPOSE_BUILT_BUCKET],
   mappings[S3_RAW_BUCKET],
]

# Gather Cloudformation outputs for existing stack
buckets = []
dynamodb_tables = []
cf = boto3.client('cloudformation')
response = cf.list_exports()
while True:
	for export in response['Exports']:
		print(f"{export['Name']}: {export['Value']}")
		if export['Name'] in bucket_exports:
			buckets.append(export['Value'])
		if re.search(r'ExportsOutputRef\w+Table', export['Name']):
			dynamodb_tables.append(export['Value'])
		# These are automatically added exports that we do not have a mapping for
		if 'GlueScriptsTemporaryBucket' in export['Name'] and not export['Value'].startswith('arn'):
			buckets.append(export['Value'])
		if args.mode == 'allbuckets' and 'GlueScriptsBucket' in export['Name'] and not export['Value'].startswith('arn'):
			buckets.append(export['Value'])
	if 'NextToken' in response:
		response = cf.list_exports(nextToken=response['NextToken'])
	else:
		break


if not buckets and not dynamodb_tables:
	print('No Cloudformation exports found for S3 buckets or DynamoDB tables; something is probably wrong (check account and region)')
	sys.exit(1)


print ('Emptying Collect/Cleanse/Consume/Glue S3 buckets...')
s3 = boto3.resource('s3')
for bucket_name in buckets:
	s3_bucket = s3.Bucket(bucket_name)
	print(f'Emptying S3 bucket: {bucket_name}: ', end='')
	bucket_versioning = s3.BucketVersioning(bucket_name)
	versions_deleted = 0
	objects_deleted = 0
	if bucket_versioning.status == 'Enabled':
		if reallydelete:
			deleted = s3_bucket.object_versions.delete()
			if deleted:
				versions_deleted = len(deleted[0]['Deleted'])
	else:
		if reallydelete:
			deleted = s3_bucket.objects.all().delete()
			if deleted:
				objects_deleted = len(deleted[0]['Deleted'])
	print(f'{objects_deleted} Objects deleted, and {versions_deleted} Object Versions deleted')


print('Deleting Cleanse/Consume Glue Data Catalogs...')
glue = boto3.client('glue')
response_databases = glue.get_databases()
db_names = {}
while True:
	if 'DatabaseList' in response_databases:
		for database in response_databases['DatabaseList']:
			response_tables = glue.get_tables(DatabaseName=database['Name'])
			while True:
				if 'TableList' in response_tables:
					for table in response_tables['TableList']:
						match = re.search(r'^s3://([^\/]+)', table['StorageDescriptor']['Location'])
						if match and match.group(1) in buckets:
							# Table location matches an InsuranceLake S3 bucket name
							# Use a dict key to remove duplicates
							db_names[table['DatabaseName']] = 1
				if 'NextToken' in response_tables:
					response_tables = glue.get_tables(
						DatabaseName=database['Name'],
						nextToken=response_tables['NextToken']
					)
				else:
					break
	if 'NextToken' in response_databases:
		response_databases = glue.get_databases(nextToken=response_databases['NextToken'])
	else:
		break

for database in db_names.keys():
	print(f'Deleting Glue Catalog Database: {database}')
	if reallydelete:
		# Deleting the DB deletes all tables in the DB
		glue.delete_database(Name=database)


print('Deleting Glue Cloudwatch Log Groups...')
glue_log_groups = ['/aws-glue/jobs/logs-v2', '/aws-glue/jobs/output', '/aws-glue/jobs/error']
logs = boto3.client('logs')
for log_group_name in glue_log_groups:
	try:
		print(f'Deleting Log Group: {log_group_name}')
		if reallydelete:
			logs.delete_log_group(logGroupName=log_group_name)
	except logs.exceptions.ResourceNotFoundException as error:
		pass

print('Emptying Cloudwatch Log Groups for Lambda functions, State Machine, and VPC Flow Log...')
logs_to_empty = [
	f'/aws/lambda/{environment.lower()}-{resource_prefix}-etl-status-update',
	f'/aws/lambda/{environment.lower()}-{resource_prefix}-etl-trigger',
]

log_groups_response = logs.describe_log_groups()
if 'NextToken' in log_groups_response:
	print('Not all log groups were parsed. Support for multiple pages is not implemented.')
for log_group in log_groups_response['logGroups']:
	if re.search(fr'{environment}-{logical_id_prefix}\w+-\w+StateMachineLogGroup', log_group['logGroupName']):
		logs_to_empty.append(log_group['logGroupName'])
	if re.search(fr'{environment}-{logical_id_prefix}\w+-\w+VpcFlowLogGroup', log_group['logGroupName']):
		logs_to_empty.append(log_group['logGroupName'])
		print("NOTE: VPC Flow Logs are created continuously; you will see some log streams immediately after deleting")

for log_group_name in logs_to_empty:
	print(f'Emptying Log Group: {log_group_name}: ', end='')
	log_streams_deleted = 0

	try:
		log_streams_response = logs.describe_log_streams(logGroupName=log_group_name)
	except botocore.exceptions.ClientError as error:
		if error.response['Error']['Code'] == 'ResourceNotFoundException':
			# Log group doesn't exist; was never created by a Lambda execution
			continue
		else:
			raise error

	while True:
		for stream in log_streams_response['logStreams']:
			if reallydelete:
				logs.delete_log_stream(logGroupName=log_group_name, logStreamName=stream['logStreamName'])
			log_streams_deleted += 1
		if 'nextToken' in log_streams_response:
			log_streams_response = logs.describe_log_streams(
				logGroupName=log_group_name, 
				nextToken=log_streams_response['nextToken']
			)
		else:
			break
	print(f'{log_streams_deleted} Log Streams deleted')


if args.mode == 'nodynamodb':
	sys.exit()

print('Deleting all items in DynamoDB tables...')
dynamodb = boto3.resource('dynamodb')
for table_name in dynamodb_tables:
	table = dynamodb.Table(table_name)
	key_names = [ key['AttributeName'] for key in table.key_schema ]
	print(f'Emptying DynamoDB Table: {table_name}: ', end='')
	flag = False
	scan = table.scan()
	while True:
		items_deleted = 0
		with table.batch_writer() as batch:
			for each in scan['Items']:
				item_key = { key_name: each[key_name] for key_name in key_names }
				if reallydelete:
					batch.delete_item(Key=item_key)
				items_deleted += 1
			if 'LastEvaluatedKey' in scan:
				scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
			else:
				break
	print(f'{items_deleted} Items deleted')