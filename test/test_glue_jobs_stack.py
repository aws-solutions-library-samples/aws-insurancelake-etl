# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template

from test.boto_mocking_helper import *
from lib.dynamodb_stack import DynamoDbStack
from lib.glue_jobs_stack import GlueJobsStack
from lib.glue_buckets_stack import GlueBucketsStack
from lib.athena_workgroup_stack import AthenaWorkgroupStack

import lib.configuration as configuration
from lib.configuration import (
    DEV, PROD, TEST, ACCOUNT_ID, REGION, LOGICAL_ID_PREFIX, RESOURCE_NAME_PREFIX, VPC_CIDR, LINEAGE,
)

def mock_get_local_configuration_with_vpc(environment, local_mapping = None):
	return {
		ACCOUNT_ID: mock_account_id,
		REGION: mock_region,
		LINEAGE: False,
		# Mix Deploy environment variables so we can return one dict for all environments
		LOGICAL_ID_PREFIX: 'TestLake',
		RESOURCE_NAME_PREFIX: 'testlake',
		VPC_CIDR: '10.0.0.0/24',
	}

def get_dependent_stacks(app):
	dynamodb_stack = DynamoDbStack(
		app,
		'DynamoDbStackForTests',
		target_environment=DEV
	)

	glue_buckets_stack = GlueBucketsStack(
		app,
		'GlueBucketsStackForTests',
		target_environment=DEV,
	)

	athena_workgroup_stack = AthenaWorkgroupStack(
		app,
		'AthenaWorkgroupStackForTests',
		target_environment=DEV,
		glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
	)

	return (dynamodb_stack, glue_buckets_stack, athena_workgroup_stack)


def test_resource_types_and_counts(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()

	# Use one of each dependent stack for all 3 environments since they are not the test subject
	(dynamodb_stack, glue_buckets_stack, athena_workgroup_stack) = get_dependent_stacks(app)

	glue_jobs_stacks = {}
	for environment in [DEV, TEST, PROD]:
		glue_jobs_stacks[environment] = GlueJobsStack(
			app,
			f'{environment}-GlueJobsStackForTests',
			target_environment=environment,
			hash_values_table=dynamodb_stack.hash_values_table,
			value_lookup_table=dynamodb_stack.value_lookup_table,
			multi_lookup_table=dynamodb_stack.value_lookup_table,
			dq_results_table=dynamodb_stack.dq_results_table,
			data_lineage_table=dynamodb_stack.data_lineage_table,
			glue_scripts_bucket=glue_buckets_stack.glue_scripts_bucket,
			glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
			athena_workgroup=athena_workgroup_stack.athena_workgroup,
		)

	# All stacks should be generated before calling Template methods
	for environment in glue_jobs_stacks.keys():
		template = Template.from_stack(glue_jobs_stacks[environment])

		# Collect-Cleanse, Cleanse-Consume, Consume-Entity-Match
		template.resource_count_is('AWS::Glue::Job', 3)
		# S3 Deployment CustomResource handler, CustomResource Log Retention handler
		template.resource_count_is('AWS::Lambda::Function', 2)
		# Glue Job role, S3 Deployment CustomResource handler role, CustomResource Log Retention handler role
		template.resource_count_is('AWS::IAM::Role', 3)


def test_stack_properties_and_exports(monkeypatch):
	"""Test that stack exports the Glue job role, and has properties needed by other stacks"""
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()
	(dynamodb_stack, glue_buckets_stack, athena_workgroup_stack) = get_dependent_stacks(app)

	glue_jobs_stack = GlueJobsStack(
		app,
		f'Dev-GlueJobsStackForTests',
		target_environment=DEV,
		hash_values_table=dynamodb_stack.hash_values_table,
		value_lookup_table=dynamodb_stack.value_lookup_table,
		multi_lookup_table=dynamodb_stack.value_lookup_table,
		dq_results_table=dynamodb_stack.dq_results_table,
		data_lineage_table=dynamodb_stack.data_lineage_table,
		glue_scripts_bucket=glue_buckets_stack.glue_scripts_bucket,
		glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
		athena_workgroup=athena_workgroup_stack.athena_workgroup,
	)

	# Verify stack has the expected properties set
	assert glue_jobs_stack.collect_to_cleanse_job is not None
	assert glue_jobs_stack.cleanse_to_consume_job is not None
	assert glue_jobs_stack.consume_entity_match_job is not None

	# Verify stack has the expected CF exports
	template = Template.from_stack(glue_jobs_stack)
	stack_outputs = template.find_outputs('*')

	glue_iam_role_output = False
	for output_id in stack_outputs.keys():
		output_name = stack_outputs[output_id]['Export']['Name']

		if output_name.find('GlueRole') != -1:
			glue_iam_role_output = True

	assert glue_iam_role_output, 'Missing CF output for Glue IAM role'


def test_glue_connections_with_vpc(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)
	monkeypatch.setattr(configuration, 'get_local_configuration', mock_get_local_configuration_with_vpc)

	app = cdk.App()
	(dynamodb_stack, glue_buckets_stack, athena_workgroup_stack) = get_dependent_stacks(app)

	glue_jobs_stack = GlueJobsStack(
		app,
		'Dev-GlueJobsStackForTests',
		target_environment=DEV,
		hash_values_table=dynamodb_stack.hash_values_table,
		value_lookup_table=dynamodb_stack.value_lookup_table,
		multi_lookup_table=dynamodb_stack.value_lookup_table,
		dq_results_table=dynamodb_stack.dq_results_table,
		data_lineage_table=dynamodb_stack.data_lineage_table,
		glue_scripts_bucket=glue_buckets_stack.glue_scripts_bucket,
		glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
		athena_workgroup=athena_workgroup_stack.athena_workgroup,
	)
	template = Template.from_stack(glue_jobs_stack)

	# 3 Glue Connections, one for each AZ
	template.resource_count_is('AWS::Glue::Connection', 3)