# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template, Match

from test.boto_mocking_helper import *
from lib.dynamodb_stack import DynamoDbStack

import lib.configuration as configuration
from lib.configuration import (
    DEV, PROD, TEST, ACCOUNT_ID, REGION, LOGICAL_ID_PREFIX, RESOURCE_NAME_PREFIX, LINEAGE,
)


mock_environment = {
	ACCOUNT_ID: mock_account_id,
	REGION: mock_region,
	# Mix Deploy environment variables so we can return one dict for all environments
	LOGICAL_ID_PREFIX: 'TestLake',
	RESOURCE_NAME_PREFIX: 'testlake',
}

def mock_get_local_configuration_with_lineage(environment, local_mapping = None):
	lineage_environment = mock_environment.copy()
	lineage_environment.update({LINEAGE: True})
	return lineage_environment

def mock_get_local_configuration_without_lineage(environment, local_mapping = None):
	nolineage_environment = mock_environment.copy()
	nolineage_environment.update({LINEAGE: False})
	return nolineage_environment


def test_resource_types_and_counts_with_lineage(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)
	monkeypatch.setattr(configuration, 'get_local_configuration', mock_get_local_configuration_with_lineage)

	app = cdk.App()
	dynamodb_stack = DynamoDbStack(
		app,
		'Dev-DynamoDbStackForTests',
		target_environment=DEV,
	)
	template = Template.from_stack(dynamodb_stack)

	# Job audit table, lookup value data, multi lookup value, hash value table, dq results table, lineage table
	template.resource_count_is('AWS::DynamoDB::Table', 6)


def test_resource_types_and_counts_without_lineage(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)
	monkeypatch.setattr(configuration, 'get_local_configuration', mock_get_local_configuration_without_lineage)

	app = cdk.App()
	dynamodb_stack = DynamoDbStack(
		app,
		'Dev-DynamoDbStackForTests',
		target_environment=DEV,
	)
	template = Template.from_stack(dynamodb_stack)

	# Job audit table, lookup value data, multi lookup value, hash value table, dq results table
	template.resource_count_is('AWS::DynamoDB::Table', 5)


def test_resource_types_and_counts_all_environments(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()
	dynamodb_stacks = {}
	for environment in [DEV, TEST, PROD]:
		dynamodb_stacks[environment] = DynamoDbStack(
			app,
			f'{environment}-DynamoDbStackForTests',
			target_environment=environment,
		)

	# All stacks should be generated before calling Template methods
	for environment in dynamodb_stacks.keys():
		template = Template.from_stack(dynamodb_stacks[environment])

		# Job audit table, lookup value data, hash value table, dq results table regardless of configuration
		template.has_resource_properties(
			'AWS::DynamoDB::Table',
			Match.object_like(
				{
					"TableName": Match. string_like_regexp('job-audit')
				}
			)
		)
		template.has_resource_properties(
			'AWS::DynamoDB::Table',
			Match.object_like(
				{
					"TableName": Match. string_like_regexp('value-lookup')
				}
			)
		)
		template.has_resource_properties(
			'AWS::DynamoDB::Table',
			Match.object_like(
				{
					"TableName": Match. string_like_regexp('multi-lookup')
				}
			)
		)
		template.has_resource_properties(
			'AWS::DynamoDB::Table',
			Match.object_like(
				{
					"TableName": Match. string_like_regexp('hash-values')
				}
			)
		)
		template.has_resource_properties(
			'AWS::DynamoDB::Table',
			Match.object_like(
				{
					"TableName": Match. string_like_regexp('dq-results')
				}
			)
		)