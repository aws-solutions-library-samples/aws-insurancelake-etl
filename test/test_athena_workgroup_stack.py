# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template

from test.boto_mocking_helper import *
from lib.dynamodb_stack import DynamoDbStack
from lib.glue_buckets_stack import GlueBucketsStack
from lib.athena_workgroup_stack import AthenaWorkgroupStack

import lib.configuration as configuration
from lib.configuration import (
	DEV, PROD, TEST
)

def test_resource_types_and_counts(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()

	# Use one stack for all 3 environments since it is not the test subject
	glue_buckets_stack = GlueBucketsStack(
		app,
		'GlueBucketsStackForTests',
		target_environment=DEV,
	)

	athena_stacks = {}
	for environment in [DEV, TEST, PROD]:
		athena_stacks[environment] = AthenaWorkgroupStack(
			app,
			f'{environment}-AthenaWorkgroupStackForTests',
			target_environment=environment,
			glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket
		)

	# All stacks should be generated before calling Template methods
	for environment in athena_stacks.keys():
		template = Template.from_stack(athena_stacks[environment])

		template.resource_count_is('AWS::Athena::WorkGroup', 1)

def test_stack_properties(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()

	glue_buckets_stack = GlueBucketsStack(
		app,
		'GlueBucketsStackForTests',
		target_environment=DEV,
	)

	athena_workgroup_stack = AthenaWorkgroupStack(
		app,
		f'Dev-AthenaWorkgroupStackForTests',
		target_environment=DEV,
		glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket
	)

	assert athena_workgroup_stack.athena_workgroup is not None