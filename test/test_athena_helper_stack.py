# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template

from test.boto_mocking_helper import *
from lib.dynamodb_stack import DynamoDbStack
from lib.glue_stack import GlueStack
from lib.athena_helper_stack import AthenaHelperStack

import lib.configuration as configuration
from lib.configuration import (
	DEV, PROD, TEST
)


def test_resource_types_and_counts(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()

	# Use one DynamoDbStack stack for all 3 environments since it is
	# not the test subject
	dynamodb_stack = DynamoDbStack(
		app,
		'DynamoDbStackForTests',
		target_environment=DEV
	)

	# Use one GlueStack stack for all 3 environments since it is not
	# the test subject
	glue_stack = GlueStack(
		app,
		'GlueStackForTests',
		target_environment=DEV,
		hash_values_table=dynamodb_stack.hash_values_table,
		value_lookup_table=dynamodb_stack.value_lookup_table,
		multi_lookup_table=dynamodb_stack.value_lookup_table,
		dq_results_table=dynamodb_stack.dq_results_table,
	)

	athena_stacks = {}
	for environment in [DEV, TEST, PROD]:
		athena_stacks[environment] = AthenaHelperStack(
			app,
			f'{environment}-AthenaHelperStackForTests',
			target_environment=environment,
			glue_scripts_temp_bucket=glue_stack.glue_scripts_temp_bucket
		)

	# All stacks should be generated before calling Template methods
	for environment in athena_stacks.keys():
		template = Template.from_stack(athena_stacks[environment])

		template.resource_count_is('AWS::Athena::WorkGroup', 1)