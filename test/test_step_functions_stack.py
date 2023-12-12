# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template

from test.boto_mocking_helper import *
from lib.step_functions_stack import StepFunctionsStack
from lib.dynamodb_stack import DynamoDbStack
from lib.glue_stack import GlueStack

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
		target_environment='Dev'
	)

	# Use one GlueStack stack for all 3 environments since it is not
	# the test subject
	glue_stack = GlueStack(
		app,
		'GlueStackForTests',
		target_environment='Dev',
        hash_values_table=dynamodb_stack.hash_values_table,
        value_lookup_table=dynamodb_stack.value_lookup_table,
        multi_lookup_table=dynamodb_stack.value_lookup_table,
		dq_results_table=dynamodb_stack.dq_results_table,
	)

	step_functions_stacks = {}
	for environment in [DEV, TEST, PROD]:
		step_functions_stacks[environment] = StepFunctionsStack(
			app,
			f'{environment}-StepFunctionsStackForTests',
			target_environment=environment,
			collect_to_cleanse_job=glue_stack.collect_to_cleanse_job,
			cleanse_to_consume_job=glue_stack.cleanse_to_consume_job,
			consume_entity_match_job=glue_stack.consume_entity_match_job,
			job_audit_table=dynamodb_stack.job_audit_table,
			glue_scripts_bucket=glue_stack.glue_scripts_bucket,
		)

	# All stacks should be generated before calling Template methods
	for environment in step_functions_stacks.keys():
		template = Template.from_stack(step_functions_stacks[environment])

		template.resource_count_is('AWS::StepFunctions::StateMachine', 1)
		# ETL Trigger, Job Audit Log, S3 Deployment CustomResource handler
		template.resource_count_is('AWS::Lambda::Function', 3)
		# 2 Lambda logs, 1 Statemachine log (future: include the CustomResource lambda log)
		template.resource_count_is('AWS::Logs::LogGroup', 3)
		# 3 Lambda roles, 1 Statemachine role
		template.resource_count_is('AWS::IAM::Role', 4)


def test_stack_has_correct_outputs(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()

	# Use one DynamoDbStack stack for all 3 environments since it is
	# not the test subject
	dynamodb_stack = DynamoDbStack(
		app,
		'DynamoDbStackForTests',
		target_environment='Dev'
	)

	# Use one GlueStack stack for all 3 environments since it is not
	# the test subject
	glue_stack = GlueStack(
		app,
		'GlueStackForTests',
		target_environment='Dev',
        hash_values_table=dynamodb_stack.hash_values_table,
        value_lookup_table=dynamodb_stack.value_lookup_table,
        multi_lookup_table=dynamodb_stack.value_lookup_table,
		dq_results_table=dynamodb_stack.dq_results_table,
	)

	step_functions_stack = StepFunctionsStack(
		app,
		'Dev-StepFunctionsStackForTests',
		target_environment='Dev',
		collect_to_cleanse_job=glue_stack.collect_to_cleanse_job,
		cleanse_to_consume_job=glue_stack.cleanse_to_consume_job,
		consume_entity_match_job=glue_stack.consume_entity_match_job,
		job_audit_table=dynamodb_stack.job_audit_table,
		glue_scripts_bucket=glue_stack.glue_scripts_bucket,
	)

	template = Template.from_stack(step_functions_stack)
	stack_outputs = template.find_outputs('*')

	state_machine_output = False
	sns_topic_output = False
	for output_id in stack_outputs.keys():
		output_name = stack_outputs[output_id]['Export']['Name']

		if output_name.find('StateMachineName') != -1:
			state_machine_output = True
		if output_name.find('SnsTopicName') != -1:
			sns_topic_output = True

	assert state_machine_output, 'Missing CF output for step functions state machine'
	assert sns_topic_output, 'Missing CF output for SNS topic'