# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template, Match

from test.boto_mocking_helper import *
from lib.glue_buckets_stack import GlueBucketsStack
from lib.data_lake_consumer_stack import DataLakeConsumerStack

import lib.configuration as configuration
from lib.configuration import (
    DEV, PROD, TEST
)

def test_resource_types_and_counts(monkeypatch):
    monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

    app = cdk.App()

    glue_buckets_stack = GlueBucketsStack(
        app,
        'GlueBucketsStackForTests',
        target_environment=DEV,
    )

    data_lake_consumer_stacks = {}
    for environment in [DEV, TEST, PROD]:
        data_lake_consumer_stacks[environment] = DataLakeConsumerStack(
            app,
            f'{environment}-DataLakeConsumerStackForTests',
            target_environment=environment,
            glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
        )

    # All stacks should be generated before calling Template methods
    for environment in data_lake_consumer_stacks.keys():
        template = Template.from_stack(data_lake_consumer_stacks[environment])

        # Should have 1 IAM managed policy
        template.resource_count_is('AWS::IAM::ManagedPolicy', 1)

def test_stack_exports(monkeypatch):
    """Test that stack exports the IAM policy name and ARN"""
    monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

    app = cdk.App()
    
    # Create GlueBucketsStack for dependencies
    glue_buckets_stack = GlueBucketsStack(
        app,
        'GlueBucketsStackForTests',
        target_environment=DEV,
    )

    data_lake_consumer_stack = DataLakeConsumerStack(
        app,
        'DataLakeConsumerStackForTests',
        target_environment=DEV,
        glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
    )

    template = Template.from_stack(data_lake_consumer_stack)
    stack_outputs = template.find_outputs('*')

    consumer_policy_output = False
    consumer_policy_arn_output = False
    
    for output_id in stack_outputs.keys():
        output_name = stack_outputs[output_id]['Export']['Name']

        if output_name.find('ConsumerPolicy') != -1 and not output_name.endswith('Arn'):
            consumer_policy_output = True
        if output_name.find('ConsumerPolicyArn') != -1:
            consumer_policy_arn_output = True

    assert consumer_policy_output, 'Missing CF output for consumer policy'
    assert consumer_policy_arn_output, 'Missing CF output for consumer policy ARN'