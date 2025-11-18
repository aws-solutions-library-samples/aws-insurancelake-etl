# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template

from test.boto_mocking_helper import *
from lib.glue_buckets_stack import GlueBucketsStack

import lib.configuration as configuration
from lib.configuration import (
    DEV, PROD, TEST
)

def test_resource_types_and_counts(monkeypatch):
    monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

    app = cdk.App()

    glue_buckets_stacks = {}
    for environment in [DEV, TEST, PROD]:
        glue_buckets_stacks[environment] = GlueBucketsStack(
            app,
            f'{environment}-GlueBucketsStackForTests',
            target_environment=environment,
        )

    # All stacks should be generated before calling Template methods
    for environment in glue_buckets_stacks.keys():
        template = Template.from_stack(glue_buckets_stacks[environment])

        # Should have 2 S3 buckets: scripts and temp
        template.resource_count_is('AWS::S3::Bucket', 2)

def test_stack_properties_and_exports(monkeypatch):
    """Test that stack exports the bucket names and ARNs, and has properties
    needed by other stacks
    """
    monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

    app = cdk.App()
    
    glue_buckets_stack = GlueBucketsStack(
        app,
        'GlueBucketsStackForTests',
        target_environment=DEV,
    )

    # Verify stack has the expected properties set
    assert glue_buckets_stack.glue_scripts_bucket is not None
    assert glue_buckets_stack.glue_scripts_temp_bucket is not None

    # Verify stack has the expected CF exports
    template = Template.from_stack(glue_buckets_stack)
    stack_outputs = template.find_outputs('*')

    scripts_bucket_output = False
    scripts_bucket_arn_output = False
    temp_bucket_output = False
    temp_bucket_arn_output = False
    
    for output_id in stack_outputs.keys():
        output_name = stack_outputs[output_id]['Export']['Name']

        if output_name.find('GlueScriptsBucket') != -1 and not output_name.endswith('Arn'):
            scripts_bucket_output = True
        if output_name.find('GlueScriptsBucketArn') != -1:
            scripts_bucket_arn_output = True
        if output_name.find('GlueScriptsTempBucket') != -1 and not output_name.endswith('Arn'):
            temp_bucket_output = True
        if output_name.find('GlueScriptsTempBucketArn') != -1:
            temp_bucket_arn_output = True

    assert scripts_bucket_output, 'Missing CF output for Glue scripts bucket'
    assert scripts_bucket_arn_output, 'Missing CF output for Glue scripts bucket ARN'
    assert temp_bucket_output, 'Missing CF output for Glue scripts temp bucket'
    assert temp_bucket_arn_output, 'Missing CF output for Glue scripts temp bucket ARN'