# !/usr/bin/env python3
# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import os
import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks, NagSuppressions

from lib.pipeline_stack import PipelineStack
from lib.code_commit_stack import CodeCommitStack
from lib.configuration import (
    ACCOUNT_ID, CODECOMMIT_MIRROR_REPOSITORY_NAME, DEPLOYMENT, DEV, TEST, PROD, REGION, CODE_BRANCH,
    get_logical_id_prefix, get_all_configurations
)
from lib.tagging import tag

app = cdk.App()

# Enable CDK Nag for the Mirror repository, Pipeline, and related stacks
# Environment stacks must be enabled on the Stage resource
cdk.Aspects.of(app).add(AwsSolutionsChecks())

raw_mappings = get_all_configurations()

deployment_account = raw_mappings[DEPLOYMENT][ACCOUNT_ID]
deployment_region = raw_mappings[DEPLOYMENT][REGION]
deployment_aws_env = {
    'account': deployment_account,
    'region': deployment_region,
}
logical_id_prefix = get_logical_id_prefix()

if raw_mappings[DEPLOYMENT][CODECOMMIT_MIRROR_REPOSITORY_NAME] != '':
    mirror_repository_stack = CodeCommitStack(
        app,
        f'{DEPLOYMENT}-{logical_id_prefix}EtlMirrorRepository',
        description='InsuranceLake stack for ETL repository mirror (SO9489)',
        target_environment=DEPLOYMENT,
        env=deployment_aws_env,
    )
    tag(mirror_repository_stack, DEPLOYMENT)

if os.environ.get('ENV', DEV) == DEV:
    target_environment = DEV
    dev_account = raw_mappings[DEV][ACCOUNT_ID]
    dev_region = raw_mappings[DEV][REGION]
    dev_aws_env = {
        'account': dev_account,
        'region': dev_region,
    }
    dev_pipeline_stack = PipelineStack(
        app,
        f'{target_environment}-{logical_id_prefix}EtlPipeline',
        description=f'InsuranceLake stack for ETL pipeline - {DEV} environment (SO9489)',
        target_environment=DEV,
        target_branch=raw_mappings[DEV][CODE_BRANCH],
        target_aws_env=dev_aws_env,
        env=deployment_aws_env,
    )
    tag(dev_pipeline_stack, DEPLOYMENT)

# TODO: Check for cross-region or cross-account deployment and explicitly create a replication bucket with naming convention, encryption, access logs
if os.environ.get('ENV', TEST) == TEST:
    target_environment = TEST
    test_account = raw_mappings[TEST][ACCOUNT_ID]
    test_region = raw_mappings[TEST][REGION]
    test_aws_env = {
        'account': test_account,
        'region': test_region,
    }
    test_pipeline_stack = PipelineStack(
        app,
        f'{target_environment}-{logical_id_prefix}EtlPipeline',
        description=f'InsuranceLake stack for ETL pipeline - {TEST} environment (SO9489)',
        target_environment=TEST,
        target_branch=raw_mappings[TEST][CODE_BRANCH],
        target_aws_env=test_aws_env,
        env=deployment_aws_env,
    )
    tag(test_pipeline_stack, DEPLOYMENT)

if os.environ.get('ENV', PROD) == PROD:
    target_environment = PROD
    prod_account = raw_mappings[PROD][ACCOUNT_ID]
    prod_region = raw_mappings[PROD][REGION]
    prod_aws_env = {
        'account': prod_account,
        'region': prod_region,
    }
    prod_pipeline_stack = PipelineStack(
        app,
        f'{target_environment}-{logical_id_prefix}EtlPipeline',
        description=f'InsuranceLake stack for ETL pipeline - {PROD} environment (SO9489)',
        target_environment=PROD,
        target_branch=raw_mappings[PROD][CODE_BRANCH],
        target_aws_env=prod_aws_env,
        env=deployment_aws_env,
    )
    tag(prod_pipeline_stack, DEPLOYMENT)

    # TODO: Modify replication bucket to have access logs and key rotation
    # Apply tagging to cross-region support stacks
    for stack in app.node.children:
        # All other stacks in the app are custom constructs
        if type(stack) == cdk.Stack:
            # Use the deployment environment for tagging because there
            # is no way to determine 1:1 which pipeline created the stack
            tag(stack, DEPLOYMENT)

            NagSuppressions.add_resource_suppressions(stack, [
                {
                    'id': 'AwsSolutions-S1',
                    'reason': 'Cross-region support stack and bucket are auto-created by Codepipeline'
                },
                {
                    'id': 'AwsSolutions-KMS5',
                    'reason': 'Cross-region support stack and bucket are auto-created by Codepipeline'
                },
            ], apply_to_children=True)

app.synth()