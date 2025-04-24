# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import re
import boto3

# Environments (targeted at accounts)
DEPLOYMENT = 'Deploy'
DEV = 'Dev'
TEST = 'Test'
PROD = 'Prod'

# The following constants are used to map to parameter paths
ENVIRONMENT = 'environment'

# Manual Inputs
CODECONNECTIONS_ARN = 'codeconnections_arn'
CODECONNECTIONS_REPOSITORY_OWNER_NAME = 'codeconnections_repository_owner_name'
CODECONNECTIONS_REPOSITORY_NAME = 'codeconnections_repository_name'
CODECOMMIT_REPOSITORY_NAME = 'codecommit_repository_name'
CODECOMMIT_MIRROR_REPOSITORY_NAME = 'codecommit_mirror_repository_name'
ACCOUNT_ID = 'account_id'
REGION = 'region'
VPC_CIDR = 'vpc_cidr'
LOGICAL_ID_PREFIX = 'logical_id_prefix'
RESOURCE_NAME_PREFIX = 'resource_name_prefix'
CODE_BRANCH = 'code_branch'
LINEAGE='lineage'

# Used in Automated Outputs
VPC_ID = 'vpc_id'
AVAILABILITY_ZONE_1 = 'availability_zone_1'
AVAILABILITY_ZONE_2 = 'availability_zone_2'
AVAILABILITY_ZONE_3 = 'availability_zone_3'
SUBNET_ID_1 = 'subnet_id_1'
SUBNET_ID_2 = 'subnet_id_2'
SUBNET_ID_3 = 'subnet_id_3'
ROUTE_TABLE_1 = 'route_table_1'
ROUTE_TABLE_2 = 'route_table_2'
ROUTE_TABLE_3 = 'route_table_3'
SHARED_SECURITY_GROUP_ID = 'shared_security_group_id'
S3_KMS_KEY = 's3_kms_key'
S3_ACCESS_LOG_BUCKET = 's3_access_log_bucket'
S3_RAW_BUCKET = 's3_raw_bucket'
S3_CONFORMED_BUCKET = 's3_conformed_bucket'
S3_PURPOSE_BUILT_BUCKET = 's3_purpose_built_bucket'
CROSS_ACCOUNT_DYNAMODB_ROLE = 'cross_account_dynamodb_role'
STATE_MACHINE = 'sfn_state_machine'
NOTIFICATION_TOPIC = 'sns_topic'

# Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
MAX_S3_BUCKET_NAME_LENGTH = 63

def get_local_configuration(environment: str, local_mapping: dict = None) -> dict:
    """Provides manually configured variables that are validated for quality and safety.

    Parameters
    ----------
    environment
        The environment used to retrieve corresponding configuration
    local_mapping: optional
        Optional override the embedded local_mapping; used for testing

    Raises
    ------
    AttributeError
        If the resource_name_prefix does not conform or if the requested
        environment does not exist

    Returns
    -------
    dict
        Configuration for the requested environment
    """
    active_account_id = boto3.client('sts').get_caller_identity()['Account']

    if local_mapping is None:
        local_mapping = {
            DEPLOYMENT: {
                ACCOUNT_ID: active_account_id,
                REGION: 'us-east-2',

                # If you use Github, Gitlab, Bitbucket Cloud or any other supported CodeConnections
                # provider, specify the CodeConnections ARN
                CODECONNECTIONS_ARN: '',

                # CodeConections repository owner or workspace name if using CodeConnections
                CODECONNECTIONS_REPOSITORY_OWNER_NAME: '',

                # Leave empty if you do not use CodeConnections
                CODECONNECTIONS_REPOSITORY_NAME: '',

                # Use only if your repository is already in CodecCommit, otherwise leave empty!
                CODECOMMIT_REPOSITORY_NAME: '',

                # Name your CodeCommit mirror repo here (recommend matching your external repo)
                # Leave empty if you use CodeConnections or your repository is in CodeCommit already
                CODECOMMIT_MIRROR_REPOSITORY_NAME: 'aws-insurancelake-etl',

                # This is used in the Logical Id of CloudFormation resources.
                # We recommend Capital case for consistency, e.g. DataLakeCdkBlog
                LOGICAL_ID_PREFIX: 'InsuranceLake',

                # Important: This is used as a prefix for resources that must be **globally** unique!
                # Resource names may only contain alphanumeric characters, hyphens, and cannot contain trailing hyphens.
                # S3 bucket names from this application must be under the 63 character bucket name limit
                RESOURCE_NAME_PREFIX: 'insurancelake',
            },
            DEV: {
                ACCOUNT_ID: active_account_id,
                REGION: 'us-east-2',
                LINEAGE: True,
                # VPC_CIDR: '10.20.0.0/22',
                CODE_BRANCH: 'develop',
            },
            TEST: {
                ACCOUNT_ID: active_account_id,
                REGION: 'us-east-2',
                LINEAGE: True,
                # VPC_CIDR: '10.10.0.0/22',
                CODE_BRANCH: 'test',
            },
            PROD: {
                ACCOUNT_ID: active_account_id,
                REGION: 'us-east-2',
                LINEAGE: True,
                # VPC_CIDR: '10.0.0.0/22',
                CODE_BRANCH: 'main',
            }
        }

    resource_prefix = local_mapping[DEPLOYMENT][RESOURCE_NAME_PREFIX]
    if (
        not re.fullmatch('^[a-z0-9-]+', resource_prefix)
        or '-' in resource_prefix[-1:] or '-' in resource_prefix[1]
    ):
        raise AttributeError('Resource names may only contain lowercase alphanumeric and hyphens '
                        'and cannot contain leading or trailing hyphens')

    for each_env in local_mapping:
        # NOTE: Resource with longest bucket name is from the infra
        #       code base, but we will assume the user wants to have
        #       a consistent resource prefix across all stacks
        longest_bucket_name = \
            f'{each_env}-{resource_prefix}-{local_mapping[each_env][ACCOUNT_ID]}-{local_mapping[each_env][REGION]}-access-logs'
        if len(longest_bucket_name) > MAX_S3_BUCKET_NAME_LENGTH:
            raise AttributeError('Resource name prefix is too long; at least one S3 bucket name '
                        f'would exceed maximum allowed length of {MAX_S3_BUCKET_NAME_LENGTH} '
                        f'characters, e.g. {longest_bucket_name}')

    if environment not in local_mapping:
        raise AttributeError(f'The requested environment: {environment} does not exist in local mappings')

    return local_mapping[environment]


def get_environment_configuration(environment: str, local_mapping: dict = None) -> dict:
    """Provides all configuration values for the given target environment

    Parameters
    ----------
    environment
        The environment used to retrieve corresponding configuration
    local_mapping: optional
        Optionally override the embedded local_mapping; used for testing

    Returns
    -------
    dict
        Combined configuration and Cloudformation output names for target environment
    """
    cloudformation_output_mapping = {
        ENVIRONMENT: f'{environment}',
        VPC_ID: f'{environment}VpcId',
        AVAILABILITY_ZONE_1: f'{environment}AvailabilityZone1',
        AVAILABILITY_ZONE_2: f'{environment}AvailabilityZone2',
        AVAILABILITY_ZONE_3: f'{environment}AvailabilityZone3',
        SUBNET_ID_1: f'{environment}SubnetId1',
        SUBNET_ID_2: f'{environment}SubnetId2',
        SUBNET_ID_3: f'{environment}SubnetId3',
        ROUTE_TABLE_1: f'{environment}RouteTable1',
        ROUTE_TABLE_2: f'{environment}RouteTable2',
        ROUTE_TABLE_3: f'{environment}RouteTable3',
        SHARED_SECURITY_GROUP_ID: f'{environment}SharedSecurityGroupId',
        S3_KMS_KEY: f'{environment}S3KmsKeyArn',
        S3_ACCESS_LOG_BUCKET: f'{environment}S3AccessLogBucket',
        S3_RAW_BUCKET: f'{environment}CollectBucketName',
        S3_CONFORMED_BUCKET: f'{environment}CleanseBucketName',
        S3_PURPOSE_BUILT_BUCKET: f'{environment}ConsumeBucketName',
        CROSS_ACCOUNT_DYNAMODB_ROLE: f'{environment}CrossAccountDynamoDbRoleArn',
        STATE_MACHINE: f'{environment}StepFunctionsStateMachineName',
        NOTIFICATION_TOPIC: f'{environment}EtlNotificationSnsTopicName',
    }

    return {
        **cloudformation_output_mapping,
        **get_local_configuration(environment, local_mapping=local_mapping)
    }


def get_all_configurations() -> dict:
    """Returns a dict mapping of configurations for all environments.
    These keys correspond to static values, stack names, and CloudFormation outputs

    Returns
    -------
    dict
        Combined configuration and Cloudformation output names for all environments
    """
    return {
        DEPLOYMENT: {
            ENVIRONMENT: DEPLOYMENT,
            **get_local_configuration(DEPLOYMENT),
        },
        DEV: get_environment_configuration(DEV),
        TEST: get_environment_configuration(TEST),
        PROD: get_environment_configuration(PROD),
    }


def get_logical_id_prefix() -> str:
    """Returns the logical id prefix to apply to all CloudFormation resources

    Returns
    -------
    str
        Logical ID prefix from deployment configuration
    """
    return get_local_configuration(DEPLOYMENT)[LOGICAL_ID_PREFIX]


def get_resource_name_prefix() -> str:
    """Returns the resource name prefix to apply to all resources names

    Returns
    -------
    str
        Resource name prefix from deployment configuration
    """
    return get_local_configuration(DEPLOYMENT)[RESOURCE_NAME_PREFIX]