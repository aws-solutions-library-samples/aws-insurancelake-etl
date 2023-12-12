# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest

from test.boto_mocking_helper import *
import lib.configuration as configuration
from lib.configuration import (
    AVAILABILITY_ZONE_1, AVAILABILITY_ZONE_2, AVAILABILITY_ZONE_3,
	ROUTE_TABLE_1, ROUTE_TABLE_2, ROUTE_TABLE_3,
    SHARED_SECURITY_GROUP_ID, SUBNET_ID_1, SUBNET_ID_2, SUBNET_ID_3, VPC_ID,
	S3_KMS_KEY, S3_PURPOSE_BUILT_BUCKET,
	ACCOUNT_ID, REGION,
	ENVIRONMENT, DEPLOYMENT, DEV, PROD, TEST, RESOURCE_NAME_PREFIX
)


def test_get_local_configuration_returns_four_valid_environments(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	for environment in [DEPLOYMENT, DEV, TEST, PROD]:
		local_config = configuration.get_local_configuration(environment)

		assert ACCOUNT_ID in local_config, f'Missing AWS Account from {environment} environment'
		assert REGION in local_config, f'Missing AWS Region from {environment} environment'


def test_get_local_configuration_catches_invalid_prefix(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	with pytest.raises(AttributeError) as e_info:
		configuration.get_local_configuration(DEPLOYMENT, local_mapping={
			DEPLOYMENT: {
				ACCOUNT_ID: mock_account_id,
				REGION: mock_region,
				RESOURCE_NAME_PREFIX: 'Bad_Prefix'
			}
		})

	assert e_info.match('names may only contain'), \
		'Expected Attribute Error for invalid characters in resource name prefix not raised'


def test_get_local_configuration_catches_long_prefix(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	with pytest.raises(AttributeError) as e_info:
		configuration.get_local_configuration(DEPLOYMENT, local_mapping={
			DEPLOYMENT: {
				ACCOUNT_ID: '12digitslong',
				REGION: mock_region,
				RESOURCE_NAME_PREFIX: 'really-long-resource-name-that-will-break-s3-buckets'
			}
		})

	assert e_info.match('prefix is too long'), \
		'Expected Attribute Error for long resource name prefix not raised'


def test_get_local_configuration_catches_bad_environment(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	with pytest.raises(AttributeError) as e_info:
		configuration.get_local_configuration('BadEnvironment', local_mapping={
			DEPLOYMENT: {
				ACCOUNT_ID: mock_account_id,
				REGION: mock_region,
				RESOURCE_NAME_PREFIX: 'testlake'
			}
		})

	assert e_info.match('does not exist in local mappings'), \
		'Expected Attribute Error for invalid environment not raised'


def test_get_environment_configuration_has_outputs_and_environment(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	test_config = configuration.get_environment_configuration(PROD)
	for parameter in [
			ACCOUNT_ID, REGION,
			ENVIRONMENT, VPC_ID, SHARED_SECURITY_GROUP_ID, 
			S3_KMS_KEY, S3_PURPOSE_BUILT_BUCKET
		]:
		assert parameter in test_config, f'Missing {parameter} from PROD environment configuration'


def test_get_all_configurations_has_all_environments(monkeypatch):
	def mock_get_environment_configuration(environment: str):
		return { ENVIRONMENT: environment }

	monkeypatch.setattr(configuration, 'get_environment_configuration', mock_get_environment_configuration)
	# The same mock can work for both functions in this test
	monkeypatch.setattr(configuration, 'get_local_configuration', mock_get_environment_configuration)

	all_config = configuration.get_all_configurations()
	for environment in [DEPLOYMENT, DEV, TEST, PROD]:
		assert environment in all_config


def test_get_logical_id_prefix_returns_string(monkeypatch):
	# Patch boto3, not get_local_configuration() so we can test the structure of local_mapping
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	test_logic_id_prefix = configuration.get_logical_id_prefix()
	assert isinstance(test_logic_id_prefix, str)
	assert len(test_logic_id_prefix) > 0


def test_get_resource_name_prefix_returns_string(monkeypatch):
	# Patch boto3, not get_local_configuration() so we can test the structure of local_mapping
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	test_resource_name_prefix = configuration.get_resource_name_prefix()
	assert isinstance(test_resource_name_prefix, str)
	assert len(test_resource_name_prefix) > 0