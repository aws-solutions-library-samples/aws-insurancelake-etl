# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
from aws_cdk.assertions import Template, Match

import lib.tagging as tagging
from lib.tagging import (
	COST_CENTER, TAG_ENVIRONMENT, TEAM, APPLICATION
)
from lib.configuration import (
	ENVIRONMENT, DEPLOYMENT, DEV, PROD, TEST
)

test_environment = DEPLOYMENT
test_id_prefix = 'TestPrefix'
test_resource_prefix = 'testprefix'

def mock_get_all_configurations():
	return { test_environment: {
				ENVIRONMENT: test_environment
			}
		}

def mock_get_logical_id_prefix():
	return test_id_prefix

def mock_get_resource_name_prefix():
	return test_resource_prefix


def test_get_tag(monkeypatch):
	monkeypatch.setattr(tagging, 'get_all_configurations', mock_get_all_configurations)
	monkeypatch.setattr(tagging, 'get_logical_id_prefix', mock_get_logical_id_prefix)
	monkeypatch.setattr(tagging, 'get_resource_name_prefix', mock_get_resource_name_prefix)

	test_tags = tagging.get_tag(APPLICATION, test_environment)
	assert f'{test_id_prefix}Etl' in test_tags

	test_tags = tagging.get_tag(TAG_ENVIRONMENT, test_environment)
	assert test_environment in test_tags


def test_get_tag_missing_environment_error(monkeypatch):
	monkeypatch.setattr(tagging, 'get_all_configurations', mock_get_all_configurations)
	monkeypatch.setattr(tagging, 'get_logical_id_prefix', mock_get_logical_id_prefix)
	monkeypatch.setattr(tagging, 'get_resource_name_prefix', mock_get_resource_name_prefix)

	with pytest.raises(AttributeError) as e_info:
		tagging.get_tag(APPLICATION, 'BadEnvironment')

	assert e_info.match('not found in environment configurations'), \
		'Expected Attribute Error for missing environment not raised'


def test_tagging_stack_resource(monkeypatch):
	monkeypatch.setattr(tagging, 'get_all_configurations', mock_get_all_configurations)
	monkeypatch.setattr(tagging, 'get_logical_id_prefix', mock_get_logical_id_prefix)
	monkeypatch.setattr(tagging, 'get_resource_name_prefix', mock_get_resource_name_prefix)

	app = cdk.App()
	stack = cdk.Stack(app, 'StackForTests')
	s3.Bucket(stack, 'BucketForTests')
	tagging.tag(stack, test_environment)

	template = Template.from_stack(stack)
	template.has_resource_properties(
		'AWS::S3::Bucket',
		Match.object_like(
			{
				"Tags": [
					{
						"Key": f"{test_resource_prefix}:application",
						"Value": f"{test_id_prefix}Etl"
					},
					{
						"Key": f"{test_resource_prefix}:cost-center",
						"Value": f"{test_id_prefix}Etl"
					},
					{
						"Key": f"{test_resource_prefix}:environment",
						"Value": test_environment
					},
					{
						"Key": f"{test_resource_prefix}:team",
						"Value": f"{test_id_prefix}Admin"
					}
				]
			}
		)
	)