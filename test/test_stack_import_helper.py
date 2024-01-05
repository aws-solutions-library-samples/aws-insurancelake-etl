# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk

from test.boto_mocking_helper import *
from lib.stack_import_helper import ImportedBuckets, ImportedVpc

import lib.configuration as configuration
from lib.configuration import (
    DEPLOYMENT, DEV, PROD, TEST, ACCOUNT_ID, REGION, VPC_CIDR, RESOURCE_NAME_PREFIX,
)


def test_importedbuckets_resource_counts(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	for environment in [DEV, TEST, PROD]:
		app = cdk.App()
		stack = cdk.Stack(app, 'StackForTests')
		stack.mappings = configuration.get_environment_configuration(environment)
		ImportedBuckets(stack, logical_id_suffix='TestSuffix')

		bucket_proxy_count = 0
		key_proxy_count = 0
		for resource in stack.node.children:
			if str(type(resource)) == "<class 'jsii._reference_map.InterfaceDynamicProxy'>":
				for child_resource in resource._delegates:
					# Use string comparison because _IKeyProxy is a private class
					if str(type(child_resource)) == "<class 'aws_cdk.aws_kms._IKeyProxy'>":
						key_proxy_count += 1

			# Use string comparison because _BucketBaseProxy is a private class
			if str(type(resource)) == "<class 'aws_cdk.aws_s3._BucketBaseProxy'>":
				bucket_proxy_count += 1

		assert bucket_proxy_count == 4, \
			f'Unexpected number of imported S3 Bucket proxies in {environment} environment'
		assert key_proxy_count == 1, \
			f'Unexpected number of imported KMS key proxies in {environment} environment'


def test_importedvpcs_resource_counts(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()
	stack = cdk.Stack(app, 'StackForTests')
	stack.mappings = configuration.get_environment_configuration(DEV, local_mapping = {
		DEPLOYMENT: {
			ACCOUNT_ID: mock_account_id,
			REGION: mock_region,
			RESOURCE_NAME_PREFIX: 'testlake',
		},
		DEV: {
			ACCOUNT_ID: mock_account_id,
			REGION: mock_region,
			VPC_CIDR: '10.0.0.0/24'
		}
	})
	ImportedVpc(stack, logical_id_suffix='TestSuffix')

	vpc_proxy_count = 0
	subnet_proxy_count = 0
	securitygroup_proxy_count = 0
	for jsii_resource in stack.node.children:
		# Go another level deeper to get better resource type visibility
		for resource in jsii_resource._delegates:
			# Use string comparison because Proxies are private classes
			if str(type(resource)) == "<class 'aws_cdk.aws_ec2._IVpcProxy'>":
				vpc_proxy_count += 1
			if str(type(resource)) == "<class 'aws_cdk.aws_ec2._ISubnetProxy'>":
				subnet_proxy_count += 1
			if str(type(resource)) == "<class 'aws_cdk.aws_ec2._ISecurityGroupProxy'>":
				securitygroup_proxy_count += 1

	# Imported values are used and exposed as 3 specific resources on the stack
	assert vpc_proxy_count == 1, \
		'Unexpected number of imported VPC proxies'
	assert subnet_proxy_count == 3, \
		'Unexpected number of imported Subnet proxies'
	assert securitygroup_proxy_count == 1, \
		'Unexpected number of imported SecurityGroup proxies'