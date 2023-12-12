# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import aws_cdk as cdk
from aws_cdk.assertions import Template, Match

from test.boto_mocking_helper import *
from lib.code_commit_stack import CodeCommitStack

import lib.configuration as configuration
from lib.configuration import DEPLOYMENT


def test_resource_types_and_counts(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()

	codecommit_stack = CodeCommitStack(
		app,
		'Deploy-CodeCommitStackForTests',
		target_environment=DEPLOYMENT,
	)

	template = Template.from_stack(codecommit_stack)
	template.resource_count_is('AWS::CodeCommit::Repository', 1)
	template.resource_count_is('AWS::IAM::User', 1)


def test_stack_has_correct_outputs(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()

	codecommit_stack = CodeCommitStack(
		app,
		'Deploy-CodeCommitStackForTests',
		target_environment=DEPLOYMENT,
	)

	template = Template.from_stack(codecommit_stack)
	stack_outputs = template.find_outputs('*')

	repository_output = False
	mirror_user_output = False
	for output_id in stack_outputs.keys():
		output_name = stack_outputs[output_id]['Export']['Name']

		if output_name.find('EtlMirrorRepository') != -1:
			repository_output = True
		if output_name.find('EtlMirrorUser') != -1:
			mirror_user_output = True

	assert repository_output, 'Missing CF output for CodeCommit mirror repository'
	assert mirror_user_output, 'Missing CF output for mirror repository user'


def test_mirror_user_can_access_repository(monkeypatch):
	monkeypatch.setattr(configuration.boto3, 'client', mock_boto3_client)

	app = cdk.App()

	codecommit_stack = CodeCommitStack(
		app,
		'Deploy-CodeCommitStackForTests',
		target_environment=DEPLOYMENT,
	)

	template = Template.from_stack(codecommit_stack)
	template.has_resource_properties(
		'AWS::IAM::Policy',
		Match.object_like(
			{
				"PolicyDocument": {
					"Statement": [
						{
							"Action": [
								'codecommit:GitPull',
								'codecommit:GitPush'
							],
							"Effect": "Allow",
							"Resource": {
								"Fn::GetAtt": [
									Match.string_like_regexp(r'EtlMirrorRepository\S+'),
									"Arn"
								],
							}
						}
					],
				},
				"PolicyName": Match.any_value(),
				'Users': [
					{ 
						"Ref": Match.string_like_regexp(r'EtlGitExternalMirrorUser\S+')
					}
				]
			}
		)
	)