# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
from moto import mock_aws
import boto3
import logging
from datetime import datetime, timedelta
import json

from test.boto_mocking_helper import *
import lib.state_machine_trigger.lambda_handler as lambda_handler

test_table_name = 'test-etl-job-audit'
test_state_machine_name = 'test-state-machine'
test_state_machine_arn = f'arn:aws:states:{mock_region}:{mock_account_id}:stateMachine:{test_state_machine_name}'
test_state_machine_role_arn = f'arn:aws:iam::123456789012:role/{test_state_machine_name}-role'
test_scripts_bucket = 'test-etl-scripts'
test_source_bucket_arn = 'arn:aws:s3:::test-bucket'
test_execution_id = '3b278a83-8b04-468a-983e-3749b6609dcb'
test_event_time = '2023-01-01T23:30:00.000Z'

test_success_event = {
    'Records': [{
        'eventTime': test_event_time, 
        's3': {
            'bucket': { 'name': 'test-bucket', 'arn': test_source_bucket_arn },
            'object': { 'key': 'level1/level2/test-file.csv' }
        },
        'userIdentity': { 'principalId': 'testuser' },
        'requestParameters': { 'sourceIPAddress': 'testipaddress'},
    }]
}

test_partition_override_event = {
    'Records': [{
        'eventTime': test_event_time,
        's3': {
            'bucket': { 'name': 'test-bucket', 'arn': test_source_bucket_arn },
            'object': { 'key': 'level1/level2/myyear/mymonth/test-file.csv' }
        },
        'userIdentity': { 'principalId': 'testuser' },
        'requestParameters': { 'sourceIPAddress': 'testipaddress'},
    }]
}

test_bad_file_path_event1 = {
    'Records': [{
        'eventTime': test_event_time, 
        's3': {
            'bucket': { 'name': 'test-bucket', 'arn': test_source_bucket_arn },
            'object': { 'key': 'top-level-test-file.csv' }
        },
        'userIdentity': { 'principalId': 'testuser' },
        'requestParameters': { 'sourceIPAddress': 'testipaddress'},
    }]
}

test_bad_file_path_event2 = {
    'Records': [{
        'eventTime': test_event_time, 
        's3': {
            'bucket': { 'name': 'test-bucket', 'arn': test_source_bucket_arn },
            'object': { 'key': 'test-source-system/first-level-test-file.csv' }
        },
        'userIdentity': { 'principalId': 'testuser' },
        'requestParameters': { 'sourceIPAddress': 'testipaddress'},
    }]
}

test_bad_folder_event = {
    'Records': [{
        'eventTime': test_event_time, 
        's3': {
            'bucket': { 'name': 'test-bucket', 'arn': test_source_bucket_arn },
            'object': { 'key': 'level1/level2/' }
        },
        'userIdentity': { 'principalId': 'testuser' },
        'requestParameters': { 'sourceIPAddress': 'testipaddress'},
    }]
}

test_context = {
    'aws_request_id': 'd7d36c0e-a1bc-11ed-a8fc-0242ac120002',
    'function_name': 'testfunction',
    'invoked_function_arn': 'arn:aws:lambda:::function:notrealarn',
    'identity': None,
    'client_context': None
}


def mock_record_etl_job_run(audit_table_name: str, item: dict):
    # Mock function does not need to do any work
    pass

@pytest.fixture
def use_moto_state():
    @mock_aws
    def sfn_client_and_state_machine_arn():
        sfn = boto3.client('stepfunctions')
        r = sfn.create_state_machine(
            name=test_state_machine_name,
            definition='{}',
            roleArn=test_state_machine_role_arn,
        )
        return r['stateMachineArn']
    return sfn_client_and_state_machine_arn

@pytest.fixture
def use_moto_table():
    @mock_aws
    def dynamodb_client_and_audit_table():
        dynamodb = boto3.resource('dynamodb')
 
        # KeySchema, AttributeDefinitions, BillingMode, and indexes
        # should match dynamodb table creation in dynamodb_stack
        table = dynamodb.create_table(
            TableName=test_table_name,
            KeySchema=[
                { 'AttributeName': 'execution_id', 'KeyType': 'HASH' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'execution_id', 'AttributeType': 'S' },
                { 'AttributeName': 'source_key', 'AttributeType': 'S' },
                { 'AttributeName': 'job_latest_status', 'AttributeType': 'S' },
                { 'AttributeName': 'job_start_date_int', 'AttributeType': 'S' },
                { 'AttributeName': 'dependency_key', 'AttributeType': 'S' }
            ],
            BillingMode='PAY_PER_REQUEST',
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'source_key-job_start_date_int-index',
                    'KeySchema': [
                        { 'AttributeName': 'source_key', 'KeyType': 'HASH' },
                        { 'AttributeName': 'job_start_date_int', 'KeyType': 'RANGE' }
                    ],
                    'Projection': { 'ProjectionType': 'ALL' },
                },
                {
                    'IndexName': 'job_latest_status-dependency_key-index',
                    'KeySchema': [
                        { 'AttributeName': 'job_latest_status', 'KeyType': 'HASH' },
                        { 'AttributeName': 'dependency_key', 'KeyType': 'RANGE' }
                    ],
                    'Projection': { 'ProjectionType': 'ALL' },
                }
            ]

        )
        return table
    return dynamodb_client_and_audit_table


def test_handler_success_returns_200(monkeypatch):
    monkeypatch.setattr(lambda_handler.boto3, 'client', mock_boto3_client)
    monkeypatch.setattr(lambda_handler, 'record_etl_job_run', mock_record_etl_job_run)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    monkeypatch.setenv('SFN_STATE_MACHINE_ARN', test_state_machine_arn)
    monkeypatch.setenv('GLUE_SCRIPTS_BUCKET_NAME', test_scripts_bucket)

    result = lambda_handler.lambda_handler(test_success_event, test_context)
    assert result['statusCode'] == 200


def test_handler_overrides_partitions(monkeypatch, caplog):
    monkeypatch.setattr(lambda_handler.boto3, 'client', mock_boto3_client)
    monkeypatch.setattr(lambda_handler, 'record_etl_job_run', mock_record_etl_job_run)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    monkeypatch.setenv('SFN_STATE_MACHINE_ARN', test_state_machine_arn)
    monkeypatch.setenv('GLUE_SCRIPTS_BUCKET_NAME', test_scripts_bucket)

    result = lambda_handler.lambda_handler(test_partition_override_event, test_context)
    assert result['statusCode'] == 200

    with caplog.at_level(logging.INFO):
        assert caplog.text.find('Date used for partitioning: myyear-mymonth-') != -1


def test_handler_bad_file_returns_400(monkeypatch):
    monkeypatch.setattr(lambda_handler.boto3, 'client', mock_boto3_client)
    monkeypatch.setattr(lambda_handler, 'record_etl_job_run', mock_record_etl_job_run)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    monkeypatch.setenv('SFN_STATE_MACHINE_ARN', test_state_machine_arn)
    monkeypatch.setenv('GLUE_SCRIPTS_BUCKET_NAME', test_scripts_bucket)

    result1 = lambda_handler.lambda_handler(test_bad_file_path_event1, test_context)
    assert result1['statusCode'] == 400

    result2 = lambda_handler.lambda_handler(test_bad_file_path_event2, test_context)
    assert result2['statusCode'] == 400


def test_handler_folder_putobject_returns_400(monkeypatch):
    monkeypatch.setattr(lambda_handler.boto3, 'client', mock_boto3_client)
    monkeypatch.setattr(lambda_handler, 'record_etl_job_run', mock_record_etl_job_run)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    monkeypatch.setenv('SFN_STATE_MACHINE_ARN', test_state_machine_arn)
    monkeypatch.setenv('GLUE_SCRIPTS_BUCKET_NAME', test_scripts_bucket)

    result = lambda_handler.lambda_handler(test_bad_folder_event, test_context)
    assert result['statusCode'] == 400


@mock_aws
def test_handler_dependency_workflow(monkeypatch, caplog, use_moto_state):
    monkeypatch.setattr(lambda_handler, 'record_etl_job_run', mock_record_etl_job_run)
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    monkeypatch.setenv('SFN_STATE_MACHINE_ARN', use_moto_state())
    monkeypatch.setenv('GLUE_SCRIPTS_BUCKET_NAME', test_scripts_bucket)

    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=test_scripts_bucket)

    s3_client.put_object(
        Bucket=test_scripts_bucket,
        Key='etl/transformation-spec/level1-level2-dependent.json',
        Body='{ "depends_on": { "anotherdb/anothertable": 1 }}')

    result = lambda_handler.lambda_handler(test_success_event, test_context)

    assert result['statusCode'] == 200
    with caplog.at_level(logging.INFO):
        assert caplog.text.find('Dependent workflow JSON') != -1


@mock_aws
def test_handler_dependency_workflow_with_loopback(monkeypatch, caplog, use_moto_table, use_moto_state):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    monkeypatch.setenv('SFN_STATE_MACHINE_ARN', use_moto_state())
    monkeypatch.setenv('GLUE_SCRIPTS_BUCKET_NAME', test_scripts_bucket)

    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=test_scripts_bucket)

    s3_client.put_object(
        Bucket=test_scripts_bucket,
        Key='etl/transformation-spec/level1-level2-dependent.json',
        Body='{ "depends_on": { "anotherdb/anothertable": 1 },'
            '"lookback": { "days": 30 } }')

    table = use_moto_table()
    ten_days_ago = datetime.now() - timedelta(days=10)
    item = {
        'execution_id': test_execution_id,
        'source_key': 'anotherdb/anothertable',
        'job_latest_status': 'SUCCEEDED',
        'job_start_date': ten_days_ago.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'job_start_date_int': int(ten_days_ago.timestamp()),
        'job_last_updated_timestamp': ten_days_ago.strftime('%Y-%m-%d %H:%M:%S.%f'),
    }
    table.put_item(Item=item)

    result = lambda_handler.lambda_handler(test_success_event, test_context)

    assert result['statusCode'] == 200
    with caplog.at_level(logging.INFO):
        assert caplog.text.find('Dependent workflow JSON') != -1 \
            and caplog.text.find('starting') != -1


@mock_aws
def test_handler_dependency_workflow_with_failed_loopback(monkeypatch, caplog, use_moto_table, use_moto_state):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    monkeypatch.setenv('SFN_STATE_MACHINE_ARN', use_moto_state())
    monkeypatch.setenv('GLUE_SCRIPTS_BUCKET_NAME', test_scripts_bucket)

    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=test_scripts_bucket)

    s3_client.put_object(
        Bucket=test_scripts_bucket,
        Key='etl/transformation-spec/level1-level2-dependent.json',
        Body=json.dumps({
            "depends_on": { "anotherdb/anothertable": 1 },
            "lookback": { "days": 10 }
        })
    )

    table = use_moto_table()
    twenty_days_ago = datetime.now() - timedelta(days=20)
    item = {
        'execution_id': test_execution_id,
        'source_key': 'anotherdb/anothertable',
        'job_latest_status': 'SUCCEEDED',
        'job_start_date': twenty_days_ago.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'job_start_date_int': int(twenty_days_ago.timestamp()),
        'job_last_updated_timestamp': twenty_days_ago.strftime('%Y-%m-%d %H:%M:%S.%f'),
    }
    table.put_item(Item=item)

    result = lambda_handler.lambda_handler(test_success_event, test_context)

    assert result['statusCode'] == 200
    with caplog.at_level(logging.INFO):
        assert caplog.text.find('Dependent workflow JSON') != -1 \
            and caplog.text.find('queuing') != -1


@mock_aws
def test_record_etl_job_run_records_status(monkeypatch,  use_moto_table, use_moto_state):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    table = use_moto_table()

    item = { 
        'execution_id': test_execution_id,
        'sfn_arn': test_state_machine_arn,
        'job_latest_status': 'STARTED'
    }
    lambda_handler.record_etl_job_run(test_table_name, item)

    item = table.get_item(
        TableName=test_table_name,
        Key={ 'execution_id': test_execution_id }
    )
    assert item['Item']['job_latest_status'] == 'STARTED'


@mock_aws
def test_record_etl_jon_run_logs_no_table(monkeypatch, caplog):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    # Purposely do not call use_moto_table() to create the table

    with pytest.raises(RuntimeError) as e_info:
        item = { 
            'execution_id': test_execution_id,
            'sfn_arn': test_state_machine_arn,
            'job_latest_status': 'STARTED'
        }
        lambda_handler.record_etl_job_run(test_table_name, item)

    assert e_info.match('ResourceNotFoundException'), 'Expected Boto3 Client Error not raised'