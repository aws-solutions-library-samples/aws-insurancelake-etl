# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
from moto import mock_aws
import boto3
import json
from lib.etl_job_auditor.lambda_handler import lambda_handler

mock_region = 'us-east-1'
test_table_name = 'test-etl-job-audit'
test_success_execution_id = '3b278a83-8b04-468a-983e-3749b6609dcb'
test_failure_execution_id = '884423e7-2dba-4597-9907-371d1f424e16'

state_machine_success_event = {
    'eventTime': '2023-01-02T23:00:00.000Z', 
    'Input': {
        'execution_id': test_success_execution_id, 
        'taskresult': {
    		'AllocatedCapacity': 50, 
            'Attempt': 0, 
            'CompletedOn': 1675701338733, 
            'ExecutionTime': 30, 
            'GlueVersion': '3.0', 
            'Id': 'jr_5ae791637444c02e3c26b8c97750446cbead7bf184494f2aaaf5be713e7e55f6', 
            'JobName': 'test_run_success', 
            'JobRunState': 'SUCCEEDED', 
            'LastModifiedOn': 1675701338733, 
            'LogGroupName': '/aws-glue/jobs', 
            'MaxCapacity': 50.0, 
            'NumberOfWorkers': 50, 
            'PredecessorRuns': [], 
            'StartedOn': 1675701301319, 
            'Timeout': 2880, 
            'WorkerType': 'G.1X'
        }
    }
}

state_machine_failure_event = {
    'eventTime': '2023-01-03T23:00:00.000Z', 
    'Input': {
        'execution_id': test_failure_execution_id, 
        'taskresult': {
            'Error': 'States.TaskFailed', 
            'Cause': json.dumps(
                {
                    "AllocatedCapacity": 50,
                    "Attempt": 0,
                    "CompletedOn": 1675434244707,
                    "DPUSeconds": 120.0,
                    "ErrorMessage": "RuntimeError: Error for testing",
                    "ExecutionTime": 24,
                    "GlueVersion": "3.0",
                    "Id": "jr_bde060a78592786933a57ade0a282f39eec026236b82ba3b678e8154592d666c",
                    "JobName": "test_run_failure",
                    "JobRunState": "FAILED",
                    "LastModifiedOn": 1675434244707,
                    "LogGroupName": "/aws-glue/jobs",
                    "MaxCapacity": 50.0,
                    "NumberOfWorkers": 50,
                    "PredecessorRuns": [],
                    "StartedOn": 1675434215855,
                    "Timeout": 2880,
                    "WorkerType":"G.1X"
                })
        }
    }
}

test_context = {
    'aws_request_id': 'd7d36c0e-a1bc-11ed-a8fc-0242ac120002',
    'function_name': 'testfunction',
    'invoked_function_arn': 'arn:aws:lambda:::function:notrealarn',
    'identity': None,
    'client_context': None
}

@pytest.fixture
def use_moto():
    @mock_aws
    def dynamodb_client_and_audit_table():
        dynamodb = boto3.resource('dynamodb')
 
        # KeySchema, AttributeDefinitions, and BillingMode should match
        # dynamodb table creation in dynamodb_stack
        table = dynamodb.create_table(
            TableName=test_table_name,
            KeySchema=[
                { 'AttributeName': 'execution_id', 'KeyType': 'HASH' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'execution_id', 'AttributeType': 'S' }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        return table
    return dynamodb_client_and_audit_table


@mock_aws
def test_handler_success_event_returns_200(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    use_moto()

    result = lambda_handler(state_machine_success_event, test_context)
    assert result['statusCode'] == 200


@mock_aws
def test_handler_success_event_returns_state(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    use_moto()

    result = lambda_handler(state_machine_success_event, test_context)
    assert result['body'].find('SUCCEEDED') != -1


@mock_aws
def test_handler_success_event_records_state(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    table = use_moto()

    lambda_handler(state_machine_success_event, test_context)

    item = table.get_item(
        TableName=test_table_name,
        Key={ 'execution_id': test_success_execution_id }
    )
    assert item['Item']['job_latest_status'] == 'SUCCEEDED'


@mock_aws
def test_handler_failure_event_returns_200(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    use_moto()

    result = lambda_handler(state_machine_failure_event, test_context)
    assert result['statusCode'] == 200


@mock_aws
def test_handler_failure_event_returns_state_and_error(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    use_moto()

    result = lambda_handler(state_machine_failure_event, test_context)
    assert result['body'].find('FAILED') != -1
    assert result['body'].find('Error for testing') != -1


@mock_aws
def test_handler_failure_event_records_error_message(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', test_table_name)
    table = use_moto()

    lambda_handler(state_machine_failure_event, test_context)

    item = table.get_item(
        TableName=test_table_name,
        Key={ 'execution_id': test_failure_execution_id }
    )
    assert item['Item']['error_message'] == 'RuntimeError: Error for testing'


@mock_aws
def test_wrong_table_returns_error(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setenv('DYNAMODB_TABLE_NAME', 'wrong_table_name')
    use_moto()

    with pytest.raises(RuntimeError) as e_info:
        lambda_handler(state_machine_success_event, test_context)
        assert e_info.match('Requested resource not found')