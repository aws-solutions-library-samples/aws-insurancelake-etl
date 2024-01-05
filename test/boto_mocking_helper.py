# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import datetime
from dateutil import tz

mock_account_id = 'notrealaccountid'
mock_region = 'us-east-1'

mock_athena_execute_query_response = {
    'QueryExecutionId': '27029f2d-83d9-4f74-9ec5-4c8a188a67d1',
    'ResponseMetadata': {
        'RequestId': 'c6dff4ba-ac1d-435b-bcb7-420e67ae8f9b',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Thu, 22 Sep 2022 14:29:30 GMT', 'x-amzn-requestid': 'c6dff4ba-ac1d-435b-bcb7-420e67ae8f9b', 'content-length': '59', 'connection': 'keep-alive'},
        'RetryAttempts': 0
    }}

mock_athena_get_query_execute_failed_response =  {
    'QueryExecution': {
        'QueryExecutionId': '308cb419-d6a9-4bcc-a01e-9efa702e6b94',
        'Query': 'CREATE OR REPLACE VIEW "test_view" AS \nSELECT\n  generatedate\nFROM\n  AwsDataCatalog.default.mytable',
        'StatementType': 'DDL',
        'ResultConfiguration': {
            'OutputLocation': 's3://fakebucket/results/308cb419-d6a9-4bcc-a01e-9efa702e6b94.txt'
        },
        'QueryExecutionContext': {'Database': 'mydb', 'Catalog': 'fakeaccount'},
        'Status': {
            'State': 'FAILED',
            'StateChangeReason': "line 3:3: Column 'generatedate' cannot be resolved",
            'SubmissionDateTime': datetime.datetime(2022, 9, 22, 15, 29, 51, 159000, tzinfo=tz.tzlocal()),
            'CompletionDateTime': datetime.datetime(2022, 9, 22, 15, 29, 51, 535000, tzinfo=tz.tzlocal())
        },
        'Statistics': {'EngineExecutionTimeInMillis': 246, 'DataScannedInBytes': 0, 'TotalExecutionTimeInMillis': 376, 'QueryQueueTimeInMillis': 102, 'ServiceProcessingTimeInMillis': 28},
        'WorkGroup': 'primary',
		'EngineVersion': {'SelectedEngineVersion': 'AUTO', 'EffectiveEngineVersion': 'Athena engine version 2'}
    },
    'ResponseMetadata': {
        'RequestId': 'ef15ebbf-2482-49ef-ae2d-9397e5efdaaa',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'date': 'Thu, 22 Sep 2022 15:29:51 GMT', 'x-amzn-requestid': 'ef15ebbf-2482-49ef-ae2d-9397e5efdaaa', 'content-length': '1961', 'connection': 'keep-alive'},
        'RetryAttempts': 0
    }}


class mock_client_sts:

    @staticmethod
    def get_caller_identity():
        return { 'Account': mock_account_id }

class mock_client_sfn:

    @staticmethod
    def start_execution(stateMachineArn: str, name: str, input: str):
        return {
            'executionArn': f'arn:aws:states:{mock_region}:{mock_account_id}:execution:test-state-machine:{name}-plusuniqueid',
            'startDate': '2023-01-01 23:00:00',
        }

# There's no way to use moto's athena mock to cause a failed query
class mock_client_athena:
    @staticmethod
    def start_query_execution(QueryExecutionContext: dict, QueryString: str, ResultConfiguration: dict):
        return mock_athena_execute_query_response

    @staticmethod
    def get_query_execution(QueryExecutionId: str):
        return mock_athena_get_query_execute_failed_response

class mock_client_s3:
    @staticmethod
    def list_objects_v2(Bucket: str, Prefix: str):
        response = {
                'Name': Bucket,
                'Prefix': Prefix,
            }
        if 'does-not-exist' in Prefix:
            return response
        else:
            response.update({ 'Contents': [{'Key': 'found.json' }] })
            return response

def mock_boto3_client(client: str):
    if client == 'sts':
        return mock_client_sts
    elif client == 'stepfunctions':
        return mock_client_sfn
    elif client == 'athena':
        return mock_client_athena
    elif client == 's3':
        return mock_client_s3
    else:
        raise RuntimeError(f'boto3 client {client} requested from mock but not implemented')