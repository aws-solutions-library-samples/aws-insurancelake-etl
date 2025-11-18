# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from datetime import datetime
from dateutil import tz

mock_account_id = 'notrealaccountid'
mock_region = 'us-east-1'

mock_response_metadata = {'ResponseMetadata': {
        'RequestId': '1a6e0fba-e7cc-446b-a800-f72e66c8a993',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'x-amzn-requestid': '1a6e0fba-e7cc-446b-a800-f72e66c8a993',
            'content-type': 'application/x-amz-json-1.1',
            'content-length': '149', 'date': 'Tue, 14 Jan 2022 02:42:08 GMT'
        },
        'RetryAttempts': 0
    }}


mock_athena_execute_query_response = {
    'QueryExecutionId': '27029f2d-83d9-4f74-9ec5-4c8a188a67d1',
} | mock_response_metadata

mock_athena_get_query_execute_failed_response =  {
    'QueryExecution': {
        'QueryExecutionId': '308cb419-d6a9-4bcc-a01e-9efa702e6b94',
        'Query': 'CREATE OR REPLACE VIEW "test_view" AS \nSELECT\n  generatedate\nFROM\n  AwsDataCatalog.default.mytable',
        'StatementType': 'DDL',
        'ResultConfiguration': {
            'OutputLocation': 's3://fakebucket/results/308cb419-d6a9-4bcc-a01e-9efa702e6b94.txt'
        },
        'QueryExecutionContext': {'Database': 'mydb', 'Catalog': mock_account_id},
        'Status': {
            'State': 'FAILED',
            'StateChangeReason': "line 3:3: Column 'generatedate' cannot be resolved",
            'SubmissionDateTime': datetime(2022, 9, 22, 15, 29, 51, 159000, tzinfo=tz.tzlocal()),
            'CompletionDateTime': datetime(2022, 9, 22, 15, 29, 51, 535000, tzinfo=tz.tzlocal())
        },
        'Statistics': {'EngineExecutionTimeInMillis': 246, 'DataScannedInBytes': 0, 'TotalExecutionTimeInMillis': 376, 'QueryQueueTimeInMillis': 102, 'ServiceProcessingTimeInMillis': 28},
        'WorkGroup': 'primary',
		'EngineVersion': {'SelectedEngineVersion': 'AUTO', 'EffectiveEngineVersion': 'Athena engine version 2'}
    }
} | mock_response_metadata


mock_redshift_execute_statement_response = {
    'CreatedAt': datetime(2022, 1, 13, 21, 42, 8, 544000, tzinfo=tz.tzlocal()),
    'Database': 'dev',
    'DbUser': 'IAMR:TestUser',
    'Id': '1a6e0fba-e7cc-446b-a800-f72e66c8a993',
    'WorkgroupName': 'default-workgroup',
} | mock_response_metadata

mock_redshift_describe_statement_response = {
    'CreatedAt': datetime(2022, 1, 13, 20, 37, 6, 837000, tzinfo=tz.tzlocal()),
    'Database': 'dev',
    'DbUser': 'IAMR:TestUser',
    'Duration': 553161143,
    'HasResultSet': False,
    'Id': '1a6e0fba-e7cc-446b-a800-f72e66c8a993',
    'QueryString': 'CREATE VIEW test_view AS SELECT 3 as c3, 4 as c4;',
    'RedshiftPid': 1073971636,
    'RedshiftQueryId': 0,
    'ResultFormat': 'json',
    'ResultRows': 0,
    'ResultSize': 0,
    'Status': 'FINISHED',
    'UpdatedAt': datetime(2022, 1, 13, 20, 37, 7, 781000, tzinfo=tz.tzlocal()),
    'WorkgroupName': 'default-workgroup',
} | mock_response_metadata

mock_redshift_failed_statement_id = 'edd703ee-7314-4a24-bcf3-059a69b46254'

mock_redshift_describe_statement_failed_response = {
    'CreatedAt': datetime(2022, 1, 13, 20, 11, 11, 451000, tzinfo=tz.tzlocal()),
    'Database': 'dev',
    'DbUser': 'IAMR:TestUser',
    'Duration': -1,
    'Error': 'ERROR: relation "undefined" does not exist',
    'HasResultSet': False,
    'Id': mock_redshift_failed_statement_id,
    'QueryString': 'CREATE VIEW test_view AS SELECT 3 as c3, 4 as c4 FROM undefined;',
    'RedshiftPid': 1073889403,
    'RedshiftQueryId': 0,
    'ResultFormat': 'json',
    'ResultRows': -1,
    'ResultSize': -1,
    'Status': 'FAILED',
    'UpdatedAt': datetime(2022, 1, 13, 20, 11, 12, 18000, tzinfo=tz.tzlocal()),
    'WorkgroupName': 'default-workgroup',
} | mock_response_metadata


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
    def start_query_execution(QueryExecutionContext: dict, QueryString: str, WorkGroup: str):
        return mock_athena_execute_query_response

    @staticmethod
    def get_query_execution(QueryExecutionId: str):
        return mock_athena_get_query_execute_failed_response

# moto's redshift-data mock only returns statements in SUBMITTED status
class mock_client_redshiftdata:
    @staticmethod

    def execute_statement(Database: str, Sql: str, ClusterIdentifier: str = None, WorkgroupName: str = None):
        if 'undefined' in Sql:
            response = mock_redshift_execute_statement_response
            return response | { 'Id': mock_redshift_failed_statement_id }
        return mock_redshift_execute_statement_response

    @staticmethod
    def describe_statement(Id: str):
        if Id == mock_redshift_failed_statement_id:
            return mock_redshift_describe_statement_failed_response
        return mock_redshift_describe_statement_response

class mock_client_s3:
    class exceptions:
        class NoSuchKey(BaseException):
            pass

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

    @staticmethod
    def get_object(Bucket: str, Key: str):
        raise mock_client_s3.exceptions.NoSuchKey

def mock_boto3_client(client: str):
    if client == 'sts':
        return mock_client_sts
    elif client == 'stepfunctions':
        return mock_client_sfn
    elif client == 'athena':
        return mock_client_athena
    elif client == 'redshift-data':
        return mock_client_redshiftdata
    elif client == 's3':
        return mock_client_s3
    else:
        raise RuntimeError(f'boto3 client {client} requested from mock but not implemented')