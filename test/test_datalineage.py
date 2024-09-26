# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import boto3
from moto import mock_aws

try:
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.datalineage import DataLineageGenerator
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

test_table_name = 'test-data-lineage'

mock_args = {
    'JOB_NAME': 'UnitTest',
    'JOB_RUN_ID': 'jr_unittest',
    'state_machine_name': f'{mock_resource_prefix}-etl-state-machine',
    'execution_id': 'test_execution_id',
    'data_lineage_table': test_table_name,
    'source_key': mock_database_name + '/' + mock_table_name,
}

mock_args_no_table = {
    'JOB_NAME': 'UnitTest',
    'JOB_RUN_ID': 'jr_unittest',
    'state_machine_name': f'{mock_resource_prefix}-etl-state-machine',
    'execution_id': 'test_execution_id',
    'source_key': mock_database_name + '/' + mock_table_name,
}

mock_table_columns = [ 'id', 'date' ]
mock_table_data = [ ( 1, '1/1/2022' ), ( 2, '12/31/2022' ) ]


@pytest.fixture
def use_moto():
    @mock_aws
    def dynamodb_client_and_lineage_table():
        dynamodb = boto3.resource('dynamodb')
 
        # KeySchema, AttributeDefinitions, and BillingMode should match
        # dynamodb table creation in dynamodb_stack
        table = dynamodb.create_table(
            TableName=test_table_name,
            KeySchema=[
                { 'AttributeName': 'step_function_execution_id', 'KeyType': 'HASH' },
                { 'AttributeName': 'job_id_operation_seq', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'step_function_execution_id', 'AttributeType': 'S' },
                { 'AttributeName': 'job_id_operation_seq', 'AttributeType': 'S' }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        return table
    return dynamodb_client_and_lineage_table


def test_lineage_is_skipped_when_no_table():
    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)
    lineage = DataLineageGenerator(mock_args_no_table)
    lineage.update_lineage(df, 'TestDb/TestTable', 'read')


@mock_aws
def test_update_lineage_writes_to_table(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    lineage_table = use_moto()

    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)
    lineage = DataLineageGenerator(mock_args)
    lineage.update_lineage(df, 'TestDb/TestTable', 'read')

    item = lineage_table.get_item(
        Key={
            'step_function_execution_id': 'test_execution_id',
            'job_id_operation_seq': 'jr_unittest-1'
        }
    )
    assert item['Item']['dataset'] == 'TestDb/TestTable'


@mock_aws
def test_update_lineage_handles_any_transform(monkeypatch, use_moto):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    lineage_table = use_moto()

    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)
    lineage = DataLineageGenerator(mock_args)
    lineage.update_lineage(df, 'TestDb/TestTable', 'anytransform',
        transform=[
            { 'field': 'id' },
            { 'field': 'date' }
        ]
    )

    item = lineage_table.get_item(
        Key={
            'step_function_execution_id': 'test_execution_id',
            'job_id_operation_seq': 'jr_unittest-1'
        }
    )
    assert item['Item']['dataset'] == 'TestDb/TestTable'
    assert item['Item']['dataset_operation'] == 'anytransform'