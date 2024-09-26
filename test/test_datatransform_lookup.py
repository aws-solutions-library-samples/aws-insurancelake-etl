# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import json
from moto import mock_aws

try:
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.datatransform_lookup import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')


mock_value_lookup_table = 'unittest-insurancelake-value-lookup-table'
mock_multi_lookup_table = 'unittest-insurancelake-multi-lookup-table'

mock_args = {
    'target_database_name': mock_database_name,
    'source_key': mock_database_name + '/' + mock_table_name,
    'value_lookup_table': mock_value_lookup_table,
    'multi_lookup_table': mock_multi_lookup_table,
}

mock_schema = [
    { 'Name': 'test_column_1', 'Type': 'date' },
    { 'Name': 'test_column_2', 'Type': 'string' },
    { 'Name': 'test_column_3', 'Type': 'int' },
]
mock_field = { 'Name': 'test_column', 'Type': 'string' }
mock_table_columns = [ 'id', 'date' ]
mock_table_data = [ ( 1, '1/1/2022' ), ( 2, '12/31/2022' ) ]
mock_table_schema = 'id int, date string'


@pytest.fixture
def dynamodb_table_for_lookup():
    @mock_aws
    def inner():
        dynamodb = boto3.resource('dynamodb')

        # KeySchema, AttributeDefinitions, and BillingMode should match
        # dynamodb table creation in dynamodb_stack
        table = dynamodb.create_table(
            TableName=mock_value_lookup_table,
            KeySchema=[
                { 'AttributeName': 'source_system', 'KeyType': 'HASH' },
                { 'AttributeName': 'column_name', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'source_system', 'AttributeType': 'S' },
                { 'AttributeName': 'column_name', 'AttributeType': 'S' }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        table.put_item(Item={
            'source_system': mock_database_name,
            'column_name': 'unittest',
            'lookup_data': json.dumps({ "1": "First", "2": "Second" })
        })
        return table
    return inner

@mock_aws
def test_transform_lookup_finds_match(monkeypatch, dynamodb_table_for_lookup):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    lineage = mock_lineage([])
    dynamodb_table_for_lookup()
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_lookup(df, [ { 'field': 'order', 'source': 'id', 'lookup': 'unittest' } ], mock_args, lineage, spark.sparkContext)
    assert 'order' in df.columns
    assert df.filter('`order` is null').count() == 0


@pytest.fixture
def dynamodb_table_for_multilookup():
    @mock_aws
    def inner():
        dynamodb = boto3.resource('dynamodb')
 
        # KeySchema, AttributeDefinitions, and BillingMode should match
        # dynamodb table creation in dynamodb_stack
        table = dynamodb.create_table(
            TableName=mock_multi_lookup_table,
            KeySchema=[
                { 'AttributeName': 'lookup_group', 'KeyType': 'HASH' },
                { 'AttributeName': 'lookup_item', 'KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                { 'AttributeName': 'lookup_group', 'AttributeType': 'S' },
                { 'AttributeName': 'lookup_item', 'AttributeType': 'S' }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        table.put_item(Item={
            'lookup_group': 'unittest-lookup',
            'lookup_item': '1-1/1/2022',
            'a': '1',
            'b': '2',
        })
        table.put_item(Item={
            'lookup_group': 'unittest-lookup',
            'lookup_item': '2-12/31/2022',
            'a': '3',
            'b': '4',
        })
        return table
    return inner

@mock_aws
def test_transform_multilookup_finds_matches(monkeypatch, dynamodb_table_for_multilookup):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    lineage = mock_lineage([])
    dynamodb_table_for_multilookup()
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_multilookup(
        df,
        [{
            'lookup_group': 'unittest-lookup',
            'match_columns': [ 'id', 'date' ],
            'return_attributes': [ 'a', 'b' ],
        }],
        mock_args, lineage, spark.sparkContext)
    assert 'a' in df.columns and 'b' in df.columns
    assert df.filter('`a` is null').count() == 0
    assert df.filter('`b` is null').count() == 0

@mock_aws
def test_transform_multilookup_nomatch_default(monkeypatch, dynamodb_table_for_multilookup):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    lineage = mock_lineage([])
    dynamodb_table_for_multilookup()
    # Test dataframe will have no matches with mock lookup items
    df = spark.createDataFrame([ ( 1, '1/1/2021' ), ( 2, '12/31/2021' ) ], schema=mock_table_schema)
    df = transform_multilookup(
        df,
        [{
            'lookup_group': 'unittest-lookup',
            'match_columns': [ 'id', 'date' ],
            'return_attributes': [ 'a', 'b' ],
            'nomatch': 'N/A'
        }],
        mock_args, lineage, spark.sparkContext)
    assert 'a' in df.columns and 'b' in df.columns
    assert df.filter("`a` = 'N/A'").count() == 2
    assert df.filter("`b` = 'N/A'").count() == 2

@mock_aws
def test_transform_multilookup_error_on_missing_lookup_data(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    lineage = mock_lineage([])
    # Create empty lookup table
    dynamodb = boto3.resource('dynamodb')
    dynamodb.create_table(
        TableName=mock_multi_lookup_table,
        KeySchema=[
            { 'AttributeName': 'lookup_group', 'KeyType': 'HASH' },
            { 'AttributeName': 'lookup_item', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
            { 'AttributeName': 'lookup_group', 'AttributeType': 'S' },
            { 'AttributeName': 'lookup_item', 'AttributeType': 'S' }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    # Test dataframe will have no matches with mock lookup items
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    with pytest.raises(RuntimeError) as e_info:
        df = transform_multilookup(
            df,
            [{
                'lookup_group': 'unittest-lookup',
                'match_columns': [ 'id', 'date' ],
                'return_attributes': [ 'a', 'b' ],
                'nomatch': 'N/A'
            }],
            mock_args, lineage, spark.sparkContext)
    e_info.match('lookup_group')

@mock_aws
def test_get_multilookup_data_paginates(monkeypatch, dynamodb_table_for_multilookup):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    dynamodb_table_for_multilookup()
    items = get_multilookup_data(
        mock_multi_lookup_table,
        'unittest-lookup',
        [ 'a', 'b' ],
        limit=1,
    )
    assert len(items) == 2