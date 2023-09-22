# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import boto3
from moto import mock_glue

try:
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.glue_catalog_helpers import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No pySpark environment found')

mock_schema = [
    { 'Name': 'test_column_1', 'Type': 'date' },
    { 'Name': 'test_column_2', 'Type': 'string' },
    { 'Name': 'test_column_3', 'Type': 'int' },
]
mock_field = { 'Name': 'test_column', 'Type': 'string' }
mock_table_columns = [ 'id', 'date' ]
mock_table_data = [ ( 1, '1/1/2022' ), ( 2, '12/31/2022' ) ]


@mock_glue
def test_table_exists_true(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    glue_client.create_table(
        DatabaseName=mock_database_name,
        TableInput={ 'Name': mock_table_name }
    )
    assert table_exists(mock_database_name, mock_table_name) == True

@mock_glue
def test_table_exists_false(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    # Purposely do not create the table
    assert table_exists(mock_database_name, mock_table_name) == False

@mock_glue
def test_create_database(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    create_database(mock_database_name)
    assert glue_client.get_database(Name=mock_database_name)

def test_check_schema_change_permissive():
    new_schema = mock_schema.copy()
    new_schema.append(mock_field)
    assert check_schema_change(mock_schema, new_schema, 'permissive')

def test_check_schema_change_strict_same():
    assert check_schema_change(mock_schema, mock_schema, 'strict')

def test_check_schema_change_strict_different():
    new_schema = mock_schema.copy()
    new_schema.append(mock_field)
    assert not check_schema_change(mock_schema, new_schema, 'strict')

def test_check_schema_change_reorder_same():
    new_schema = mock_schema.copy()
    assert check_schema_change(mock_schema, new_schema, 'reorder')

def test_check_schema_change_reorder_reordered():
    new_schema = list(reversed(mock_schema.copy()))
    assert check_schema_change(mock_schema, new_schema, 'reorder')

def test_check_schema_change_reorder_nonpermissible():
    new_schema = mock_schema.copy()
    new_schema.append(mock_field)
    assert not check_schema_change(mock_schema, new_schema, 'reorder')

@mock_glue
def test_upsert_catalog_table_insert(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    # create_database tests will test that function, so skip it by creating db now
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)

    upsert_catalog_table(df, mock_database_name, mock_table_name, [], 's3://fake-bucket', 'strict')

    assert glue_client.get_table(DatabaseName=mock_database_name, Name=mock_table_name)

@mock_glue
def test_upsert_catalog_table_update(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    glue_client.create_table(
        DatabaseName=mock_database_name,
        TableInput={
            'Name': mock_table_name,
            'StorageDescriptor': { 'Columns': [ mock_field ] }
        }
    )

    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)
    upsert_catalog_table(df, mock_database_name, mock_table_name, [], 's3://fake-bucket', 'permissive')

    df_schema = [ { 'Name': field[0], 'Type': field[1] } for field in df.dtypes ]
    response = glue_client.get_table(DatabaseName=mock_database_name, Name=mock_table_name)

    for field in df_schema:
        assert field in response['Table']['StorageDescriptor']['Columns']

@mock_glue
def test_upsert_catalog_table_update_raises_error(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    glue_client.create_table(
        DatabaseName=mock_database_name,
        TableInput={
            'Name': mock_table_name,
            'StorageDescriptor': { 'Columns': [ mock_field ] }
        }
    )

    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)

    with pytest.raises(RuntimeError) as e_info:
        upsert_catalog_table(df, mock_database_name, mock_table_name, [], 's3://fake-bucket', 'strict')
    assert e_info.match('Nonpermissible')