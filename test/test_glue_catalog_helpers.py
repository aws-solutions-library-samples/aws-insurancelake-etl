# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import boto3
from moto import mock_aws

try:
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.glue_catalog_helpers import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

mock_schema = [
    { 'Name': 'test_column_1', 'Type': 'date' },
    { 'Name': 'test_column_2', 'Type': 'string' },
    { 'Name': 'test_column_3', 'Type': 'int' },
]
mock_new_field = { 'Name': 'test_column', 'Type': 'string' }
mock_int_to_bigint_field = { 'Name': 'test_column_3', 'Type': 'bigint' }
mock_date_to_timestamp_field = { 'Name': 'test_column_1', 'Type': 'timestamp' }
mock_date_to_string_field = { 'Name': 'test_column_1', 'Type': 'string' }
mock_string_to_int_field = { 'Name': 'test_column_2', 'Type': 'int' }
mock_decimal1_field = { 'Name': 'test_column_3', 'Type': 'decimal(10,6)' }
mock_decimal2_field = { 'Name': 'test_column_3', 'Type': 'decimal(11,8)' }
mock_table_columns = [ 'id', 'date' ]
mock_table_data = [ ( 1, '1/1/2022' ), ( 2, '12/31/2022' ) ]


@mock_aws
def test_table_exists_returns_dict_when_table_exists(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    glue_client.create_table(
        DatabaseName=mock_database_name,
        TableInput={ 'Name': mock_table_name }
    )
    assert isinstance(table_exists(mock_database_name, mock_table_name), dict)

@mock_aws
def test_table_exists_false(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    # Purposely do not create the table
    assert not table_exists(mock_database_name, mock_table_name)


@mock_aws
def test_create_database(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    create_database(mock_database_name, mock_database_uri)
    database_detail = glue_client.get_database(Name=mock_database_name)
    assert 'Database' in database_detail
    assert database_detail['Database'].get('LocationUri') == mock_database_uri

@mock_aws
def test_create_database_with_description(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    create_database(mock_database_name, mock_database_uri, 'Test Description')
    database_detail = glue_client.get_database(Name=mock_database_name)
    assert 'Database' in database_detail
    assert database_detail['Database'].get('Description') == 'Test Description'


def test_check_schema_change_permissive():
    new_schema = mock_schema.copy()
    new_schema.append(mock_new_field)
    assert check_schema_change(mock_schema, new_schema, 'permissive')

def test_check_schema_change_strict_same():
    assert check_schema_change(mock_schema, mock_schema, 'strict')

def test_check_schema_change_strict_different():
    new_schema = mock_schema.copy()
    new_schema.append(mock_new_field)
    assert not check_schema_change(mock_schema, new_schema, 'strict')

def test_check_schema_change_reorder_same():
    new_schema = mock_schema.copy()
    assert check_schema_change(mock_schema, new_schema, 'reorder')

def test_check_schema_change_reorder_reordered():
    new_schema = list(reversed(mock_schema.copy()))
    assert check_schema_change(mock_schema, new_schema, 'reorder')

def test_check_schema_change_reorder_nonpermissible():
    new_schema = mock_schema.copy()
    new_schema.append(mock_new_field)
    assert not check_schema_change(mock_schema, new_schema, 'reorder')

def test_check_schema_change_evolve_fails_field_removal():
    new_schema = mock_schema.copy()
    new_schema.remove({ 'Name': 'test_column_3', 'Type': 'int' })
    assert not check_schema_change(mock_schema, new_schema, 'evolve')

def test_check_schema_change_evolve_allows_reordered():
    new_schema = list(reversed(mock_schema.copy()))
    assert check_schema_change(mock_schema, new_schema, 'evolve')

def test_check_schema_change_evolve_allows_new_field():
    new_schema = mock_schema.copy()
    new_schema.append(mock_new_field)
    assert check_schema_change(mock_schema, new_schema, 'evolve')

def test_check_schema_change_evolve_allows_date_to_timestamp():
    new_schema = mock_schema.copy()
    new_schema[0] = mock_date_to_timestamp_field
    assert check_schema_change(mock_schema, new_schema, 'evolve')

def test_check_schema_change_evolve_fails_date_to_string():
    new_schema = mock_schema.copy()
    new_schema[0] = mock_date_to_string_field
    assert not check_schema_change(mock_schema, new_schema, 'evolve')

def test_check_schema_change_evolve_allows_int_to_bigint():
    new_schema = mock_schema.copy()
    new_schema[2] = mock_int_to_bigint_field
    assert check_schema_change(mock_schema, new_schema, 'evolve')

def test_check_schema_change_evolve_allows_string_to_int():
    new_schema = mock_schema.copy()
    new_schema[1] = mock_string_to_int_field
    assert check_schema_change(mock_schema, new_schema, 'evolve')

def test_check_schema_change_evolve_allows_decimal_to_decimal():
    old_schema = mock_schema.copy()
    new_schema = mock_schema.copy()
    old_schema[2] = mock_decimal1_field
    new_schema[2] = mock_decimal2_field
    assert check_schema_change(old_schema, new_schema, 'evolve')
    assert not check_schema_change(new_schema, old_schema, 'evolve')    # decimal can only get bigger


@mock_aws
def test_upsert_catalog_table_insert(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    # create_database tests will test that function, so skip it by creating db now
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)

    upsert_catalog_table(df, mock_database_name, mock_table_name, [], 's3://fake-bucket', 'strict')

    assert glue_client.get_table(DatabaseName=mock_database_name, Name=mock_table_name)

@mock_aws
def test_upsert_catalog_table_update(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    glue_client.create_table(
        DatabaseName=mock_database_name,
        TableInput={
            'Name': mock_table_name,
            'StorageDescriptor': { 'Columns': mock_schema }
        }
    )

    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)
    upsert_catalog_table(df, mock_database_name, mock_table_name, [],
        's3://fake-bucket', allow_schema_change='permissive')

    df_schema = [ { 'Name': field_name, 'Type': field_type } for field_name, field_type in df.dtypes ]
    response = glue_client.get_table(DatabaseName=mock_database_name, Name=mock_table_name)

    for field in df_schema:
        assert field in response['Table']['StorageDescriptor']['Columns']

@mock_aws
def test_upsert_catalog_table_update_raises_error(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    glue_client = boto3.client('glue')
    glue_client.create_database(DatabaseInput={ 'Name': mock_database_name })
    glue_client.create_table(
        DatabaseName=mock_database_name,
        TableInput={
            'Name': mock_table_name,
            'StorageDescriptor': { 'Columns': mock_schema }
        }
    )

    df = spark.createDataFrame(mock_table_data).toDF(*mock_table_columns)

    with pytest.raises(RuntimeError) as e_info:
        upsert_catalog_table(df, mock_database_name, mock_table_name, [], 's3://fake-bucket', 'strict')
    assert e_info.match('Nonpermissible')


def test_gluecatalogdecimal_init():
    test_decimal = GlueCatalogDecimal('decimal(10,6)')
    assert test_decimal.precision == 10
    assert test_decimal.scale == 6

def test_gluecatalogdecimal_gt():
    test_decimal = GlueCatalogDecimal('decimal(10,6)')
    test_other_decimal = GlueCatalogDecimal('decimal(10,2)')
    assert not test_decimal > GlueCatalogDecimal('decimal(10,6)')  # equal, not greater
    assert test_decimal > test_other_decimal    # greater than should be true
    assert not test_other_decimal > test_decimal    # flipping around should be false

def test_gluecatalogdecimal_mixed():
    test_decimal = GlueCatalogDecimal('decimal(10,6)')
    test_other_decimal = GlueCatalogDecimal('decimal(11,2)')
    assert not test_other_decimal > test_decimal    # scale is not greater