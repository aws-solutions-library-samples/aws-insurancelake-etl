# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import boto3
from moto import mock_aws
from test.boto_mocking_helper import *
from lib.glue_scripts.lib.dataquery import *

mock_database_name = 'TestGlueCatalogDatabase'
mock_table_name = 'TestGlueCatalogTable'
mock_resource_prefix = 'unittest-insurancelake'
mock_workgroup = f'{mock_resource_prefix}-workgroup'

mock_args = {
    'source_key': mock_database_name + '/' + mock_table_name,
}

mock_sql_file = """create view test_view as
    select
    1 as c1,
    2 as c2
    """

@mock_aws
def test_athena_execute_query_success(monkeypatch):
    """
    Test simple create view and expect success
    """
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)

    athena_client = boto3.client('athena', region_name=mock_region)
    athena_client.create_work_group(
        Name=mock_workgroup,
        Description='Test workgroup for unit tests',
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': 's3://test-bucket/results/'
            }
        }
    )

    result = athena_execute_query(mock_database_name, mock_sql_file, mock_workgroup)
    assert result == 'SUCCEEDED'

@mock_aws
def test_athena_execute_query_max_retries_error(monkeypatch):
    """
    Provide 0 attempts and expect failure from exceeding max attempts
    """
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setattr(sys, 'argv', mock_args)

    athena_client = boto3.client('athena', region_name=mock_region)
    athena_client.create_work_group(
        Name=mock_workgroup,
        Description='Test workgroup for unit tests',
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': 's3://test-bucket/results/'
            }
        }
    )

    with pytest.raises(RuntimeError) as e_info:
        athena_execute_query(mock_database_name, mock_sql_file, mock_workgroup, max_attempts = 0)
    assert e_info.match('exceeded max_attempts')

def test_athena_execute_query_fail(monkeypatch):
    """
    Test simple create view and expect failure
    Mock API directly because there is no way to generate a failure with moto's athena mock
    """
    monkeypatch.setattr(sys, 'argv', mock_args)
    monkeypatch.setattr(boto3, 'client', mock_boto3_client)
    with pytest.raises(RuntimeError) as e_info:
        athena_execute_query(mock_database_name, mock_sql_file, mock_workgroup)
    assert e_info.match('failed with query engine error')

def test_redshift_execute_query_success(monkeypatch):
    """
    Test simple create view and expect success
    Mock API directly because moto's redshift-data mock execute_statement() never returns
    FINISHED status
    """
    monkeypatch.setattr(sys, 'argv', mock_args)
    monkeypatch.setattr(boto3, 'client', mock_boto3_client)
    result = redshift_execute_query(mock_database_name, mock_sql_file, workgroup_name='test', max_attempts=1)
    # function will exceed maximum attempts because moto only starts query and never finishes
    assert result == 'FINISHED'

def test_redshift_execute_query_fail(monkeypatch):
    """
    Test simple create view and expect failure
    Mock API directly because moto's redshift-data mock execute_statement() only returns
    SUBMITTED status
    """
    monkeypatch.setattr(sys, 'argv', mock_args)
    monkeypatch.setattr(boto3, 'client', mock_boto3_client)
    # mock methods look for keyword undefined to return failure
    sql = mock_sql_file + ' from undefined'

    with pytest.raises(RuntimeError) as e_info:
        redshift_execute_query(mock_database_name, sql, workgroup_name='test', max_attempts=1)
    assert e_info.match('failed with error')