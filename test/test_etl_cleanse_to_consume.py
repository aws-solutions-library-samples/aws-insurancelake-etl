# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
from urllib.parse import urlparse
import shutil
from moto import mock_athena
from test.boto_mocking_helper import *

try:
    from test.glue_job_mocking_helper import *
    from awsglue.utils import GlueArgumentError
    import lib.glue_scripts.etl_cleanse_to_consume as etl_cleanse_to_consume
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No pySpark environment found')

mock_args = [
    'etl_cleanse-to-consume.py',
    '--JOB_NAME=UnitTest',
    f'--state_machine_name={mock_resource_prefix}-etl-state-machine',
    '--execution_id=test_execution_id',
    '--environment=unittest',
    f'--TempDir={mock_temp_bucket}',
    f'--txn_bucket={mock_scripts_bucket}',
    '--txn_sql_prefix_path=/etl/transformation-sql/',
    f'--source_bucket={mock_cleanse_bucket}',
    f'--target_bucket={mock_consume_bucket}',
    f'--database_name_prefix={mock_database_name}',
    f'--table_name={mock_table_name}',
    '--base_file_name=testfile.csv',
    '--p_year=2022',
    '--p_month=12',
    '--p_day=6',
]

mock_spark_sql_file = """select
    1 as year,
    2 as month,
    3 as day,
    1234 as data
    """

mock_create_table_spark_sql_file = """create table differenttable as
    select
    1 as year,
    2 as month,
    3 as day,
    1234 as data
    """

mock_athena_sql_file = """create view test_view as
    select
    1 as c1,
    2 as c2
    """


@mock_glue_job(etl_cleanse_to_consume)
def test_job_execution_and_commit(monkeypatch):
    monkeypatch.setattr(sys, 'argv', mock_args)
    etl_cleanse_to_consume.main()


@mock_glue_job(etl_cleanse_to_consume)
def test_consume_bucket_write(monkeypatch):
    monkeypatch.setattr(sys, 'argv', mock_args)
    write_local_file(f'{mock_scripts_bucket}/etl/transformation-sql', f'spark-{mock_database_name}-{mock_table_name}.sql', mock_spark_sql_file)

    consume_path = f'{mock_consume_bucket}/{mock_database_name}/{mock_table_name}'
    parsed_uri = urlparse(consume_path)
    shutil.rmtree(parsed_uri.path, ignore_errors=True)

    etl_cleanse_to_consume.main()
    assert os.path.isdir(parsed_uri.path), \
        'Cleanse-to-Consume Glue job failed to write to Consume bucket with supplied Spark SQL'


@mock_glue_job(etl_cleanse_to_consume)
def test_missing_argument_exception(monkeypatch):
    """
    Check error is raised if base_file_name argument is missing
    """
    mock_args_without_base_file_name = mock_args.copy()
    mock_args_without_base_file_name.remove('--base_file_name=testfile.csv')
    monkeypatch.setattr(sys, 'argv', mock_args_without_base_file_name)
    with pytest.raises(GlueArgumentError):
        etl_cleanse_to_consume.main()


@mock_athena()
def test_athena_execute_query_success(monkeypatch):
    """
    Test simple create view and expect success
    """
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    result = etl_cleanse_to_consume.athena_execute_query(mock_database_name, mock_athena_sql_file, mock_temp_bucket)
    assert result == 'SUCCEEDED'


@mock_athena
def test_athena_execute_query_max_retries_error(monkeypatch):
    """
    Provide 0 attempts and expect failure from exceeding max attempts
    """
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setattr(sys, 'argv', mock_args)
    with pytest.raises(RuntimeError) as e_info:
        etl_cleanse_to_consume.athena_execute_query(mock_database_name, mock_athena_sql_file, mock_temp_bucket, max_attempts = 0)
    assert e_info.match('exceeded max_attempts')


def test_athena_execute_query_fail(monkeypatch):
    """
    Test simple create table and expect failure
    Mock API directly because there is no way to generate a failure with moto's athena mock
    """
    monkeypatch.setattr(sys, 'argv', mock_args)
    monkeypatch.setattr(etl_cleanse_to_consume.boto3, 'client', mock_boto3_client)
    with pytest.raises(RuntimeError) as e_info:
        etl_cleanse_to_consume.athena_execute_query(mock_database_name, mock_athena_sql_file, mock_temp_bucket)
    assert e_info.match('failed with query engine error')