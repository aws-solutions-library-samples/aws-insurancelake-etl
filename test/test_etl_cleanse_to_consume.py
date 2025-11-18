# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import os
from urllib.parse import urlparse
import shutil
from test.boto_mocking_helper import *

try:
    from test.glue_job_mocking_helper import *
    from awsglue.utils import GlueArgumentError
    import lib.glue_scripts.etl_cleanse_to_consume as etl_cleanse_to_consume
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

mock_args = [
    'etl_cleanse-to-consume.py',
    '--JOB_NAME=UnitTest',
    '--JOB_RUN_ID=jr-unittest',
    f'--state_machine_name={mock_resource_prefix}-etl-state-machine',
    '--execution_id=test_execution_id',
    '--environment=unittest',
    f'--TempDir={mock_temp_bucket}',
    f'--txn_bucket={mock_scripts_bucket}',
    '--txn_sql_prefix_path=/etl/transformation-sql/',
    f'--source_bucket={mock_cleanse_bucket}',
    f'--target_bucket={mock_consume_bucket}',
    f'--source_key={mock_database_name}/{mock_table_name}',
    f'--source_database_name={mock_database_name}',
    f'--target_database_name={mock_database_name}_consume',
    f'--table_name={mock_table_name}',
    '--base_file_name=testfile.csv',
    '--p_year=2022',
    '--p_month=12',
    '--p_day=6',
    f'--dq_results_table={mock_resource_prefix}-dq-results-table',
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

mock_sql_view_file = """create view test_view as
    select
    1 as c1,
    2 as c2
    """

@mock_glue_job(etl_cleanse_to_consume)
def test_job_execution_and_commit(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', mock_args)
    etl_cleanse_to_consume.main()


@mock_glue_job(etl_cleanse_to_consume)
def test_consume_bucket_write(monkeypatch, capsys):
    mock_args_with_athena = mock_args.copy()
    mock_args_with_athena.extend(['--athena_workgroup=workgroup'])
    monkeypatch.setattr(sys, 'argv', mock_args_with_athena)
    file = write_local_file(
        f'{mock_scripts_bucket}/etl/transformation-sql',
        f'spark-{mock_database_name}-{mock_table_name}.sql',
        mock_spark_sql_file)

    consume_path = f'{mock_consume_bucket}/{mock_database_name}/{mock_table_name}'
    parsed_uri = urlparse(consume_path)
    shutil.rmtree(parsed_uri.path, ignore_errors=True)

    etl_cleanse_to_consume.main()
    os.remove(file)
    assert os.path.isdir(parsed_uri.path), \
        'Cleanse-to-Consume Glue job failed to write to Consume bucket with supplied Spark SQL'


@mock_glue_job(etl_cleanse_to_consume)
def test_missing_argument_exception(monkeypatch, capsys):
    """
    Check error is raised if base_file_name argument is missing
    """
    mock_args_without_base_file_name = mock_args.copy()
    mock_args_without_base_file_name.remove('--base_file_name=testfile.csv')
    monkeypatch.setattr(sys, 'argv', mock_args_without_base_file_name)
    with pytest.raises(GlueArgumentError):
        etl_cleanse_to_consume.main()


@mock_glue_job(etl_cleanse_to_consume)
def test_athena_view_create(monkeypatch, capsys):
    mock_args_with_athena = mock_args.copy()
    mock_args_with_athena.extend(['--athena_workgroup=workgroup'])
    monkeypatch.setattr(sys, 'argv', mock_args_with_athena)
    file = write_local_file(
        f'{mock_scripts_bucket}/etl/transformation-sql',
        f'athena-{mock_database_name}-{mock_table_name}.sql',
        mock_sql_view_file)

    etl_cleanse_to_consume.main()
    captured = capsys.readouterr()
    os.remove(file)
    assert 'Athena query execution status' in captured.out


@mock_glue_job(etl_cleanse_to_consume)
def test_redshift_view_create(monkeypatch, capsys):
    mock_args_with_redshift = mock_args.copy()
    mock_args_with_redshift.extend([
        '--redshift_workgroup_name=default',
        '--redshift_database=dev'])
    monkeypatch.setattr(sys, 'argv', mock_args_with_redshift)
    file = write_local_file(
        f'{mock_scripts_bucket}/etl/transformation-sql',
        f'redshift-{mock_database_name}-{mock_table_name}.sql',
        mock_sql_view_file)

    etl_cleanse_to_consume.main()
    captured = capsys.readouterr()
    os.remove(file)
    assert 'Redshift query execution status' in captured.out


@mock_glue_job(etl_cleanse_to_consume)
def test_missing_redshift_parameter(monkeypatch, capsys):
    """
    Check error is raised if redshift_database argument is missing
    """
    mock_args_with_redshift = mock_args.copy()
    mock_args_with_redshift.append('--redshift_workgroup_name=default')
    monkeypatch.setattr(sys, 'argv', mock_args_with_redshift)
    file = write_local_file(
        f'{mock_scripts_bucket}/etl/transformation-sql',
        f'redshift-{mock_database_name}-{mock_table_name}.sql',
        mock_sql_view_file)

    with pytest.raises(GlueArgumentError):
        etl_cleanse_to_consume.main()
    os.remove(file)


@mock_glue_job(etl_cleanse_to_consume)
def test_missing_all_redshift_parameters(monkeypatch, capsys):
    """
    Check error is raised if all redshift job arguments are missed
    """
    monkeypatch.setattr(sys, 'argv', mock_args)
    file = write_local_file(
        f'{mock_scripts_bucket}/etl/transformation-sql',
        f'redshift-{mock_database_name}-{mock_table_name}.sql',
        mock_sql_view_file)

    with pytest.raises(GlueArgumentError):
        etl_cleanse_to_consume.main()
    os.remove(file)