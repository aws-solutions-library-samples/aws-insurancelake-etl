# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
from urllib.parse import urlparse
import shutil
import json

try:
    from pyspark.sql.dataframe import DataFrame
    from awsglue.utils import GlueArgumentError
    from test.glue_job_mocking_helper import *
    import lib.glue_scripts.etl_consume_entity_match as etl_consume_entity_match
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

mock_args = [
    'etl_consume_entity_match.py',
    '--JOB_NAME=UnitTest',
    '--JOB_RUN_ID=jr-unittest',
    f'--state_machine_name={mock_resource_prefix}-etl-state-machine',
    '--execution_id=test_execution_id',
    '--environment=unittest',
    f'--TempDir=file:///tmp/{mock_resource_prefix}-temp',
    f'--txn_bucket={mock_scripts_bucket}',
    '--txn_spec_prefix_path=/etl/transformation-spec/',
    '--iceberg_catalog=local',
    f'--target_bucket={mock_consume_bucket}',
    f'--source_key={mock_database_name}/{mock_table_name}',
    f'--database_name_prefix={mock_database_name}',
    f'--table_name={mock_table_name}',
    '--p_year=2022',
    '--p_month=12',
    '--p_day=6',
]

mock_parsed_args = {
    'source_key': mock_database_name + '/' + mock_table_name,
}

mock_primary_entity_table = 'customer_primary'
mock_global_id_field = 'globalid'

mock_simple_spec_file = {
    'primary_entity_table': mock_primary_entity_table,
    'global_id_field': mock_global_id_field,
}

mock_table_data = [
    ( None, 'Customer1', 1 ),
    ( 'GlobalID2', 'Customer2', 1 ),
    ( 'GlobalID3', 'Customer3', 1 ),
    ( None, 'Customer4', 1 ),
    ( None, 'Customer5', 1 ),
]
mock_table_schema = f'{mock_global_id_field} string, name string, version int'


def mock_table_exists_false(database_name, table_name):
    return False

def mock_table_exists_true(database_name, table_name):
    return True


@mock_glue_job(etl_consume_entity_match)
def test_entity_primary_table_write(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', mock_args)
    monkeypatch.setattr(etl_consume_entity_match, 'table_exists', mock_table_exists_false)
    write_local_file(f'{mock_scripts_bucket}/etl/transformation-spec', f'{mock_database_name}-entitymatch.json', json.dumps(mock_simple_spec_file))

    # Iceberg file paths match the name and case of the catalog names
    consume_path = f'{mock_consume_bucket}/iceberg/' \
        f'{mock_database_name.lower()}_consume/{mock_primary_entity_table.lower()}'
    parsed_uri = urlparse(consume_path)
    shutil.rmtree(parsed_uri.path, ignore_errors=True)

    etl_consume_entity_match.main()
    assert os.path.isdir(parsed_uri.path), \
        'Entity Match Glue job failed to write primary entity table to Consume bucket'


@mock_glue_job(etl_consume_entity_match)
def test_missing_argument_exception(monkeypatch, capsys):
    """
    Check error is raised if table_name argument is missing
    """
    mock_args_without_base_file_name = mock_args.copy()
    mock_args_without_base_file_name.remove(f'--table_name={mock_table_name}')
    monkeypatch.setattr(sys, 'argv', mock_args_without_base_file_name)

    with pytest.raises(GlueArgumentError):
        etl_consume_entity_match.main()


def test_split_dataframe_splits_correctly():
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    matched_df, tomatch_df = etl_consume_entity_match.split_dataframe(df, mock_global_id_field)

    assert type(matched_df) == DataFrame
    assert matched_df.count() == 2
    assert matched_df.filter(f'{mock_global_id_field} is null').count() == 0, \
        'Some returned matched rows have no global ID assigned'

    assert type(tomatch_df) == DataFrame
    assert tomatch_df.count() == 3
    assert tomatch_df.filter(f'{mock_global_id_field} is not null').count() == 0, \
        'Some returned unmatched rows have a global ID assigned'


def test_fill_global_id_fills_nulls():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = etl_consume_entity_match.fill_global_id(df, mock_global_id_field, mock_parsed_args, lineage)

    assert df.count() == 5
    assert df.filter(f'{mock_global_id_field} is null').count() == 0, \
        'Some returned rows have no global ID assigned'


def test_fill_global_id_keeps_global_id_as_first_field():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = etl_consume_entity_match.fill_global_id(df, mock_global_id_field, mock_parsed_args, lineage)
    assert df.schema[0].name == mock_global_id_field