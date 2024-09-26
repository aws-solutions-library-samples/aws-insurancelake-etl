# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys

try:
    from pyspark.sql.functions import length
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.datatransform_dataprotection import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

mock_hash_value_table = 'unittest-insurancelake-hash-value-table'

mock_args = {
    'execution_id': 'test_execution_id',
    'source_key': mock_database_name + '/' + mock_table_name,
    'hash_value_table': mock_hash_value_table,
}

mock_table_data = [ ( 1, '2022-01-01' ), ( 2, '2022-12-31' ), ( 3, '0000-00-00') ]
mock_table_schema = 'id int, date string'

def test_hash_transform_replaces_column_with_sha256_hash():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_hash(df, [ 'id' ], mock_args, lineage)

    assert 'id' in df.columns
    assert df.filter('`id` is null').count() == 0
    assert df.filter(length('id') == 64).count() == 3


def test_redact_transform_replaces_column():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_redact(df, { 'date': None }, mock_args, lineage)

    assert 'date' in df.columns
    assert df.filter("`date` is null").count() == 3

def test_redact_transform_throws_error_if_column_not_found():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    with pytest.raises(Exception) as e:
        transform_redact(df, { 'not_found': None }, mock_args, lineage)

    assert 'not found in incoming data' in str(e.value)


def mock_write_dynamic_frame_from_options(
    self,
    frame: any,
    connection_type: str,
    connection_options: dict = {},
    transformation_ctx: str = None
):
    # Not implemented for tests
    pass

def test_transform_tokenize_replaces_column_with_sha256_hash(monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', mock_region)
    monkeypatch.setattr(GlueContext, 'write_dynamic_frame_from_options',
        mock_write_dynamic_frame_from_options)
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_tokenize(df, [ 'id' ], mock_args, lineage, spark.sparkContext)

    assert 'id' in df.columns
    assert df.filter('`id` is null').count() == 0
    assert df.filter(length('id') == 64).count() == 3