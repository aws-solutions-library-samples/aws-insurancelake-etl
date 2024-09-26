# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys

try:
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.datatransform_misc import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')


mock_args = {
    'source_key': mock_database_name + '/' + mock_table_name,
}

mock_table_data = [
    ( 1, 'policyline1', 'coverageA', 5000 ),
    ( 2, None, 'coverageB', 4000 ),
    ( 3, None, 'coverageC', 3000 ),
    ( 4, 'policyline2', 'coverageB', 2000 ),
    ( 5, None, 'coverageC', 1000 ),
]
mock_table_data_merge = [
    ( 1, 'policyline1', 'coverageA', 1000 ),
    ( 2, None, None, 1000 ),
    ( 3, '', 'coverageB', 1000 ),
    ( 4, '', '', 1000 ),
]
mock_table_schema = 'id int, policyline string, coveragedetail string, amount int'


def test_transform_filldown_fills_one_column():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_filldown(df, [ { 'field': 'policyline' } ], mock_args, lineage)
    assert 'policyline' in df.columns
    assert df.filter('`policyline` is null').count() == 0
    assert df.filter('`policyline` = "policyline1"').count() == 3
    assert df.filter('`policyline` = "policyline2"').count() == 2


def test_transform_rownumber_adds_number_to_every_row():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_rownumber(df, [ { 'field': 'row_number' } ], mock_args, lineage)
    assert 'row_number' in df.columns
    assert df.filter('`row_number` is null').count() == 0
    assert df.agg({'row_number': 'min'}).first()['min(row_number)'] == 1
    assert df.agg({'row_number': 'max'}).first()['max(row_number)'] == 5

def test_transform_rownumber_numbers_over_partition():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)

    # Cleanup the mock data
    df = transform_filldown(df, [ { 'field': 'policyline' } ], mock_args, lineage)

    # Call the function to test
    df = transform_rownumber(df, [ { 'field': 'policy_line_row_number', 'partition': [ 'policyline' ] } ], mock_args, lineage)
    assert 'policy_line_row_number' in df.columns
    assert df.filter('`policy_line_row_number` is null').count() == 0

    # Test first partition
    assert df.filter('`policyline` = "policyline1"'). \
        agg({'policy_line_row_number': 'max'}).first()['max(policy_line_row_number)'] == 3

    # Test second partition
    assert df.filter('`policyline` = "policyline2"'). \
        agg({'policy_line_row_number': 'max'}).first()['max(policy_line_row_number)'] == 2


def test_transform_filterrows_removes_rows():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_filterrows(df, [{
		'condition': 'policyline is not null'
	}], mock_args, lineage)
    assert df.count() == 2


def test_merge_combines_null_columns():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data_merge, schema=mock_table_schema)
    df = transform_merge(df, [{
        'field': 'combined',
        'source_list': [ 'policyline', 'coveragedetail' ],
        'default': 'default',
	}], mock_args, lineage)

    assert 'combined' in df.columns
    assert df.filter('`combined` is null').count() == 0
    assert df.filter('`id` = 1').first()['combined'] == 'policyline1'
    assert df.filter('`id` = 2').first()['combined'] == 'default'
    assert df.filter('`id` = 3').first()['combined'] == ''
    assert df.filter('`id` = 4').first()['combined'] == ''

def test_merge_combines_columns_with_empty_string():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data_merge, schema=mock_table_schema)
    df = transform_merge(df, [{
        'field': 'combined',
        'source_list': [ 'policyline', 'coveragedetail' ],
        'default': 'default',
        'empty_string_is_null': True,
	}], mock_args, lineage)

    df.show(10,False)

    assert 'combined' in df.columns
    assert df.filter('`combined` is null').count() == 0
    assert df.filter('`id` = 1').first()['combined'] == 'policyline1'
    assert df.filter('`id` = 2').first()['combined'] == 'default'
    assert df.filter('`id` = 3').first()['combined'] == 'coverageB'
    assert df.filter('`id` = 4').first()['combined'] == 'default'