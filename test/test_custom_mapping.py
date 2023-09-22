# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys

try:
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.custom_mapping import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No pySpark environment found')


mock_args = {
    'source_key': mock_database_name + '/' + mock_table_name,
}

mock_table_data = [ ( 1, '1/1/2022', 1000, 'test' ), ( 2, '12/31/2022', 2000, 'test' ) ]
mock_table_schema = 'test_column_1 int, test_column_2 string, test_column_3 int, test_column_4 string'

mock_mapping = [
    {
        'sourcename': 'test_column_1',
        'destname': 'renamed_column_1'
    },
    {
        'sourcename': 'test_column_2',
        'destname': 'null'
    },
    {
        'sourcename': 'test_column_3',
        'destname': 'renamed_column_3'
    }
]


def test_custommapping_renames_field():
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    lineage = mock_lineage([])
    df = custommapping(df, mock_mapping, mock_args, lineage)
    mapped_fieldnames = [ field.name for field in df.schema ]
    assert 'renamed_column_1' in mapped_fieldnames
    assert 'renamed_column_3' in mapped_fieldnames


def test_custommapping_drops_fields():
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    lineage = mock_lineage([])
    df = custommapping(df, mock_mapping, mock_args, lineage)
    mapped_fieldnames = [ field.name for field in df.schema ]
    assert 'test_column_2' not in mapped_fieldnames, 'Explicit field drop failed'
    assert 'test_column_4' not in mapped_fieldnames, 'Implicit field drop failed'


def test_custommapping_fuzzymatching_tokenratio():
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    lineage = mock_lineage([])
    df = custommapping(
        df, 
        [
            {
                'sourcename': '1_column_test',
                'destname': 'renamed_column_1',
                'threshold': 90,
                'scorer': 'token_sort_ratio'
            },
            {
                'sourcename': 'test_column_2',
                'destname': 'renamed_column_2',
            },
            {
                'sourcename': 'test_column_3',
                'destname': 'renamed_column_3',
            },
            {
                'sourcename': 'test_column_4',
                'destname': 'renamed_column_4',
            }
        ],
        mock_args, lineage)
    mapped_fieldnames = [ field.name for field in df.schema ]
    assert 'renamed_column_1' in mapped_fieldnames


def test_custommapping_ignores_extra_mapping_data(monkeypatch):
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    lineage = mock_lineage([])
    df = custommapping(
        df, 
        [
            {
                'sourcename': '1_column_test',
                'destname': 'renamed_column_1',
                'threshold': 90,
                'scorer': 'token_set_ratio',
                'comment': 'nothinghere',
                'notes': 'something',
                'owner': 'tom',
            },
            {
                'sourcename': 'test_column_2',
                'destname': 'renamed_column_2',
                'comment': 'nothinghere',
            },
            {
                'sourcename': 'test_column_3',
                'destname': 'renamed_column_3',
            },
            {
                'sourcename': 'test_column_4',
                'destname': 'renamed_column_4',
                'notes': 'somenotes',
            }
        ],
        mock_args, lineage)
    mapped_fieldnames = [ field.name for field in df.schema ]
    for new_name in [ 'renamed_column_1', 'renamed_column_2', 'renamed_column_3', 'renamed_column_4' ]:
        assert new_name in mapped_fieldnames


def test_custommapping_logs_dropped_fields(capsys):
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    lineage = mock_lineage([])
    df = custommapping(df, mock_mapping, mock_args, lineage)
    captured = capsys.readouterr()
    assert 'discarded unmapped fields' in captured.out.lower()