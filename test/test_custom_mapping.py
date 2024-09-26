# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys

try:
    from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.custom_mapping import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')


mock_args = {
    'source_key': mock_database_name + '/' + mock_table_name,
}

mock_table_data = [ ( 1, '1/1/2022', 1000, 'test' ), ( 2, '12/31/2022', 2000, 'test' ) ]
mock_table_schema = 'test_column_1 int, test_column_2 string, test_column_3 int, test_column_4 string'

mock_mapping = [
    {
        'sourcename': 'test_column_1',
        'destname': 'renamed_column_1',
    },
    {
        'sourcename': 'test_column_2',
        'destname': 'null',
    },
    {
        'sourcename': 'test_column_3',
        'destname': 'renamed_column_3',
    }
]

def test_escape_field_name_adds_backticks():
    test_fieldname = 'abc-def.ghi'
    escaped_fieldname = escape_field_name(test_fieldname)
    assert escaped_fieldname.startswith('`')
    assert escaped_fieldname.endswith('`')

def test_escape_field_name_doesnot_add_backticks():
    test_fieldname = '`abc-def`.`ghi`'
    escaped_fieldname = escape_field_name(test_fieldname)
    assert escaped_fieldname.count('`') == 4    # i.e. Unchamged

def test_unescape_field_name_removes_backticks():
    test_fieldname = '`abc-def`.`ghi`'
    unescaped_fieldname = unescape_field_name(test_fieldname)
    assert 'abc-def' in unescaped_fieldname
    assert 'ghi' in unescaped_fieldname
    assert unescaped_fieldname.count('`') == 0


def test_custommapping_renames_field():
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    lineage = mock_lineage([])
    df = custommapping(df, mock_mapping, mock_args, lineage)
    assert 'renamed_column_1' in df.columns
    assert 'renamed_column_3' in df.columns

def test_custommapping_drops_fields():
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    lineage = mock_lineage([])
    df = custommapping(df, mock_mapping, mock_args, lineage)
    assert 'test_column_2' not in df.columns, 'Explicit field drop failed'
    assert 'test_column_4' not in df.columns, 'Implicit field drop failed'

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
    assert 'renamed_column_1' in df.columns

def test_custommapping_ignores_extra_mapping_data():
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
                'notes': 'nothinghere',
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
    for new_name in [ 'renamed_column_1', 'renamed_column_2', 'renamed_column_3', 'renamed_column_4' ]:
        assert new_name in df.columns

def test_custommapping_logs_dropped_fields(capsys):
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    lineage = mock_lineage([])
    df = custommapping(df, mock_mapping, mock_args, lineage)
    captured = capsys.readouterr()
    assert 'discarded unmapped fields' in captured.out.lower()


mock_nested_table_data = [
        (('James',None,'Smith'),'OH','M',[]),
        (('Anna','Rose',''),'NY','F',[23,75,12,9]),
        (('Julia','','Williams'),'OH','F',[5,6]),
]

mock_nested_table_schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True),
     StructField('picked_numbers', ArrayType(IntegerType()), True),
])

def test_custommapping_collapses_nested_field():
    df = spark.createDataFrame(mock_nested_table_data, schema=mock_nested_table_schema)
    lineage = mock_lineage([])
    df = custommapping(
        df,
        [
            {
                'sourcename': '`name`.`firstname`',
                'destname': 'name_first',
            },
        ],
        mock_args, lineage)
    assert 'name_first' in df.columns