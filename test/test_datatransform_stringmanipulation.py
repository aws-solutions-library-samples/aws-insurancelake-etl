# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys

try:
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.datatransform_stringmanipulation import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

mock_args = {
    'source_key': mock_database_name + '/' + mock_table_name
}

mock_table_data = [ ( 1, '2022-01-01' ), ( 2, '2022-12-31' ), ( 3, '0000-00-00') ]
mock_table_schema = 'id int, date string'

def test_columnreplace_transform_replaces_0s_with_nulls():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_columnreplace(df, [{
        'field': 'date',
        'pattern': '0000-00-00',
        'replacement': '',
    }], mock_args, lineage)
    assert df.filter("`date` = ''").count() == 1

def test_literal_transform_adds_column():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_literal(df, {
        'newfield': 'staticvalue',
        'newfield2': 'staticvalue2',
    }, mock_args, lineage)
    assert 'newfield' in df.columns
    assert 'newfield2' in df.columns
    assert df.filter("`newfield` = 'staticvalue'").count() == 3
    assert df.filter("`newfield2` = 'staticvalue2'").count() == 3