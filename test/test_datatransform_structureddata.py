# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys

try:
    from pyspark.sql.types import (
        StringType, IntegerType, StructType, StructField, ArrayType, MapType
    )
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.datatransform_structureddata import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

mock_args = {
    'source_key': mock_database_name + '/' + mock_table_name
}

mock_nested_table_data = [
        (('James',None,'Smith'),'OH','M',[],{'mother': 'Sally', 'father': 'Bill'}),
        (('Anna','Rose',''),'NY','F',[23,75,12,9],{}),
        (('Julia','','Williams'),'OH','F',[5,6],{'mother': 'Samantha', 'son': 'Terry'}),
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
	 StructField('relatives', MapType(StringType(), StringType(), True)),
])

def test_transform_jsonexpandarray_expands_array():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_nested_table_data, schema=mock_nested_table_schema)
    df = transform_jsonexpandarray(df, [ { 
        'field': 'picked_number',
        'source': 'picked_numbers',
        'index_field': 'index',
    } ], mock_args, lineage)
    assert 'index' in df.columns
    assert 'picked_number' in df.columns
    assert df.count() == 7
    assert df.agg({'index': 'min'}).first()['min(index)'] == 1

def test_transform_jsonexpandmap_expands_map():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_nested_table_data, schema=mock_nested_table_schema)
    df = transform_jsonexpandmap(df, [{ 
        'field': 'relative_type',
        'source': 'relatives',
        'index_field': 'index',
        'key_field': 'map_key',
    }], mock_args, lineage)
    assert 'index' in df.columns
    assert 'map_key' in df.columns
    assert 'relative_type' in df.columns
    assert df.count() == 5
    assert df.agg({'index': 'min'}).first()['min(index)'] == 1

def test_transform_jsonexpandmap_expands_struct():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_nested_table_data, schema=mock_nested_table_schema)
    df = transform_jsonexpandmap(df, [{ 
        'field': 'name_part',
        'source': 'name',
        'index_field': 'index',
        'key_field': 'map_key',
    }], mock_args, lineage)
    assert 'index' in df.columns
    assert 'map_key' in df.columns
    assert 'name_part' in df.columns
    assert df.count() == 8

def test_transform_jsonexpandarray_expands_in_place():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_nested_table_data, schema=mock_nested_table_schema)
    df = transform_jsonexpandarray(df, [ { 
        'field': 'picked_numbers',
        'index_field': 'index',
    } ], mock_args, lineage)
    assert df.count() == 7
    assert 'picked_numbers' in df.columns
    assert len(df.columns) == 6

def test_transform_jsonexpandmap_expands_in_place():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_nested_table_data, schema=mock_nested_table_schema)
    df = transform_jsonexpandmap(df, [{ 
        'field': 'name',
        'index_field': 'index',
        'key_field': 'map_key',
    }], mock_args, lineage)
    assert df.count() == 8
    assert 'name' in df.columns
    assert len(df.columns) == 7


mock_xml_column_table_data = [
    ((1, """<?xml version="1.0" encoding="UTF-8"?>
<metadata>
    <metd>20230541</metd>
    <cntorgp>
        <cntorg>TestData</cntorg>
    </cntorgp>
</metadata>"
""")),
    ((2, """<?xml version="1.0" encoding="UTF-8"?>
<metadata>
    <metd>20230551</metd>
    <cntorgp>
        <cntorg>TestData2</cntorg>
    </cntorgp>
</metadata>"
""")),
]

def test_transform_xmlstructured_from_string():
    mock_table_schema = 'id int, xmldata string'
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_xml_column_table_data, schema=mock_table_schema)
    df = transform_xmlstructured(df, [ 'xmldata' ], mock_args, lineage, spark.sparkContext)
    assert df.filter('`xmldata` is null').count() == 0
    assert df.filter('`xmldata`.`cntorgp`.`cntorg` is not null').count() == 2


mock_json_column_table_data = [
    ((1, '{ "myjson": { "key1": "value1", "key2": "value2" }, "version": 1 }')),
    ((2, '{ "myjson": {}, "version": 1 }')),
]

def test_transform_jsonstructured_from_string():
    mock_table_schema = 'id int, jsondata string'
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_json_column_table_data, schema=mock_table_schema)
    df = transform_jsonstructured(df, [ 'jsondata' ], mock_args, lineage, spark.sparkContext)
    assert df.filter('`jsondata` is null').count() == 0
    assert df.filter('`jsondata`.`myjson`.`key1` is not null').count() == 1

def test_transform_flatten_flattens_struct():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_nested_table_data, schema=mock_nested_table_schema)
    df = transform_flatten(df, [ { 'field': 'name' } ], mock_args, lineage)
    assert 'name' in df.columns
    assert 'firstname' in df.columns
    assert 'middlename' in df.columns
    assert 'lastname' in df.columns