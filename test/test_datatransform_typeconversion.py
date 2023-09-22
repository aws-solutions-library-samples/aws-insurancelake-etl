# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
from decimal import Decimal

try:
    from pyspark.sql.types import DateType, StringType
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.datatransform_typeconversion import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No pySpark environment found')

mock_args = {
    'source_key': mock_database_name + '/' + mock_table_name
}

mock_schema = [
    { 'Name': 'test_column_1', 'Type': 'date' },
    { 'Name': 'test_column_2', 'Type': 'string' },
    { 'Name': 'test_column_3', 'Type': 'int' },
]
mock_field = { 'Name': 'test_column', 'Type': 'string' }
mock_table_columns = [ 'id', 'date' ]
mock_table_data = [ ( 1, '1/1/2022' ), ( 2, '12/31/2022' ) ]
mock_table_schema = 'id int, date string'


def test_transform_date_converts_schema():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    assert df.schema['date'].dataType == StringType()
    df = transform_date(df, [ { 'field': 'date', 'format': 'M/d/yyyy' } ], mock_args, lineage)
    assert df.schema['date'].dataType == DateType()

def test_transform_date_converts_data():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_date(df, [ { 'field': 'date', 'format': 'M/d/yyyy' } ], mock_args, lineage)
    assert df.filter('`date` is null').count() == 0

def test_transform_date_throws_error_on_bad_convert():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    # This date format will fail to convert 1/1/2022 because of the single digit month and day
    df = transform_date(df, [ { 'field': 'date', 'format': 'MM/dd/yyyy' } ], mock_args, lineage)
    with pytest.raises(Exception) as e_info:
        df.show(5)
    # Look for mention of this setting in Spark Exception to modify date parsing behavior
    assert e_info.match('spark.sql.legacy.timeParserPolicy')


def test_transform_currency_converts_schema():
    mock_currency_schema = 'money string'
    mock_currency_data = [ ( '100.00', ), ( '$500.00', ), ( '$1,234,567.89', ), ( '-$2,000', ) ]
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_currency_data, schema=mock_currency_schema)
    assert df.schema['money'].dataType == StringType()
    df = transform_currency(df, [ { 'field': 'money', 'format': '10,2' } ], mock_args, lineage)
    assert str(df.schema['money'].dataType) == 'DecimalType(10,2)'

def test_transform_currency_converts_data():
    mock_currency_schema = 'money string'
    mock_currency_data = [ ( '100.00', ), ( '$500.00', ), ( '$1,234,567.89', ), ( '-$2,000', ) ]
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_currency_data, schema=mock_currency_schema)
    df = transform_currency(df, [ { 'field': 'money' } ], mock_args, lineage)
    assert df.filter('`money` is null').count() == 0
    assert df.agg({ 'money': 'sum' }).first()['sum(money)'] == Decimal('1233167.89')

def test_transform_currency_converts_euro_data():
    mock_currency_schema = 'money string'
    mock_currency_data = [ ( '100,00', ), ( '$500,00', ), ( '$1.234.567,89', ), ( '-$2.000', ) ]
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_currency_data, schema=mock_currency_schema)
    df = transform_currency(df, [ { 'field': 'money', 'euro': True } ], mock_args, lineage)
    assert df.filter('`money` is null').count() == 0
    assert df.agg({ 'money': 'sum' }).first()['sum(money)'] == Decimal('1233167.89')

def test_transform_titlecase_converts_string():
    lineage = mock_lineage([])
    df = spark.createDataFrame([ ( 'test string', ) ], schema='text string')
    df = transform_titlecase(df, [ { 'field': 'text' } ], mock_args, lineage)
    assert df.filter('`text` = "Test String"').count() == 1

def test_transform_bigint_converts_string():
    lineage = mock_lineage([])
    df = spark.createDataFrame([ ( '100', ) ], schema='amount string')
    assert df.schema['amount'].dataType == StringType()
    df = transform_bigint(df, [ { 'field': 'amount' } ], mock_args, lineage)
    assert str(df.schema['amount'].dataType) == 'LongType()'
    assert df.filter('`amount` = 100').count() == 1