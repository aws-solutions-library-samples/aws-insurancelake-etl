# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
from datetime import date
from decimal import Decimal

try:
    from pyspark.sql.types import DateType, IntegerType, StringType
    from test.glue_job_mocking_helper import *
    from lib.glue_scripts.lib.datatransform_premium import *
except ModuleNotFoundError as e:
    if 'pyspark' not in e.msg:
        raise e
    from test.glue_job_mocking_helper_stub import *

pytestmark = pytest.mark.skipif('pyspark' not in sys.modules, reason='No PySpark environment found')

mock_args = {
    'source_key': mock_database_name + '/' + mock_table_name
}

mock_table_columns = [ 'id', 'effectivedate', 'expirationdate', 'premium1', 'premium2', 'premium3' ]
mock_table_data = [
    ( 1, date(2022, 1, 1), date(2022, 12, 31), Decimal('3000.00'), Decimal('2000.00'), Decimal('1000.00') ),
    ( 2, date(2022, 3, 1), date(2022,  8, 31), Decimal('100.50'), Decimal('0.0'), Decimal('0.0') )
]
mock_table_schema = 'id int, effectivedate date, expirationdate date, ' \
    'premium1 decimal(16,2), premium2 decimal(16,2), premium3 decimal(16,2)'

mock_table_data_split = [
    ( 1, Decimal('3000.00'), Decimal('0.50'), Decimal('0.50') ),
    ( 2, Decimal('100.50'), Decimal('0.70'), None )
]
mock_table_schema_split = 'id int, ' \
    'premium decimal(16,2), split1 decimal(16,2), split2 decimal(16,2)'


def test_add_columns_sums_three_columns():
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = df.withColumn('totalpremium', add_columns('premium1', 'premium2', 'premium3'))

    assert df.filter('id = 1').first()['totalpremium'] == Decimal('6000.00')
    assert df.filter('id = 2').first()['totalpremium'] == Decimal('100.50')


def test_transform_addcolumns_adds_column_sum():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_addcolumns(df, [{
            'field': 'totalpremium',
            'source_columns': [ 'premium1', 'premium2', 'premium3' ],
        }], mock_args, lineage)

    assert 'totalpremium' in df.columns
    assert df.filter('id = 1').first()['totalpremium'] == Decimal('6000.00')
    assert df.filter('id = 2').first()['totalpremium'] == Decimal('100.50')


def test_expandpolicymonths_adds_month_columns():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_expandpolicymonths(df, {
			'policy_month_start_field': 'startdate',
			'policy_month_end_field': 'enddate',
			'policy_effective_date': 'effectivedate',
			'policy_expiration_date': 'expirationdate',
			'policy_month_index': 'policymonthindex'
        }, mock_args, lineage)

    assert 'startdate' in df.columns and df.schema['startdate'].dataType == DateType()
    assert 'enddate' in df.columns and df.schema['enddate'].dataType == DateType()
    assert 'policymonthindex' in df.columns and df.schema['policymonthindex'].dataType == IntegerType()


def test_expandpolicymonths_expands_rows():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_expandpolicymonths(df, {
			'policy_month_start_field': 'startdate',
			'policy_month_end_field': 'enddate',
			'policy_effective_date': 'effectivedate',
			'policy_expiration_date': 'expirationdate',
			'policy_month_index': 'policymonthindex'
        }, mock_args, lineage)

    # One policy is 12 months, the other is 6 months
    assert df.count() == 18

    # Ensure numbering starts with 1 and ends on expected number of months (longer policy only)
    assert df.agg({'policymonthindex': 'min'}).first()['min(policymonthindex)'] == 1
    assert df.agg({'policymonthindex': 'max'}).first()['max(policymonthindex)'] == 12


def test_expandpolicymonths_adds_uniqueid():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data, schema=mock_table_schema)
    df = transform_expandpolicymonths(df, {
            'uniqueid_field': 'uniqueid',
			'policy_month_start_field': 'startdate',
			'policy_month_end_field': 'enddate',
			'policy_effective_date': 'effectivedate',
			'policy_expiration_date': 'expirationdate',
			'policy_month_index': 'policymonthindex'
        }, mock_args, lineage)

    assert 'uniqueid' in df.columns and df.schema['uniqueid'].dataType == StringType()
    # Two policies, so 2 unique IDs, regardless of number of months
    assert df.select('uniqueid').distinct().count() == 2


def test_transform_multiplycolumns_multiply_values_in_three_columns():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data_split, schema=mock_table_schema_split)
    df = transform_multiplycolumns(df, [{
            'field': 'split_premium',
            'source_columns': [ 'premium', 'split1', 'split2' ],
        }], mock_args, lineage)

    assert df.filter('id = 1').first()['split_premium'] == Decimal('750.00')
    assert df.filter('id = 2').first()['split_premium'] == Decimal('70.35')


def test_transform_multiplycolumns_uses_empty_value():
    lineage = mock_lineage([])
    df = spark.createDataFrame(mock_table_data_split, schema=mock_table_schema_split)
    df = transform_multiplycolumns(df, [{
            'field': 'split_premium',
            'source_columns': [ 'premium', 'split1', 'split2' ],
            'empty_value': 0,
        }], mock_args, lineage)

    assert df.filter('id = 1').first()['split_premium'] == Decimal('750.00')
    assert df.filter('id = 2').first()['split_premium'] == Decimal('0')