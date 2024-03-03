# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from decimal import Decimal
import datetime
from dateutil import relativedelta, rrule
import calendar
from functools import reduce
from operator import add, mul
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf, expr, months_between, coalesce, lit, col
from pyspark.sql.types import DecimalType, DateType, ArrayType, IntegerType

def add_columns(*source_columns):
    """Add values from an arbitrary number of Spark columns together and return the result (as a column)
    Uses only Spark native functions for performance (contributed by Gonzalo Herreros <gonher@amazon.com>)
    """
    return reduce(add, [ coalesce(col(c), lit(0)) for c in source_columns ])


def policy_month_list(policy_effective_date: datetime.date, policy_expiration_date: datetime.date):
    """Return list of months from the first month of the policy to the last
    """
    first_day_of_month = datetime.date(policy_effective_date.year, policy_effective_date.month, 1)
    return list(rrule.rrule(freq=rrule.MONTHLY, dtstart=first_day_of_month, until=policy_expiration_date))


def months_between_normalized(expiration_date: datetime.date, effective_date: datetime.date) -> any:
    """Calculate the number of months between 2 dates with floor-style rounding to nearest whole month
    Designed to work as regular Python function and Spark UDF
    """
    if None in [ expiration_date, effective_date ]:
        return None

    # Columns of DateType are automatically converted to Python datetime.date
    # Achieve a floor-style rounding of number of months by subtracting 1 month from the end date
    expiration_prior_month = expiration_date - relativedelta.relativedelta(months=1)
    if expiration_prior_month < effective_date:
        # Except when policy length is one month or less
        expiration_prior_month = expiration_date

    # Using a list of months will ensure an integer number of months
    return len(policy_month_list(effective_date, expiration_prior_month))


def transform_enddate(df: DataFrame, enddate_spec: list, args: dict, lineage, *extra) -> DataFrame:
    """Add a number of months to a specified date to get an ending/expiration date

    Parameters
    ----------
    policymonths_spec
        A list of dictionary objects in the form:
            field: "NewFieldName"
            start_date: field name containing the start date or policy effective date
            num_months: field name containing the number of months to add to the start date
                Column must be integer values
    """
    cols_map = {}
    for spec in enddate_spec:
        # "Column is not iterable" workaround:
        # https://sparkbyexamples.com/pyspark/pyspark-typeerror-column-is-not-iterable/
        cols_map.update({spec['field']: expr(
            'add_months(' + spec['start_date'] + ',' + spec['num_months'] + ')'
        )})

    lineage.update_lineage(df, args['source_key'], 'enddate', transform=enddate_spec)
    return df.withColumns(cols_map)


def transform_policymonths(df: DataFrame, policymonths_spec: list, args: dict, lineage, *extra) -> DataFrame:
    """Calculate number of months between policy start/end dates

    Parameters
    ----------
    policymonths_spec
        A list of dictionary objects in the form:
            field: "NewFieldName"
            policy_effective_date: field name containing policy effective date
            policy_expiration_date: field name containing policy expiration date
            normalized: (optional) boolean if number of months should be floor-rounded
    """
    cols_map = {}
    for spec in policymonths_spec:
        dates = [ spec['policy_expiration_date'], spec['policy_effective_date'] ]

        if spec.get('normalized', False):
            months_function = udf(months_between_normalized, IntegerType())
            cols_map.update({ spec['field']: months_function(*dates) })
        else:
            cols_map.update({ spec['field']: months_between(*dates).cast('Decimal(16,2)') })

    lineage.update_lineage(df, args['source_key'], 'policymonths', transform=policymonths_spec)
    return df.withColumns(cols_map)


@udf(ArrayType(DateType()))
def add_policy_months_list(policy_effective_date, policy_expiration_date):
    """Return an array column containing a list of active policy months for a policy
    Intended to be used with Spark explode
    """
    if None not in [ policy_effective_date, policy_expiration_date ]:
        # ArrayType is equivalent to Python list
        return policy_month_list(policy_effective_date, policy_expiration_date)

@udf(DateType())
def last_day_of_month(any_date_in_month):
    """Given a date, return the last day of the month for that date
    """
    if any_date_in_month:
        return datetime.date(any_date_in_month.year, any_date_in_month.month, 
            calendar.monthrange(any_date_in_month.year, any_date_in_month.month)[1]
        )

def transform_expandpolicymonths(df: DataFrame, expandpremium_spec: dict, args: dict, lineage, *extra) -> DataFrame:
    """Expand dataset to one row for each month the policy is active with a calculated earned premium

    Parameters
    ----------
    earnedpremium_spec
        A dictionary object in the form:
            policy_effective_date: field name containing policy effective date
            policy_expiration_date: field name containing policy expiration date
            uniqueid_field: (optional) Name of field to add with generated unique identifier (uuid)
			policy_month_start_field: Name of field to store the policy month
			policy_month_end_field: Name of field to store the policy month
            policy_month_index: field name to store the month index (starting with 1)
    """
    # If specified, add a unique ID for each policy before exploding the rows
    if 'uniqueid_field' in expandpremium_spec:
        df = df.withColumn(expandpremium_spec['uniqueid_field'], expr('uuid()'))

    # Create array column with list of policy months (first day of the month)
    df = df.withColumn('expandpolicymonths_transform_policy_months_list',
            add_policy_months_list(
                expandpremium_spec['policy_effective_date'],
                expandpremium_spec['policy_expiration_date']
        ))

    # Use posexplode_outer to expand array of months to one row per month with an index column
    # Syntax reference: https://issues.apache.org/jira/browse/SPARK-20174
    df = df.selectExpr('*',
        "posexplode_outer(expandpolicymonths_transform_policy_months_list) "
        f"as ({expandpremium_spec['policy_month_index']}, `{expandpremium_spec['policy_month_start_field']}`)"
    ).drop('expandpolicymonths_transform_policy_months_list')

    cols_map = {
        # Add column for last day of month, so we have both start and end date of period
        expandpremium_spec['policy_month_end_field']:
            last_day_of_month(expandpremium_spec['policy_month_start_field']),
        # Index is 0-based by default, so add 1 to normalize it
        expandpremium_spec['policy_month_index']:
            expr(f"{expandpremium_spec['policy_month_index']} + 1")
    }
    df = df.withColumns(cols_map)

    lineage.update_lineage(df, args['source_key'], 'expandpolicymonths', transform=expandpremium_spec)
    return df


@udf(DecimalType(16,2))
def earnedpremium_straightline(total_premium, effective_date, expiration_date, period_start_date, period_end_date):
    """Calculate Earned Premium for a given policy and period dates using a straighline method
    """
    if None in [ total_premium, effective_date, expiration_date, period_start_date, period_end_date ]:
        return None

    month_list = policy_month_list(effective_date, expiration_date)
    # Check for empty list due to effective_date >= expiration_date (i.e. bad data)
    # Use last month in normalized month list instead of expiration_date to provide consistent results
    if not month_list or \
            period_end_date < effective_date or period_end_date > month_list[-1].date():
        # Last month of the policy will have $0, first month will have a full earned premium
        # expiration_date must be on or earlier than effective_date (prevents division by 0)
        return None

    return total_premium / months_between_normalized(expiration_date, effective_date)


@udf(DecimalType(16,2))
def earnedpremium_byday(total_premium, effective_date, expiration_date, period_start_date, period_end_date):
    """Calculate Earned Premium for a given policy and period dates using number of days in period
    """
    if None in [ total_premium, effective_date, expiration_date, period_start_date, period_end_date ]:
        return None

    if period_end_date < effective_date or period_start_date > expiration_date or \
            (expiration_date - effective_date).days < 0:
        # Specified period is outside the policy dates
        # expiration_date must be on or earlier than effective_date (prevents division by 0)
        return None

    start_date = effective_date if period_start_date < effective_date else period_start_date
    end_date = expiration_date if period_end_date > expiration_date else period_end_date
    return total_premium * \
        Decimal(
            ((end_date - start_date).days + 1)
            /
            ((expiration_date - effective_date).days + 1)
        )

def transform_earnedpremium(df: DataFrame, earnedpremium_spec: list, args: dict, lineage, *extra) -> DataFrame:
    """Calculate monthly earned premium

    Parameters
    ----------
    earnedpremium_spec
        A list of dictionary objects in the form:
            field: "NewFieldName"
            written_premium_list: [ "first", "second", "third" ] list of fields containing written premium to be summed
            period_start_date: field name containing period start date
            period_end_date: field name containing period end date
            policy_effective_date: field name containing policy effective date
            policy_expiration_date: field name containing policy expiration date
            byday: (optional) boolean to determine method of calculation (by month vs. by day)
    """
    cols_map = {}
    for spec in earnedpremium_spec:
        if 'byday' in spec and spec['byday']:
            cols_map.update({spec['field']: earnedpremium_byday(
                add_columns(*spec['written_premium_list']),
                spec['policy_effective_date'],
                spec['policy_expiration_date'],
                spec['period_start_date'],
                spec['period_end_date'],
            )})
        else:
            cols_map.update({spec['field']: earnedpremium_straightline(
                add_columns(*spec['written_premium_list']),
                spec['policy_effective_date'],
                spec['policy_expiration_date'],
                spec['period_start_date'],
                spec['period_end_date'],
            )})

    lineage.update_lineage(df, args['source_key'], 'earnedpremium', transform=earnedpremium_spec)
    return df.withColumns(cols_map)


def transform_addcolumns(df: DataFrame, addcolumns_spec: list, args: dict, lineage, *extra) -> DataFrame:
    """Add two or more columns together in a new or existing column

    Parameters
    ----------
    addcolumns_spec
        A list of dictionary objects in the form:
            field: "NewFieldName"
            source_columns: [ "first", "second", "third" ] list of fields to add together
    """
    cols_map = {}
    for spec in addcolumns_spec:
        cols_map.update({ spec['field']: add_columns(*spec['source_columns']) })

    lineage.update_lineage(df, args['source_key'], 'addcolumns', transform=addcolumns_spec)
    return df.withColumns(cols_map)


def transform_flipsign(df: DataFrame, field_list: list, args: dict, lineage, *extra) -> DataFrame:
    """Flip the sign of a numeric column in a Spark DataFrame, optionally in a new column

    Parameters
    ----------
    field_list
        List of dictionary objects in the form:
            field: "NeworCurrentFieldName"
            source: "SourceFieldName" (optional, if omitted, field will be changed in place)
    """
    cols_map = {}
    for spec in field_list:
        sourcefield = spec.get('source', spec['field'])
        cols_map.update({ spec['field']: - df[sourcefield] })

    lineage.update_lineage(df, args['source_key'], 'flipsign', transform=field_list)
    return df.withColumns(cols_map)


def transform_multiplycolumns(df: DataFrame, multiplycolumns_spec: list, args: dict, lineage, *extra) -> DataFrame:
    """Multiply two or more columns together in a new or existing column

    Parameters
    ----------
    multiplycolumns_spec
        List of dictionary objects in the form:
            field: "NewFieldName"
            source_columns: [ "first", "second", "third" ] list of fields to multiply together
            empty_value: Value to use if field value is null/empty, (optional, default is 1)
    """
    cols_map = {}
    for spec in multiplycolumns_spec:
        empty_value = spec.get('empty_value', 1)
        cols_map.update({
            spec['field']: reduce(mul, [
                coalesce(col(c), lit(empty_value))
                    for c in spec['source_columns']
            ])
        })

    lineage.update_lineage(df, args['source_key'], 'multiplycolumns', transform=multiplycolumns_spec)
    return df.withColumns(cols_map)