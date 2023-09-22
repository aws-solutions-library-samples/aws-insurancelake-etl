# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pyspark
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, regexp_extract, concat_ws, regexp_replace, initcap

def transform_date(df: DataFrame, date_formats: list, args: dict, lineage, *extra):
    """Convert specified date fields to ISO format based on known input format

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply date format conversion
    date_formats
        List of fieldnames and expected date format to convert _from_ in the form:
            field: 'FieldName'
            format: 'ExpectedDateFormat'
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with date format conversion applied
    """
    cols_map = {}
    for conversion in date_formats:
        cols_map.update({
            conversion['field']:
            to_date( col(conversion['field']), conversion['format'] )
        })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'dateconversion', transform=date_formats)
    return df.withColumns(cols_map)


def transform_timestamp(df: DataFrame, timestamp_formats: list, args: dict, lineage, *extra):
    """Convert specified date/time fields to ISO format based on known input format

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply timestamp format conversion
    timestamp_formats
        List of fieldnames and expected timestamp format to convert _from_ in the form:
            field: 'FieldName'
            format: 'ExpectedTimestampFormat'
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with timestamp format conversion applied
    """
    cols_map = {}
    for conversion in timestamp_formats:
        cols_map.update({
            conversion['field']:
            to_timestamp( col(conversion['field']), conversion['format'] )
        })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'timestampconversion', transform=timestamp_formats)
    return df.withColumns(cols_map)


def transform_decimal(df: DataFrame, decimal_formats: list, args: dict, lineage, *extra):
    """Convert specified numeric field (usually Float or Double) fields to Decimal (fixed
    precision) type

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply decimal conversion
    decimal_formats
        List of fieldnames and decimal precision, scale in the form:
            field: 'FieldName'
            format: 'precision,scale'
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with decimal conversion applied
    """
    cols_map = {}
    for conversion in decimal_formats:
        cols_map.update({
            conversion['field']:
            col(conversion['field']).cast(f"Decimal({conversion['format']})")
        })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'decimalconversion', transform=decimal_formats)
    return df.withColumns(cols_map)


def transform_implieddecimal(df: DataFrame, decimal_formats: list, args: dict, lineage, *extra):
    """Convert specified numeric field (usually Float or Double) fields to Decimal (fixed
    precision) type with implied decimal point support (i.e. last 2 digits are to the right of
    decimal, unless specified otherwise)

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply decimal conversion
    decimal_formats
        List of fieldnames and decimal precision, scale in the form:
            field: 'FieldName'
            format: 'precision,scale'
            num_implied: (optional) number of implied decimal points, default 2
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with decimal conversion applied
    """
    implied_decimal_pattern = r'([+-]?\d+)(\d{{{num_implied}}})$'

    cols_map = {}
    for conversion in decimal_formats:
        pattern = implied_decimal_pattern.format(num_implied = conversion.get('num_implied', 2))
        cols_map.update({
            conversion['field']:
            concat_ws(
                '.',
                regexp_extract(conversion['field'], pattern, 1),
                regexp_extract(conversion['field'], pattern, 2)
            ) \
            .cast(f"Decimal({conversion['format']})")
        })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'implieddecimalconversion', transform=decimal_formats)
    return df.withColumns(cols_map)


def transform_currency(df: DataFrame, currency_formats: list, args: dict, lineage, *extra):
    """Convert specified numeric field with currnecy formatting to Decimal (fixed precision)

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply currency conversion
    currency_formats
        List of fieldnames and decimal precision, scale in the form:
            field: 'FieldName'
            format: 'precision,scale' (optional, defaults to 16,2)
            euro: boolean (if true, expects European 5.000.000,12 format, default false)
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with currency conversion applied
    """
    cols_map = {}
    for conversion in currency_formats:
        decimal_format = conversion.get('format', '16,2')
        if conversion.get('euro', False):
            # Decimal = ,  Thousands = .
            new_column = regexp_replace(regexp_replace(conversion['field'], r'[^\-\d,-]+', ''), r',', '.')
        else:
            # Decimal = .  Thousands = ,
            new_column = regexp_replace(conversion['field'], r'[^\-\d\.]+', '')
        cols_map.update({
            conversion['field']: new_column.cast(f'Decimal({decimal_format})')
        })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'currencyconversion', transform=currency_formats)
    return df.withColumns(cols_map)


def transform_titlecase(df: DataFrame, titlecase_formats: list, args: dict, lineage, *extra):
    """Convert specified string field to title case

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply title case conversion
    titlecase_formats
        List of fieldnames in the form:
            field: 'FieldName'
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with title case conversion applied
    """
    cols_map = {}
    for conversion in titlecase_formats:
        cols_map.update({
            conversion['field']:
            initcap(col(conversion['field']))
        })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'titlecaseconversion', transform=titlecase_formats)
    return df.withColumns(cols_map)


def transform_bigint(df: DataFrame, bigint_formats: list, args: dict, lineage, *extra):
    """Convert specified numeric field to bigint

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply bigint conversion
    bigint_formats
        List of fieldnames in the form:
            field: 'FieldName'
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with bigint conversion applied
    """
    cols_map = {}
    for conversion in bigint_formats:
        cols_map.update({
            conversion['field']:
            col(conversion['field']).cast('long')
        })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'bigintconversion', transform=bigint_formats)
    return df.withColumns(cols_map)