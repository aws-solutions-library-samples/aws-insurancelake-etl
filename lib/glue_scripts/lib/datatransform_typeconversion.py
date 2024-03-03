# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col, to_date, to_timestamp, regexp_extract, concat_ws, regexp_replace, initcap, to_json )
from pyspark.sql.types import StringType

def transform_date(df: DataFrame, date_formats: list, args: dict, lineage, *extra):
    """Convert specified date fields to ISO format based on known input format

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply date format conversion
    date_formats
        List of fieldnames and expected date format to convert _from_ in the form:
            field: 'FieldName'
            source: "SourceFieldName" (optional, if omitted, field will be changed in place)
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
    cols_map = {
        conversion['field']: to_date(
            conversion.get('source', conversion['field']), conversion['format']
        )
            for conversion in date_formats
    }
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
            source: "SourceFieldName" (optional, if omitted, field will be changed in place)
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
    cols_map = {
        conversion['field']: to_timestamp(
                col(conversion.get('source', conversion['field'])),
                conversion['format']
            )
            for conversion in timestamp_formats
    }
    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'timestampconversion', transform=timestamp_formats)
    return df.withColumns(cols_map)


def transform_decimal(df: DataFrame, decimal_formats: list, args: dict, lineage, *extra):
    print('WARNING: decimal transform is deprecated and will be removed in the future; '
          'replace with changetype transform')
    return transform_changetype(
        df,
        { spec['field']: f"decimal({spec['format']})" for spec in decimal_formats },
        args,
        lineage
    )


def transform_changetype(df: DataFrame, field_types: dict, args: dict, lineage, *extra):
    """Cast columns to specified data type

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply column type conversion
    columns
        Dictionary of field -> type mappings in the form:
            field_name_1: bigint,
            field_name_2: decimal(10,2),
            field_name_3: string
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with field type conversion applied
    """
    cols_map = {
        field: to_json(field) if field_type.lower() == 'json' else col(field).cast(field_type)
            for field, field_type in field_types.items()
    }
    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'changetype', transform=field_types)
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
            source: "SourceFieldName" (optional, if omitted, field will be changed in place)
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
        sourcefield = conversion.get('source', conversion['field'])
        pattern = implied_decimal_pattern.format(num_implied = conversion.get('num_implied', 2))
        cols_map.update({
            conversion['field']:
            concat_ws(
                '.',
                regexp_extract(sourcefield, pattern, 1),
                regexp_extract(sourcefield, pattern, 2)
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
            field: "FieldName"
            source: "SourceFieldName" (optional, if omitted, field will be changed in place)
            format: "precision,scale" (optional, defaults to 16,2)
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
        sourcefield = conversion.get('source', conversion['field'])
        decimal_format = conversion.get('format', '16,2')

        if df.schema[sourcefield].dataType != StringType():
            # Skip regex replace with non-strings so we do not corrupt values
            new_column = col(sourcefield)
        else:
            if conversion.get('euro', False):
                # Decimal = ,  Thousands = .
                new_column = regexp_replace(regexp_replace(sourcefield, r'[^\-\d,-]+', ''), r',', '.')
            else:
                # Decimal = .  Thousands = ,
                new_column = regexp_replace(sourcefield, r'[^\-\d\.]+', '')

        cols_map.update({
            conversion['field']: new_column.cast(f'Decimal({decimal_format})')
        })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'currencyconversion', transform=currency_formats)
    return df.withColumns(cols_map)


def transform_titlecase(df: DataFrame, titlecase_fields: list, args: dict, lineage, *extra):
    """Convert specified string field to title case

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply title case conversion
    titlecase_fields
        Simple list of fieldnames
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with title case conversion applied
    """
    cols_map = {
        field: initcap(col(field))
            for field in titlecase_fields
    }
    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'titlecaseconversion', transform=titlecase_fields)
    return df.withColumns(cols_map)