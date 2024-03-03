# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit, regexp_extract, regexp_replace, udf
import re

def transform_filename(df: DataFrame, filename_patterns: list, args: dict, lineage, *extra):
    """Add column in DataFrame based on regexp group match on the filename argument to the Glue job

    Parameters
    ----------
    df
        pySpark DataFrame on which to add the column
    filename_patterns
        List of patterns and fieldnames to add:
            pattern: 'regex-with-grouping'
            field: 'FieldName'
            required: boolean
    args
        Glue job arguments, from which source_key and base_file_name are used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with filename transforms applied
    """
    cols_map = {}
    for spec in filename_patterns:
        filename_re = re.compile(spec['pattern'])
        match = filename_re.search(args['base_file_name'])
        if match:
            cols_map.update({ spec['field']: lit(match.group(1)) })
        else:
            if spec['required']:
                # Pattern is required, so stop workflow
                raise RuntimeError(f"Filename '{args['base_file_name']}' failed to match required pattern {spec['pattern']} for field '{spec['field']}'")
            else:
                # Pattern is not required, so add empty column as placeholder
                cols_map.update({ spec['field']: lit(None).cast(StringType()) })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'filename', transform=filename_patterns)
    return df.withColumns(cols_map)


def transform_columnfromcolumn(df: DataFrame, cfc_spec: list, args: dict, lineage, *extra):
    """Add or replace column in DataFrame based on regexp group match pattern

    Parameters
    ----------
    cfc_spec
        List of patterns and fieldnames to add:
            field: 'FieldName'
            source: 'FieldName' (optional, field will be replaced if ommitted)
            pattern: 'regex-with-grouping'
    """
    cols_map = {}
    for spec in cfc_spec:
        sourcefield = spec.get('source', spec['field'])
        cols_map.update({
            spec['field']:
            regexp_extract(sourcefield, spec['pattern'], 1)
        })

    lineage.update_lineage(df, args['source_key'], 'columnfromcolumn', transform=cfc_spec)
    return df.withColumns(cols_map)


def transform_columnreplace(df: DataFrame, replace_spec: list, args: dict, lineage, *extra):
    """Add or replace column in DataFrame with regex substitution on an existing column

    Parameters
    ----------
    replace_spec
        List of patterns and fieldnames to add:
            field: 'FieldName'
            source: 'FieldName' (optional, field will be replaced if ommitted)
            pattern: 'regex-replace-string'
            replacement: 'newstring'
    """
    cols_map = {}
    for spec in replace_spec:
        sourcefield = spec.get('source', spec['field'])
        cols_map.update({
            spec['field']:
            regexp_replace(sourcefield, spec['pattern'], spec['replacement'])
        })

    lineage.update_lineage(df, args['source_key'], 'columnreplace', transform=replace_spec)
    return df.withColumns(cols_map)


def transform_literal(df: DataFrame, spec: dict, args: dict, lineage, *extra):
    """Add column to DataFrame with static/literal value supplied in specification

    Parameters
    ----------
    spec
        Dictionary of field names and values
    """
    cols_map = {}
    for field_name, field_value in spec.items():
        cols_map.update({ field_name: lit(field_value) })

    lineage.update_lineage(df, args['source_key'], 'literal', transform=spec)
    return df.withColumns(cols_map)


@udf(StringType())
def format_column(formatstring, *args):
    """Format list of column values using a Python format string; convert None values to
    empty strings and attempt to clean up dangling whitespace separators
    """
    if formatstring:
        checked_args = [ '' if value is None else value for value in args ]
        return formatstring.format(*checked_args).strip()

def transform_combinecolumns(df: DataFrame, combine_spec: list, args: dict, lineage, *extra):
    """Add column to DataFrame using format string and source columns

    Parameters
    ----------
    combine_spec
        List of dictionary objects in the form:
            field: 'FieldName'
            source_columns: [ 'FieldName', 'FieldName2' ] list of columns
            format: '{}-{}.{}' Python style format string with substitutions for source columns
    """
    cols_map = {}
    for spec in combine_spec:
        cols_map.update({
            spec['field']:
            format_column(lit(spec['format']), *spec['source_columns'])
        })

    lineage.update_lineage(df, args['source_key'], 'columnfromcolumn', transform=combine_spec)
    return df.withColumns(cols_map)