# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import coalesce, lit, col, first, count, row_number, when

def transform_merge(df: DataFrame, merge_spec: list, args: dict, lineage, *extra) -> DataFrame:
    """Merge columns using coalesce

    Parameters
    ----------
    merge_spec
        List of dictionary objects in the form:
            field: 'NewFieldName'
            source_list: [ 'first', 'second', 'third' ] list of fields in order of priority to coalesce
            default: 'N/A' optional lowest priority default value to use in the coalesce (instead of null)
            empty_string_is_null: optional, whether empty strings should be treated as null values
    """
    cols_map = {}
    for spec in merge_spec:
        column_list = [
            # Treat any empty strings as null, unless specified otherwise
            when(col(col_name) == '', None).otherwise(col(col_name))
                if spec.get('empty_string_is_null', False) else col_name
            for col_name in spec['source_list']
        ]

        if 'default' in spec:
            # Append the default value as a literal to the list of fields
            column_list.append(lit(spec['default']))

        cols_map.update({ spec['field']: coalesce(*column_list) })
        lineage.update_lineage(df, args['source_key'], 'merge', transform=spec)

    return df.withColumns(cols_map)

def transform_filldown(df: DataFrame, fill_spec: list, args: dict, lineage, *extra) -> DataFrame:
    """Fill starting column value down the columns for all null values until the next non-null
    Spark implementation of Pandas ffill()
    Based on: https://towardsdatascience.com/tips-and-tricks-how-to-fill-null-values-in-sql-4fccb249df6f

    Parameters
    ----------
    fill_spec
        List of dictionary objects in the form:
            field: existing field on which to apply the fill down operation
            sort: optional sort columns (must be a list)
    """
    for spec in fill_spec:
        # TODO: Support descending sort of columns
        sort_columns = spec.get('sort', [ lit(1) ])
        initial_window_spec  = Window.partitionBy(lit(1)).orderBy(*sort_columns). \
            rowsBetween(Window.unboundedPreceding, Window.currentRow)
        df = df.withColumn('transform_filldown_group_number', count(spec['field']).over(initial_window_spec))

        grouped_window_spec = Window.partitionBy('transform_filldown_group_number').orderBy(*sort_columns)
        columns = []
        for field in df.columns:
            if field == 'transform_filldown_group_number':
                continue
            if field == spec['field']:
                columns.append(
                    coalesce(
                        col(field),
                        first(col(field), ignorenulls=True).over(grouped_window_spec)
                    ).alias(field)
                )
            else:
                columns.append(col(field))

        df = df.select(columns)
        lineage.update_lineage(df, args['source_key'], 'filldown', transform=spec)

    return df

def transform_rownumber(df: DataFrame, rank_spec: list, args: dict, lineage, *extra) -> DataFrame:
    """Add a column representing an integer number of rows, optionally specifying a sort column
    and partition column. Specifying no partition column will simply number all rows.

    Parameters
    ----------
    rank_spec
        List of dictionary objects in the form:
            field: field name to add with the row number
            partition: optional partition columns (must be a list)
            sort: optional sort columns (must be a list)
    """
    for spec in rank_spec:
        # TODO: Support descending sort of columns
        sort_columns = spec.get('sort', [ lit(1) ])
        partition_columns = spec.get('partition', [ lit(1) ])

        df = df.withColumn(spec['field'], row_number(). \
            over(Window.partitionBy(*partition_columns).orderBy(*sort_columns)))

        lineage.update_lineage(df, args['source_key'], 'rownumber', transform=spec)

    return df

def transform_filterrows(df: DataFrame, filter_spec: list, args: dict, lineage, *extra):
    """Filter out (remove) rows of data that fail the provided filter operation
    NOTE: Use only when certain rows can be systematically discarded; otherwise use data quality quarantine rules

    Parameters
    ----------
    filter_spec
        List of dictionary objects in the form:
            condition: '`field` IS NOT NULL and `field2` == 'Test'
    """
    for spec in filter_spec:
        df = df.filter(spec['condition'])
    lineage.update_lineage(df, args['source_key'], 'filterrows', transform=filter_spec)
    return df