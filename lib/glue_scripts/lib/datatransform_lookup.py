# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import json
import boto3
from boto3.dynamodb.conditions import Key
from pyspark.context import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import broadcast, concat_ws
from awsglue.context import GlueContext

def get_lookup_data(table_name: str, key_values: dict, lookup_data_attribute_name: str) -> list:
    """Gets a single data item from a DynamoDB table based on key values
    key_values must contain all keys needed to represent a primary key

    Parameters
    ----------
    table_name
        DynamoDB table from which to get item
    key_values
        Dictionary of key values to identify item
    lookup_data_attribute_name
        Attribute name to use for lookup_data (will be json parsed)

    Returns
    -------
    list
        List of lookup keys and values suitable for creating a DataFrame
    """
    dynamodb = boto3.resource('dynamodb')
    lookup_table = dynamodb.Table(table_name)

    response = lookup_table.get_item(
            Key = key_values
        )

    if 'Item' not in response:
        raise RuntimeError(f'No lookup data found for key values {key_values} on table {table_name}')

    lookup_json = json.loads(response['Item'][lookup_data_attribute_name])
    # Format the data so it can be used to create a dataframe
    return [ ( key, lookup_json[key] ) for key in lookup_json.keys() ]

def transform_lookup(df_for_lookup: DataFrame, lookup_spec: list, args: dict, lineage, sc: SparkContext, *extra) -> DataFrame:
    """Replace specified column values with values looked up from an external table

    Parameters
    ----------
    df_for_lookup
        pySpark DataFrame on which to perform the lookups and replacement
    lookup_spec
        List of dictionary objects in the form:
            field: 'NewFieldName' (must be a new field if source is specified)
            source: 'OriginalFieldName' (optional, field will be replaced if ommitted)
            lookup: 'LookupData' (DynamoDB column_name value to use to get looked up values')
            nomatch: 'N/A' (optional, no matches will be empty/null)
            source_system: 'global' (optional, override the source system name in the workflow for
                global lookup tables)
    args
        Glue job arguments, from which source_key, value_lookup_table, and target_database_name are used
    lineage
        Initialized lineage class object from the calling job
    sc
        Spark Context class from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with lookup applied
    """
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Pointers to the DataFrame df_for_lookup will be repeatedly dereferenced in the loop (and
    # freed from memory when Spark is able) as the variable is replaced with a new DataFrame 
    # with each operation.
    for spec in lookup_spec:

        # Get lookup data from DynamoDB
        lookup_data = get_lookup_data(
                args['value_lookup_table'],
                {
                    'source_system': spec.get('source_system', args['target_database_name']),
                    'column_name': spec['lookup'],
                },
                'lookup_data'
            )

        # spec will indicate a source field if the looked up value should be added as a new column
        sourcefield = spec.get('source', spec['field'])

        # Create a DataFrame with lookup values to join to
        df_for_join = spark.createDataFrame(lookup_data, schema=['orig_value', 'new_value'])

        # Left join so that no source data is lost
        # After join, drop the now unneeded orig_value column
        df_for_lookup = df_for_lookup.join(
                broadcast(df_for_join),
                df_for_lookup[sourcefield] == df_for_join['orig_value'],
                'left'
            ). \
            drop('orig_value')
        df_for_join.unpersist()

        if 'source' not in spec:
            # Drop the original value, because the spec indicates replacing the existing column
            df_for_lookup = df_for_lookup.drop(spec['field'])

        # Rename the new_value column
        df_for_lookup = df_for_lookup.withColumnRenamed('new_value', spec['field'])

        if 'nomatch' in spec:
            # Fill default literal value for unmatched
            df_for_lookup = df_for_lookup.fillna(spec['nomatch'], spec['field'])

        lineage.update_lineage(df_for_lookup, args['source_key'], 'lookup', transform=spec)

    return df_for_lookup


def get_multilookup_data(table_name: str, lookup_group: str, lookup_data_attributes: list, limit: int = None) -> list:
    """Gets data items for multilookup from a DynamoDB table based on lookup_group

    Parameters
    ----------
    table_name
        DynamoDB table from which to get item
    lookup_group
        Filter string to query DynamoDB table lookup results
    lookup_data_attributes
        List of attribute names to retrieve; lookup_item is always retrieved
    limit: optional
        Optional pagination size primarily used for testing

    Returns
    -------
    list
        List of lookup items from DynamoDB suitable for creating a DataFrame
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    query_parameters = {
        'KeyConditionExpression': ( Key('lookup_group').eq(lookup_group) ),
        'ProjectionExpression': ','.join(['lookup_item'] + lookup_data_attributes)
    }
    if limit:
        query_parameters.update({ 'Limit': limit})

    items = []
    response = table.query(**query_parameters)
    while True:
        items += response['Items']
        if 'LastEvaluatedKey' in response:
            # Results are paginated due to size over 1 MB
            response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **query_parameters)
        else:
            break

    if not items:
        raise RuntimeError(f'No lookup data found for lookup_group {lookup_group}')
    return items

def transform_multilookup(df_for_lookup: DataFrame, lookup_spec: list, args: dict, lineage, sc: SparkContext, *extra) -> DataFrame:
    """Add specified multiple values looked up from an external table as new columns

    Parameters
    ----------
    df_for_lookup
        pySpark DataFrame on which to perform the lookups and replacement
    lookup_spec
        List of dictionary objects in the form:
            field: 'NewFieldName' (must be a new field if source is specified)
            lookup: 'LookupData' (DynamoDB column_name value for filter to get looked up values')
            nomatch: 'N/A' (optional, no matches will be empty/null)
    args
        Glue job arguments, from which source_key, multi_lookup_table, and target_database_name are used
    lineage
        Initialized lineage class object from the calling job
    sc
        Spark Context class from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with multi lookup applied
    """
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Pointers to the DataFrame df_for_lookup will be repeatedly dereferenced in the loop (and
    # freed from memory when Spark is able) as the variable is replaced with a new DataFrame
    # with each operation.
    for spec in lookup_spec:

        lookup_items = get_multilookup_data(
            table_name=args['multi_lookup_table'],
            lookup_group=spec['lookup_group'],
            lookup_data_attributes=spec['return_attributes'],
        )

        # Create a DataFrame with lookup values to join to
        df_for_join = spark.createDataFrame(lookup_items)

        # Left join so that no source data is lost
        # After join, drop the now unneeded lookup_item column
        df_for_lookup = df_for_lookup.join(
                broadcast(df_for_join),
                concat_ws('-', *spec['match_columns']) == df_for_join['lookup_item'],
                'left'
            ). \
             drop('lookup_item')

        df_for_join.unpersist()

        if 'nomatch' in spec:
            # Fill default literal value for unmatched
            fill_map = { column_name: spec['nomatch'] for column_name in spec['return_attributes'] }
            df_for_lookup = df_for_lookup.fillna(fill_map)

        lineage.update_lineage(df_for_lookup, args['source_key'], 'multilookup', transform=spec)

    return df_for_lookup