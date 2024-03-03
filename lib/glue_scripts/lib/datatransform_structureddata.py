# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import expr

def transform_jsonexpandarray(df: DataFrame, jsonexpandarray_spec: list, args: dict, lineage, *extra):
    """Function to expand array type columns into multiple rows

    Parameters
    ----------
    jsonexpandarray_spec
        List of fields to expand:
            field: "FieldName" of type Map, Array, or Struct to expand
            source: "SourceFieldName" (optional, if omitted, field will be changed in place)
            index_field: "MyIndex" (name of field to store index from expansion)
    """
    for spec in jsonexpandarray_spec:
        source = spec.get('source', spec['field'])

        # List of fields in schema, except for destination field that will be overwritten
        select_list = [ field_name for field_name in df.columns if field_name != spec['field'] ]

        # Syntax reference: https://issues.apache.org/jira/browse/SPARK-20174
        df = df.selectExpr(*select_list,
            f"posexplode_outer({source}) as (`{spec['index_field']}`, `{spec['field']}`)"
        )
        # Index is 0-based by default, so add 1 to normalize it
        df = df.withColumn(spec['index_field'], expr(f"{spec['index_field']} + 1"))

        lineage.update_lineage(df, args['source_key'], 'jsonexpandarray', transform=spec)

    return df

def transform_jsonexpandmap(df: DataFrame, jsonexpand_spec: list, args: dict, lineage, *extra):
    """Function to expand struct or map type columns into multiple rows

    Parameters
    ----------
    jsonexpandmap_spec
        List of fields to expand:
            field: "FieldName" of type Map, Array, or Struct to expand (used for map values)
            source: "SourceFieldName" (optional, if omitted, field will be changed in place)
            index_field: "MyIndex" (name of field to store index from expansion)
            key_field: "MapKey" (name of field to store map keys)
    """
    for spec in jsonexpand_spec:
        source = spec.get('source', spec['field'])

        if isinstance(df.schema[source].dataType, StructType):
            # Convert StructType to Map (string will be used in select statement)
            try:
                # Assume value schemas for all keys are the same
                value_schema = df.schema[source].dataType[0].dataType.simpleString()
            except:
                raise RuntimeError(f'Column {source} of type Struct does not have key-value pair '
                    ' schema needed for Spark explode')
            source = f"from_json(to_json(`{source}`), 'map<string, {value_schema}>')"

        # List of fields in schema, except for destination field that will be overwritten
        select_list = [ field_name for field_name in df.columns if field_name != spec['field'] ]

        # Syntax reference: https://issues.apache.org/jira/browse/SPARK-20174
        df = df.selectExpr(*select_list,
            f"posexplode_outer({source}) as "
            f"(`{spec['index_field']}`, `{spec['key_field']}`, `{spec['field']}`)"
        )
        # Index is 0-based by default, so add 1 to normalize it
        df = df.withColumn(spec['index_field'], expr(f"{spec['index_field']} + 1"))

        lineage.update_lineage(df, args['source_key'], 'jsonexpandmap', transform=spec)

    return df