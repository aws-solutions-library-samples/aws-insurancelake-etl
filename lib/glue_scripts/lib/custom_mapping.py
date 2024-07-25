# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from rapidfuzz import fuzz
from rapidfuzz import process as fuzz_process
from rapidfuzz.utils import default_process
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import ApplyMapping


def flatten_schema(schema: StructType, prefix: str = '') -> StructType:
    """Recursively iterate over a schema in a DataFrame, handling StructType nested elements,
    and return a flattened list of field names
    """
    flat_schema = []

    for field in schema:
        # Always add the field so that every level of nesting can be referenced
        flat_schema.append(StructField(f'{prefix}{field.name}', field.dataType, field.nullable))

        if isinstance(field.dataType, StructType):
            flat_schema += flatten_schema(field.dataType, prefix=f'{prefix}{field.name}.')

        if ( isinstance(field.dataType, ArrayType) and
                (isinstance(field.dataType.elementType, StructType)) ):
            flat_schema += flatten_schema(
                field.dataType.elementType,
                prefix=f'{prefix}{field.name}.'
            )

    return StructType(flat_schema)


def escape_field_name(name: str) -> str:
    """Escape Spark DataFrame field name with backticks if not already present
    """
    return '`' + name + '`' if '`' not in name else name


def unescape_field_name(name: str) -> str:
    """Remove backticks from Spark DataFrame field name
    """
    return name.replace('`', '')


def custommapping(df: DataFrame, field_mapping_list: list, args: dict, lineage, strict: bool = False) -> DataFrame:
    """Apply a custom field mapping to the data schema in a DataFrame
    Uses RapidFuzz for fuzzy matching: https://maxbachmann.github.io/RapidFuzz/

    Parameters
    ----------
    df
        Spark DataFrame on which to apply schema mapping
    field_mapping_list
        List of dictionary objects with the form:
            'sourcename': 'source_field_name',
            'destname': 'destination_field_name', use Null to drop a column
            'threshold': (optional) fuzzy match minimum confidence level
            'scorer': (optional, must be specified with threshhold) fuzzy match scoring algorithm
    args
        Glue job arguments, from which source_key and execution_id are used
    lineage
        Initialized lineage class object from the calling job
    strict
        If true, a field in the mapping that is missing from the schema will cause an error

    Returns
    -------
    DataFrame
        Spark DataFrame with the custom mapping applied
    """
    unmapped_fields = [ field.name for field in flatten_schema(df.schema) ]

    select_list = []
    for map_row in field_mapping_list:
        in_schema = True
        # Omit fuzzy matching rows from direct mapping
        if not map_row.get('threshold'):
            try:
                # Prepare unmapped_fields for fuzzy matching, and log message
                unmapped_fields.remove(unescape_field_name(map_row['sourcename']))
            except ValueError:
                in_schema = False

            # null mappings are explicit so check separately so we can remove them from unmapped
            if map_row['destname'].lower() != 'null' and (in_schema or strict):
                select_list.append(
                    col(escape_field_name(map_row['sourcename'])).alias(map_row['destname'])
                )

    # Perform fuzzy match with rapidfuzz, specified sort, and column alias
    if unmapped_fields:
        for map_row in field_mapping_list:
            if map_row.get('threshold'):
                match, score, _ = fuzz_process.extractOne(
                    map_row['sourcename'],
                    unmapped_fields,
                    processor=default_process,
                    scorer=getattr(fuzz, map_row['scorer'])
                )
                if score >= int(map_row['threshold']):
                    select_list.append(col(escape_field_name(match)).alias(map_row['destname']))
                    unmapped_fields.remove(match)
                    # Add match to mutable dictionary object to use for lineage
                    map_row['match'] = match

                print(f"Fuzzy matched {map_row['sourcename']} with column {match} and score {score}")

    if unmapped_fields:
        print(f'Discarded unmapped fields {unmapped_fields}')

    lineage.update_lineage(df, args['source_key'], 'mapping', map=field_mapping_list)
    return df.select(select_list)


def custommapping_with_glue(dyf: DynamicFrame, field_mapping_list: list, args: dict, lineage) -> DynamicFrame:
    """Apply a custom field mapping to the data schema in a Glue DynamicFrame
    """
    # NOTE: Fuzzy matching mappings will be skipped
    prepared_map = [ (map_row['sourcename'], map_row['destname'])
        for map_row in field_mapping_list
        if not map_row['threshold'] and map_row['destname'].lower() != 'null' ]

    # NOTE: ApplyMapping.apply "optimizes" DecimalTypes, reorders fields, and re-samples columns
    #   resulting in Null/Void column types
    # NOTE: Discarded unmapped fields will not be logged
    lineage.update_lineage(dyf, args['source_key'], 'mapping', map=field_mapping_list)
    return ApplyMapping.apply(
        info='Field name mapping transform',
        frame=dyf,
        mappings=prepared_map,
        transformation_ctx=f"{args['execution_id']}-custommapping",
    )