# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import boto3
import botocore
import io
import csv
from urllib.parse import urlparse
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DoubleType, NullType
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

def table_exists(target_database: str, table_name: str) -> bool:
    """Function to check if table exists returns true/false
    All other exceptions from get_table are raised normally

    Parameters
    ----------
    target_database
        Glue Catalog database in which table exists
    table_name
        Glue Catalog table to check for existance

    Returns
    -------
    bool
        whether table exists or not
    """
    try:
        glue_client = boto3.client('glue')
        glue_client.get_table(DatabaseName=target_database, Name=table_name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False


def create_database(database_name: str):
    """Function to create catalog database if does not exists

    Parameters
    ----------
    database_name
        Glue Catalog database name for creating or confirming existance
    """
    response = None
    glue_client = boto3.client('glue')

    try:
        response = glue_client.get_database(
            Name=database_name
        )
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            print(f'The requested database: {database_name} was not found, creating it')
        else:
            raise RuntimeError(f"Error getting database {database_name}: {error.response['Error']['Message']}")

    if response is None:
        # Database does not exist, so we should create it
        response = glue_client.create_database(
            DatabaseInput={
                'Name': database_name
            }
        )
        print(f'create_database response: {response}')


def check_schema_change(existing_schema: list, new_schema: list, allow_schema_change: str) -> bool:
    """Function to check if schema change is allowed based on allow_schema_change setting

    Parameters
    ----------
    existing_schema
        Schema that already exists in the data lake
    new_schema
        Incoming data file schema that may be different from the existing schema
    allow_schema_changes
        strict - no changes
        reorder - allow reordering of columns
        evolve - allow reorder, adding, changing certain data types
        permissive - allow everything (may break Athena queries)

    Returns
    -------
    bool
        New schema passes the rules for the provided schema change setting

    """
    if allow_schema_change == 'permissive':
        # Permissive
        print('Permissive schema change: allowed')
        return True

    if allow_schema_change == 'strict':
        # Strict
        print('Strict schema change: not allowed')
        return existing_schema == new_schema

    existing_schema_set = set([ field_def['Name'] for field_def in existing_schema ])
    new_schema_set = set([ field_def['Name'] for field_def in new_schema ])
    if allow_schema_change == 'reorder':
        # Reorder (duplicate fields in new_schema will fail)
        print('Reorder schema change: checking')
        return existing_schema_set == new_schema_set \
            and len(new_schema) == len(new_schema_set)

    if allow_schema_change == 'evolve':
        pass
        # TODO: Finish implementation
        # field additions: new_schema_set - existing_schema_set
        # field removals: existing_schema_set - new_schema_set

    raise RuntimeError(f'Unsupported value for allow_schema_change {allow_schema_change}')


def upsert_catalog_table(
        df: any, 
        target_database: str, 
        table_name: str, 
        partition_keys: list, 
        storage_location: str, 
        allow_schema_change: str = 'strict',
    ):
    """Function to upsert Glue Data Catalog table

    Parameters
    ----------
    df
        Spark Dataframe or Glue DynamicFrame from which to gather schema
    target_database
        Glue Catalog database name to use
    table_name
        Glue Catalog table name to use
    partition_keys
        list of key names to use as partition keys
    storage_location
        S3 path to data stored in parquet files
    allow_schema_change: optional
        Schema evolution setting to be used by check_schema_change()
    """
    create_database(target_database)

    df_schema = df.toDF().dtypes if type(df) == DynamicFrame else df.dtypes
    schema = [
        # Glue Catalog will transparently convert all column names to lowercase
        # so do it explicitly so that schema change detection works
        { 'Name': field[0].lower(), 'Type': field[1] }
        for field in df_schema
            # Skip explicitly defining partition keys in the catalog
            # schema if they already exist in the dataframe schema
            if field[0] not in partition_keys
    ]

    input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    output_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    serde_info = {
        'Parameters': {
            'serialization.format': '1'
        },
        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    }
    storage_descriptor = {
        'Columns': schema,
        'InputFormat': input_format,
        'OutputFormat': output_format,
        'SerdeInfo': serde_info,
        'Location': storage_location
    }
    partition_key = [
        { 'Name': key_name, 'Type': 'string' } for key_name in partition_keys
    ]
    table_input = {
        'Name': table_name,
        'StorageDescriptor': storage_descriptor,
        'Parameters': {
            'classification': 'PARQUET',
            'SourceType': 's3',
            'SourceTableName': table_name,
            'TableVersion': '0'
        },
        'TableType': 'EXTERNAL_TABLE',
        'PartitionKeys': partition_key
    }

    print(f'Upserting Glue Catalog schema: {schema}')
    try:
        glue_client = boto3.client('glue')
        if not table_exists(target_database, table_name):
            print(f'Target Table name: {table_name} does not exist')
            glue_client.create_table(DatabaseName=target_database, TableInput=table_input)
            return

        # Compare new schema to existing Glue catalog
        table_response = glue_client.get_table(DatabaseName=target_database, Name=table_name)
        schema_equal = ( table_response['Table']['StorageDescriptor']['Columns'] == schema )
        if schema_equal:
            print('No schema changes detected')
            return

        if check_schema_change(table_response['Table']['StorageDescriptor']['Columns'], schema, allow_schema_change):
            print(f'Trying to update Target Table: {table_name}')
            glue_client.update_table(DatabaseName=target_database, TableInput=table_input)
            return

        # Abort if there are changes because it breaks Athena queries to have partitions 
        # with different schema. Even though we are updating the same Glue catalog, the
        # existing partitions will retain the original schema. and be in conflict.
        print(f'Schema change not permissible by {allow_schema_change} alllow_schema_changes '
            'setting detected between existing partitions and new partitions; aborting')
        print(f"Existing schema: {table_response['Table']['StorageDescriptor']['Columns']}")
        print(f'New schema: {schema}')
        raise RuntimeError(f'Nonpermissible schema change detected with existing Glue catalog {target_database}.{table_name}')

    except botocore.exceptions.ClientError as error:
        print(f'Glue job client process failed: {error}')
        raise error


def clean_column_names(df: DataFrame) -> tuple:
    """Rename DataFrame column names to conform to Parquet/Athena naming requirements

    Parameters
    ----------
    df
        Spark DataFrame to use for field names and initial data types

    Returns
    -------
    DataFrame, field_map
        DataFrame with cleaned column names, and dictionary of mapping operations performed
    """
    cols = []
    field_map_rows = [[ 'SourceName', 'DestName' ]]
    for field in df.schema:
        column = col('`' + field.name + '`')

        # Remove leading/trailing whitespace, and trim length to max allowed
        new_name = field.name.strip()[:255].lower()

        for char in ',;{}()\n\t=':
            # Remove bad Parquet column naming characters
            new_name = new_name.replace(char, '')
        for char in ' .':
            # Replace characters for Parquet rules/conventions
            new_name = new_name.replace(char, '_')

        # Consolidate excessive character replacement
        new_name = new_name.replace('_-_', '-').replace('__', '_').replace('__', '_')

        if field.name != new_name:
            column = column.alias(new_name)
        cols.append(column)

        # Fields appear in the recommended map regardless of whether they were aliased
        field_map_rows.append([ field.name, new_name ])

    return df.select(cols), field_map_rows


def generate_spec(df: DataFrame, input_file_extension: str) -> dict:
    """Generate a suggested transform spec file (primarily type conversions)

    Parameters
    ----------
    df
        Spark DataFrame to use for field names and initial data types
    input_file_extension
        File extension of input file to determine if input spec is needed

    Returns
    -------
    dict
        Dictionary of recommended transform specifications
    """
    input_spec = {}
    if input_file_extension.lower() in [ '.xlsx', '.xls' ]:
        input_spec.update({
            'excel': { 'sheet_names': [ '0' ], 'data_address': 'A1', 'header': True }
        })

    transform_spec = { 'date': [], 'decimal': [] }
    for field in df.schema:
        if field.dataType == DoubleType():
            transform_spec['decimal'].append({ 'field': field.name, 'format': '16,2' })

        if 'date' in field.name.lower():
            transform_spec['date'].append({ 'field': field.name, 'format': 'MM/dd/yy' })

    return { 'input_spec': input_spec, 'transform_spec': transform_spec }


def put_s3_object(uri: str, data: any):
    """Save data to S3 object bypassing Spark write operation

    Parameters
    ----------
    uri
        Full HDFS S3 path URI to create
    data
        list or string of data to write to S3 (list will be converted to CSV)
    """
    if type(data) is list:
        string_output = io.StringIO()
        writer = csv.writer(string_output)
        writer.writerows(data)
        data = string_output.getvalue()
    parsed_uri = urlparse(uri)
    s3 = boto3.client('s3')
    s3.put_object(Body=str(data), Bucket=parsed_uri.netloc, Key=parsed_uri.path[1:])


def clean_nulltypes(df: DataFrame) -> DataFrame:
    """Convert Void/NullType columns typically created from DynamicFrame operations to string
    """
    cols = []
    for field in df.schema:
        column = col('`' + field.name + '`')
        if field.dataType == NullType():
            # Cast null and void type columns to string (safest conversion)
            column = column.cast('string')
        cols.append(column)
    return df.select(cols)