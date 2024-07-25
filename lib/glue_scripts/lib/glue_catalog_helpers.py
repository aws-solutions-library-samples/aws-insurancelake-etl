# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import io
import csv
from urllib.parse import urlparse
import re
import boto3
import botocore
from pyspark.context import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DoubleType, FloatType, NullType
from pyspark.sql.functions import col
from awsglue.context import GlueContext

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
    response
        get_table response dictionary, or None if table does not exist
    """
    try:
        glue_client = boto3.client('glue')
        response = glue_client.get_table(DatabaseName=target_database, Name=table_name)
        return response
    except glue_client.exceptions.EntityNotFoundException:
        pass


def create_database(database_name: str, database_description: str = None):
    """Function to create catalog database if does not exists

    Parameters
    ----------
    database_name
        Glue Catalog database name for creating or confirming existance
    database_description
        Description of the database to set in Glue Catalog
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

    database_input = { 'Name': database_name }
    if database_description:
        database_input.update({ 'Description': database_description })

    if response is None:
        # Database does not exist, so we should create it
        response = glue_client.create_database(DatabaseInput=database_input)
        print(f'create_database response: {response}')


class GlueCatalogDecimal:
    """Class to help compare Spark DecimalType schema
    Implements only the methods needed for schema evolution logic
    """
    def __init__(self, decimalstring: str):
        decimal_re = re.compile(r'decimal\((\d+),(\d+)\)')
        match = decimal_re.match(decimalstring)
        if not match:
            raise RuntimeError(f'GlueCatalogDecimal initialized with invalid decimal string {decimalstring}')
        self.precision = int(match.group(1))
        self.scale = int(match.group(2))

    def __gt__(self, other):
        return (self.precision > other.precision and self.scale >= other.scale) or \
            (self.precision >= other.precision and self.scale > other.scale)

def check_schema_change(existing_schema: list, new_schema: list, allow_schema_change: str) -> bool:
    """Function to check if schema change is allowed based on allow_schema_change setting
    NOTE: Field name comparison is case-sensitive; normalize case for case-insensitive check

    Parameters
    ----------
    existing_schema
        Schema that already exists in the data lake
        List of Dict objects containing, at least, elements Name and Type
        Matches format of the Columns object within StorageDescriptor for the Glue Catalog API
    new_schema
        Incoming (new, possibly changed) data file schema; same format as existing schema
    allow_schema_changes
        strict - no changes
        reorder - allow reordering of columns
        evolve - allow reorder, adding, changing certain data types
        permissive - allow everything (may break Spark and Athena queries)

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

    existing_schema_map = { field_def['Name']: field_def for field_def in existing_schema }
    existing_schema_set = set(existing_schema_map.keys())
    new_schema_map = { field_def['Name']: field_def for field_def in new_schema }
    new_schema_set = set(new_schema_map.keys())

    if allow_schema_change == 'reorder':
        # Reorder (check length for duplicate fields in new_schema which will fail)
        print('Reorder schema change: checking')
        return existing_schema_set == new_schema_set \
            and len(new_schema) == len(new_schema_set)

    if allow_schema_change == 'evolve':
        # Allow reordering columns, adding columns, Athena supported data types changes
        evolve_check = True
        if len(new_schema) != len(new_schema_set):
            print('Evolve schema change: duplicate fields not allowed')
            evolve_check = False

        if existing_schema_set - new_schema_set:
            print('Evolve schema change: field deletion not allowed')
            evolve_check = False

        common_fields = new_schema_set.intersection(existing_schema_set)
        for field_name in common_fields:
            old_type = existing_schema_map[field_name]['Type'].lower()
            new_type = new_schema_map[field_name]['Type'].lower()

            if old_type == new_type \
                or old_type == 'string' and new_type in [ 'byte', 'tinyint', 'smallint', 'int', 'bigint' ] \
                or old_type == 'byte' and new_type in [ 'tinyint', 'smallint', 'int', 'bigint' ] \
                or old_type == 'tinyint' and new_type in [ 'smallint', 'int', 'bigint' ] \
                or old_type == 'smallint' and new_type in [ 'int', 'bigint' ] \
                or old_type == 'int' and new_type == 'bigint' \
                or old_type == 'float' and new_type == 'double' \
                or old_type == 'date' and new_type == 'timestamp':
                continue

            if old_type.startswith('decimal') and new_type.startswith('decimal') and \
                GlueCatalogDecimal(new_type) > GlueCatalogDecimal(old_type):
                continue

            # Allowed data type changes have been checked, must be a non-permitted change
            evolve_check = False

        # Implicitly allow reorder, new field, field metadata change, no change
        return evolve_check

    raise RuntimeError('Input schema configuration is not valid: '
        f'Unsupported value for allow_schema_change {allow_schema_change}')


def upsert_catalog_table(
        df: DataFrame,
        target_database: str,
        table_name: str,
        partition_keys: list,
        storage_location: str,
        database_description: str = None,
        table_description: str = None,
        allow_schema_change: str = 'strict',
    ):
    """Function to upsert Glue Data Catalog table

    Parameters
    ----------
    df
        Spark Dataframe from which to gather schema
    target_database
        Glue Catalog database name to use
    table_name
        Glue Catalog table name to use
    partition_keys
        list of key names to use as partition keys
    storage_location
        S3 path to data stored in parquet files
    database_description
        Description of the database to set in Glue Catalog
    table_description
        Description of the table to set in Glue Catalog
    allow_schema_change: optional
        Schema evolution setting to be used by check_schema_change()
    """
    # Always run this because most save operations do not create the database
    create_database(target_database, database_description)

    schema = []
    partition_schema = []
    for field_name, field_type in df.dtypes:
        # Glue Catalog will transparently convert all column names to lowercase
        # so do it explicitly so that schema change detection works
        field_def = { 'Name': field_name.lower(), 'Type': field_type }

        if 'comment' in df.schema[field_name].metadata:
            field_def.update({ 'Comment': df.schema[field_name].metadata['comment'] })

        # Partition keys should only be defined in the partition keys schema
        if field_name in partition_keys:
            partition_schema.append(field_def)
        else:
            schema.append(field_def)

    # Derived partition_schema should match specified partition keys
    if set(partition_keys) != { field['Name'] for field in partition_schema }:
        raise RuntimeError(f'Some or all specified partition keys {partition_keys} not found in Dataframe schema')

    serde_info = {
        'Parameters': {
            'serialization.format': '1'
        },
        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    }
    storage_descriptor = {
        'Columns': schema,
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'SerdeInfo': serde_info,
        'Location': storage_location
    }
    table_input = {
        'Name': table_name,
        'StorageDescriptor': storage_descriptor,
        'Parameters': {
            'classification': 'Parquet',
            'SourceType': 's3',
            'SourceTableName': table_name,
            'TableVersion': '0',
        },
        'TableType': 'EXTERNAL_TABLE',
        'PartitionKeys': partition_schema
    }
    if table_description:
        table_input.update({ 'Description': table_description })

    glue_client = boto3.client('glue')
    table_response = table_exists(target_database, table_name)
    if not table_response:
        print(f'Target table name: {target_database}.{table_name} does not exist, no schema to compare')
        print(f'Creating target table: {target_database}.{table_name}')
        glue_client.create_table(DatabaseName=target_database, TableInput=table_input)
        return

    # Compare new schema to existing Glue catalog
    partitions_equal = ( table_response['Table'].get('PartitionKeys', []) == partition_schema )
    if not partitions_equal:
        raise RuntimeError('Nonpermissible change to partition keys detected with existing '
            f'Glue catalog {target_database}.{table_name}')

    existing_column_schema = table_response['Table']['StorageDescriptor'].get('Columns', [])
    if existing_column_schema == schema:
        print(f'No schema changes detected with: {target_database}.{table_name}')
        return

    if check_schema_change(existing_column_schema, schema, allow_schema_change):
        # Schema changes are permissible
        print(f'Updating target table schema: {target_database}.{table_name}')
        glue_client.update_table(DatabaseName=target_database, TableInput=table_input)
    else:
        print(f'Schema change between existing partitions and new partitions '
            'not permissible by {allow_schema_change} allow_schema_changes setting; aborting')
        print(f"Existing schema: {existing_column_schema}")
        print(f'New schema: {schema}')
        raise RuntimeError('Nonpermissible schema change detected with existing '
            f'Glue catalog {target_database}.{table_name}')


def clear_partition(database_name: str, table_name: str, partition: dict, sc: SparkContext):
    """Explicitly clear the existing partition in S3 and Glue Catalog (i.e. overwrite)

    Parameters
    ----------
    database_name
        Glue Catalog database name
    table_name
        Glue Catalog table name
    partition
        key value pairs of each partition column and the predict value to clear
    sc
        Spark Context class from the calling job
    """
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    try:
        glueContext.purge_table(
            database=database_name,
            table_name=table_name,
            options={
                'partitionPredicate': ' AND '. \
                    join([ f"{name} == '{value}'" for name, value in partition.items() ]),
                'retentionPeriod': 0
            }
        )
    except Exception as e:
        # "Pushdown predicate cannot be resolved" error occurs when table is just created above
        # and has no data (thus no partitions)
        if "User's pushdown predicate" in e.java_exception.getMessage() or 'EntityNotFoundException' in e.java_exception.getMessage():
            print('No existing partition data to clear')
        else:
            raise e

    spark.sql(f'ALTER TABLE {database_name}.{table_name} DROP IF EXISTS PARTITION '
        '( ' + ', '.join([ f"{name} = '{value}'" for name, value in partition.items() ]) + ' )')


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
    field_map_rows = []
    for field in df.schema:
        column = col('`' + field.name + '`')

        # Remove leading/trailing whitespace, and trim length to max allowed
        new_name = field.name.strip()[:255].lower()

        for char in ',;{}()\n\r\t=':
            # Remove bad Parquet column naming characters
            new_name = new_name.replace(char, '')
        for char in ' .:':
            # Replace characters for Parquet rules/conventions
            new_name = new_name.replace(char, '_')

        # Consolidate excessive character replacement
        new_name = new_name.replace('_-_', '-').replace('__', '_').replace('__', '_')

        if field.name != new_name:
            column = column.alias(new_name)
        cols.append(column)

        # Fields appear in the recommended map regardless of whether they were aliased
        field_map_rows.append({ 'sourcename': field.name, 'destname': new_name })

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

    transform_spec = { 'date': [], 'timestamp': [], 'changetype': {} }
    for field in df.schema:
        if field.dataType in [ DoubleType(), FloatType() ]:
            transform_spec['changetype'].update({ field.name: 'decimal(16,2)' })

        if 'date' in field.name.lower():
            transform_spec['date'].append({ 'field': field.name, 'format': 'MM/dd/yy' })

        if 'time' in field.name.lower():
            transform_spec['timestamp'].append({ 'field': field.name, 'format': 'yyyy-MM-dd HH:mm:ss' })

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
        writer = csv.DictWriter(string_output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        data = string_output.getvalue()
    parsed_uri = urlparse(uri)
    s3 = boto3.client('s3')
    s3.put_object(Body=str(data), Bucket=parsed_uri.netloc, Key=parsed_uri.path[1:])


def clean_nulltypes(df: DataFrame) -> DataFrame:
    """Convert Void/NullType columns to string, typically created from DynamicFrame operations
    """
    cols = []
    for field in df.schema:
        column = col('`' + field.name + '`')
        if field.dataType == NullType():
            # Cast null and void type columns to string (safest conversion)
            column = column.cast('string')
        cols.append(column)
    return df.select(cols)