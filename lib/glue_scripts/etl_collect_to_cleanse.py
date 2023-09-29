# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import sys
import csv
import json
import os
from functools import reduce

from pyspark.context import SparkContext
from pyspark.sql.functions import trim
from pyspark.sql.utils import IllegalArgumentException

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# For running in local Glue container
sys.path.append(os.path.dirname(__file__) + '/lib')

from glue_catalog_helpers import upsert_catalog_table, clean_column_names, generate_spec, put_s3_object
from custom_mapping import custommapping
from datatransform_typeconversion import *
from datatransform_dataprotection import *
from datatransform_regex import *
from datatransform_lookup import *
from datatransform_premium import *
from datalineage import DataLineageGenerator

try:
    # Try/Except block for running in AWS-provided Glue container until Glue DQ support is added
    from dataquality_check import check_dataquality_warn, check_dataquality_quarantine, check_dataquality_halt
except ModuleNotFoundError:
    pass

expected_arguments = [
    'JOB_NAME',
    'environment',
    'TempDir',
    'txn_bucket',
    'txn_spec_prefix_path',
    'source_bucket',
    'target_bucket',
    'hash_value_table',
    'value_lookup_table',
    'multi_lookup_table',
    'dq_results_table',
    'state_machine_name',
    'execution_id',
    'source_key',
    'target_database_name',
    'table_name',
    'base_file_name',
    'p_year',
    'p_month',
    'p_day',
]

# Handle optional arguments
for arg in sys.argv:
    if '--data_lineage_table' in arg:
        expected_arguments.append('data_lineage_table')


def main():
    args = getResolvedOptions(sys.argv, expected_arguments)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    filename, ext = os.path.splitext(args['base_file_name'])
    source_path = args['source_bucket'] + '/' + args['source_key'] + '/' + args['base_file_name']
    print(f'Source path: {source_path}')

    # Set default schema change detection based on InsuranceLake environment
    if args['environment'] == 'Dev':
        allow_schema_change = 'permissive'
    elif args['environment'] == 'Test':
        allow_schema_change = 'reorder'
    else:
        allow_schema_change = 'strict'

    # Read input/transformation spec file
    txn_spec_prefix = args['txn_spec_prefix_path']
    txn_spec_key = txn_spec_prefix[1:] + args['target_database_name'] + '-' + args['table_name'] + '.json'
    print(f'Using input/transformation specification from: {txn_spec_key}')
    try:
        spec_data = sc.textFile(f"{args['txn_bucket']}/{txn_spec_key}")
        spec_json_data = json.loads('\n'.join(spec_data.collect()))
    except Exception as e:
        print(f'No input/transformation spec file exists or error reading: {e.java_exception.getMessage()}')
        spec_json_data = None

    # Read schema mapping file
    txn_spec_prefix = args['txn_spec_prefix_path']
    txn_map_key = txn_spec_prefix[1:] + args['target_database_name'] + '-' + args['table_name'] + '.csv'
    print(f'Using schema mapping from: {txn_map_key}')
    try:
        mapping_file = sc.textFile(f"{args['txn_bucket']}/{txn_map_key}").collect()
        reader = csv.DictReader(mapping_file)
        # Implement case insensitive field name matching
        reader.fieldnames = [ field_name.lower() for field_name in reader.fieldnames ]
        mapping_data = [ field_map for field_map in reader ]
    except Exception as e:
        print(f'No transformation mapping file exists or error reading: {e.java_exception.getMessage()}, skipping')
        mapping_data = None

    # Read collected S3 object
    input_spec = spec_json_data.get('input_spec', {}) if spec_json_data else {}
    if 'fixed' in input_spec:
        # mapping_data should be SourceName (ignored for fixed format), DestinationName, (Column) Width
        initial_df = spark.read.text(source_path)
        initial_df = initial_df.select(
            [
                trim(initial_df.value.substr(
                    # Calculate start index based on all prior column widths
                    reduce(lambda a, b: a + int(b['width']), mapping_data[:index], 0) + 1,
                    int(field_data['width'])
                )).alias(field_data['destname'])
                    for index, field_data in enumerate(mapping_data)
                        if field_data['destname'].lower() != 'null'
            ]
        )
        print('Performed fixed width file load and field mapping')

    elif ext.lower() in [ '.xlsx', '.xls' ]:
        if 'excel' in input_spec:
            sheet_names = input_spec['excel']['sheet_names']
            data_address = input_spec['excel']['data_address']
            # Retrieve and remove the password so it is not printed
            workbook_password = input_spec['excel'].pop('password', None)
            print(f"Using Excel input specification: {input_spec['excel']}")
            header = input_spec['excel'].get('header', True)
        else:
            print(f'No Excel input specification, using defaults')
            sheet_names = [ '0' ]
            data_address = 'A1'
            header = True
            workbook_password = None

        for sheet_name in sheet_names:
            try:
                initial_df = spark.read.format('excel') \
                    .option('header', header) \
                    .option('inferSchema', 'true') \
                    .option('mode', 'PERMISSIVE') \
                    .option("usePlainNumberFormat", 'true') \
                    .option('dataAddress', f"'{sheet_name}'!{data_address}") \
                    .option('workbookPassword', workbook_password) \
                    .load(source_path)
                print(f"Found table at '{sheet_name}'!{data_address}")
                break
            except IllegalArgumentException:
                pass

        if 'initial_df' not in locals():
            raise RuntimeError(f'None of sheet names {sheet_names} found in Excel workbook')

    elif ext.lower() == '.parquet':
        # TODO: Support partitioned Parquet folder structures (read correctly and repartition later)
        initial_df = spark.read.format('parquet').load(source_path)

    else:
        # Comma delimited is default, with support for tab delimited if specified
        delimiter = ','
        header = True
        if 'csv' in input_spec:
            header = input_spec['csv'].get('header', True)
        if 'tsv' in input_spec:
            delimiter = '\t'
            header = input_spec['tsv'].get('header', True)

        initial_df = spark.read.format('csv') \
            .option('header', header) \
            .option('delimiter', delimiter) \
            .option('inferSchema', 'true') \
            .option('mode', 'PERMISSIVE') \
            .load(source_path)

    initial_df.cache()
    lineage = DataLineageGenerator(args)
    lineage.update_lineage(initial_df, args['source_key'], 'read')
    lineage.update_lineage(initial_df, args['source_key'], 'numericaudit')

    if initial_df.count() == 0:
        raise RuntimeError('No rows of data in source file; aborting')

    # Skip fixed format because mapping is already done
    if 'fixed' not in input_spec:
        # Perform schema mapping if mapping data exists
        if mapping_data:
            initial_df = custommapping(initial_df, mapping_data, args, lineage)
            print('Performed field mapping')
        else:
            generated_map_path = args['TempDir'] + '/' + args['target_database_name'] + '-' + args['table_name'] + '.csv'
            print(f'No mapping found, generated recommended mapping to: {generated_map_path}')
            initial_df, generated_mapping_data = clean_column_names(initial_df)
            put_s3_object(generated_map_path, generated_mapping_data)

    totransform_df = initial_df
    totransform_df.cache()

    # Perform transforms
    if spec_json_data and 'transform_spec' in spec_json_data:
        transform_spec = spec_json_data['transform_spec']
        print(f'Using transformation specification: {transform_spec}')

        for transform in transform_spec.keys():
            if 'transform_' + transform not in globals():
                print(f'Transform function transform_{transform} called for in {txn_map_key} not implemented')
                continue

            transform_func = globals()['transform_' + transform]
            totransform_df = transform_func(
                totransform_df,
                transform_spec[transform],
                args,
                lineage,
                sc
            )
            print(f'Performed {transform} transform')
    else:
        generated_spec_path = args['TempDir'] + '/' + args['target_database_name'] + '-' + args['table_name'] + '.json'
        print(f'No transformation specification found, generating recommended spec to: {generated_spec_path}')
        generated_spec_data = generate_spec(totransform_df, ext)
        put_s3_object(generated_spec_path, json.dumps(generated_spec_data, indent=4))

    fields_to_add = {
        # Add State Machine execution ID to DataFrame for lineage tracking
        'execution_id':  args['execution_id'],
        # Add partition columns for daily data load partitioning strategy
        'year': args['p_year'],
        'month': args['p_month'],
        'day': args['p_day'],
    }
    transformed_df = transform_literal(totransform_df, fields_to_add, args, lineage)
    transformed_df.cache()
    print('Added partition columns and State Machine execution_id column')

    # Record final DataFrame schema
    transformed_schema = list(transformed_df.schema)
    print(str(transformed_schema))

    # Run Data Quality rules and take appropriate action
    dq_rules_key = 'etl/dq-rules/dq-' + args['target_database_name'] + '-' + args['table_name'] + '.json'
    print(f'Using data quality rules from: {dq_rules_key}')
    try:
        rules_data = sc.textFile(f"{args['txn_bucket']}/{dq_rules_key}")
        rules_json_data = json.loads('\n'.join(rules_data.collect()))
    except Exception as e:
        print(f'No data quality rules file exists or error reading: {e.java_exception.getMessage()}, skipping')
        rules_json_data = None

    # NOTE: Only quarantine rules will change filtered Dataframe
    filtered_df = transformed_df
    if rules_json_data:
        if 'dataquality_check' not in sys.modules:
            print('Skipping data quality rules because awsgluedq library is not available')
        else:
            dq_rules = rules_json_data['dq_rules']
            print(f'Using data quality rules: {dq_rules}')
            fordq_dyf = DynamicFrame.fromDF(transformed_df, glueContext, f"{args['execution_id']}-fordq_dyf")
            if 'warn_rules' in dq_rules:
                print(f'Glue Data Quality Warn-on-Failure results:')
                check_dataquality_warn(fordq_dyf, dq_rules['warn_rules'], args, sc)
            if 'quarantine_rules' in dq_rules:
                print(f'Glue Data Quality Quarantine-on-Failure results:')
                filtered_df.unpersist()
                filtered_df = check_dataquality_quarantine(fordq_dyf, dq_rules['quarantine_rules'], args, sc)
                lineage.update_lineage(filtered_df, args['source_key'], 'dataqualityquarantine', transform=dq_rules['quarantine_rules'])
            if 'halt_rules' in dq_rules:
                # NOTE: Original DynamicFrame is used to process halt rules, so even rows filtered above can trigger abort
                print(f'Glue Data Quality Halt-on-Failure results:')
                check_dataquality_halt(fordq_dyf, dq_rules['halt_rules'], args, sc)

    # Post transform and DQ filter numeric audit
    filtered_df.cache()
    lineage.update_lineage(filtered_df, args['source_key'], 'numericaudit')

    # Build Glue Catalog for data
    storage_location = args['target_bucket'] + '/' + args['target_database_name'] + '/' + args['table_name']
    allow_schema_change = input_spec.get('allow_schema_change', allow_schema_change)
    upsert_catalog_table(
            filtered_df,
            args['target_database_name'],
            args['table_name'],
            ['year', 'month', 'day'],
            storage_location,
            allow_schema_change,
        )

    # Set dynamic overwrite to preserve historical partitions, but overwrite current partition
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    spark.conf.set('hive.exec.dynamic.partition', 'true')
    spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')

    # Write the data with new schema and partitioning to the target location in hadoop parquet
    filtered_df.write.partitionBy('year', 'month', 'day').save(storage_location, format='parquet', mode='overwrite')

    # Do not use RECOVER PARTITIONS because we already updated the Glue Catalog if it was needed
    spark.sql(f"ALTER TABLE {args['target_database_name']}.{args['table_name']} ADD IF NOT EXISTS PARTITION"
        f"(year='{args['p_year']}', month='{args['p_month']}', day='{args['p_day']}')")

    job.commit()


if __name__ == '__main__':
    main()