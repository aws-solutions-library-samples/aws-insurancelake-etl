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

# For running in local Glue container
sys.path.append(os.path.dirname(__file__) + '/lib')

from glue_catalog_helpers import ( upsert_catalog_table, clean_column_names, generate_spec,
    put_s3_object, clear_partition )
from custom_mapping import custommapping
from datatransform_typeconversion import *
from datatransform_dataprotection import *
from datatransform_stringmanipulation import *
from datatransform_lookup import *
from datatransform_structureddata import *
from datatransform_misc import *
from datatransform_premium import *
from datalineage import DataLineageGenerator
from dataquality_check import DataQualityCheck


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
    'source_path',
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

    _, ext = os.path.splitext(args['base_file_name'])
    source_path = args['source_bucket'] + '/' + args['source_path'] + '/' + args['base_file_name']
    source_key_dashes = args['target_database_name'] + '-' + args['table_name']
    print(f'Source path: {source_path}')

    # Job parameter supplied date partition strategy (object created date)
    partition = {
        # Strongly type job arguments to reduce risk of SQL injection
        'year': f"{int(args['p_year'])}",
        'month': f"{int(args['p_month']):02}",
        'day': f"{int(args['p_day']):02}",
    }

    # Set default schema change detection based on InsuranceLake environment
    if args['environment'] == 'Dev':
        allow_schema_change = 'permissive'
    elif args['environment'] == 'Test':
        allow_schema_change = 'reorder'
    else:
        allow_schema_change = 'strict'

    # Read input/transformation spec file
    txn_spec_prefix = args['txn_spec_prefix_path']
    txn_spec_key = txn_spec_prefix[1:] + source_key_dashes + '.json'
    print(f'Using input/transformation specification from: {txn_spec_key}')
    try:
        spec_data = sc.textFile(f"{args['txn_bucket']}/{txn_spec_key}")
        spec_json_data = json.loads('\n'.join(spec_data.collect()))
    except Exception as e:
        message = e.java_exception.getMessage() if hasattr(e, 'java_exception') else str(e)
        print(f'No input/transformation spec file exists or error reading: {message}')
        spec_json_data = {}

    # Read schema mapping file
    txn_spec_prefix = args['txn_spec_prefix_path']
    txn_map_key = txn_spec_prefix[1:] + source_key_dashes + '.csv'
    print(f'Using schema mapping from: {txn_map_key}')
    try:
        mapping_file = sc.textFile(f"{args['txn_bucket']}/{txn_map_key}").collect()
        reader = csv.DictReader(mapping_file)
        # Implement case insensitive field name matching
        reader.fieldnames = [ field_name.lower() for field_name in reader.fieldnames ]
        mapping_data = [ field_map for field_map in reader ]
    except Exception as e:
        message = e.java_exception.getMessage() if hasattr(e, 'java_exception') else str(e)
        print(f'No transformation mapping file exists or error reading: {message}, skipping')
        mapping_data = {}

    # Read Data Quality rules
    dq_rules_key = 'etl/dq-rules/dq-' + source_key_dashes + '.json'
    print(f'Using data quality rules from: {dq_rules_key}')
    try:
        rules_data = sc.textFile(f"{args['txn_bucket']}/{dq_rules_key}")
        rules_json_data = json.loads('\n'.join(rules_data.collect()))
    except Exception as e:
        print(f'No data quality rules file exists or error reading: {e.java_exception.getMessage()}, skipping')
        rules_json_data = {}

    # Read collected S3 object
    input_spec = spec_json_data.get('input_spec', {})
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

    elif ext.lower() in [ '.xlsx', '.xls', 'xlsm', 'xlm' ]:
        if 'excel' in input_spec:
            sheet_names = input_spec['excel']['sheet_names']
            data_address = input_spec['excel']['data_address']
            # Retrieve and remove the password so it is not printed
            workbook_password = input_spec['excel'].pop('password', None)
            print(f"Using Excel input specification: {input_spec['excel']}")
            header = input_spec['excel'].get('header', True)
        else:
            print('No Excel input specification, using defaults')
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

    elif ext.lower() in [ '.json', '.jsonl' ]:
        multiline = False
        if 'json' in input_spec:
            multiline = input_spec['json'].get('multiline', False)

        initial_df = spark.read.format('json') \
            .option('prefersDecimal', True) \
            .option('allowComments', True) \
            .option('multiLine', multiline) \
            .option('mode', 'PERMISSIVE') \
            .load(source_path)

    elif ext.lower() == '.parquet' or 'parquet' in input_spec:
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
    dataquality = DataQualityCheck(rules_json_data, partition, args, lineage, sc)

    if initial_df.count() == 0:
        raise RuntimeError('No rows of data in source file; aborting')

    # Skip fixed format because mapping is already done
    if 'fixed' not in input_spec:
        # Perform schema mapping if mapping data exists
        if mapping_data:
            initial_df = custommapping(initial_df, mapping_data, args, lineage,
                input_spec.get('strict_schema_mapping'))
            print('Performed field mapping')
        else:
            generated_map_path = args['TempDir'] + '/' + source_key_dashes + '.csv'
            print(f'No mapping found, generated recommended mapping to: {generated_map_path}')
            initial_df, generated_mapping_data = clean_column_names(initial_df)
            put_s3_object(generated_map_path, generated_mapping_data)
            # Log generated mapping data as lineage here
            lineage.update_lineage(initial_df, args['source_key'], 'mapping', map=generated_mapping_data)

    initial_df.cache()
    totransform_df = dataquality.run_data_quality(initial_df, rules_json_data, 'before_transform')
    totransform_df.cache()

    # Perform transforms
    transform_spec = spec_json_data.get('transform_spec', {})
    if transform_spec:
        print(f'Using transformation specification: {transform_spec}')

        for transform in transform_spec.keys():
            # Trim optional suffix to support multiple calls to the same transform
            transform_parts = transform.split(':')
            if 'transform_' + transform_parts[0] not in globals():
                print(f'Transform function transform_{transform_parts[0]} called for in {txn_spec_key} not implemented')
                continue

            transform_func = globals()['transform_' + transform_parts[0]]
            totransform_df = transform_func(
                totransform_df,
                transform_spec[transform],
                args,
                lineage,
                sc
            )
            print(f'Performed {transform} transform')
    else:
        generated_spec_path = args['TempDir'] + '/' + source_key_dashes + '.json'
        print(f'No transformation specification found, generating recommended spec to: {generated_spec_path}')
        generated_spec_data = generate_spec(totransform_df, ext)
        put_s3_object(generated_spec_path, json.dumps(generated_spec_data, indent=4))

    # Add partition columns for daily data load partitioning strategy
    fields_to_add = partition.copy()
    fields_to_add |= {
        # Add State Machine execution ID to DataFrame for lineage tracking
        'execution_id': args['execution_id'],
    }

    transformed_df = transform_literal(totransform_df, fields_to_add, args, lineage)
    transformed_df.cache()
    print(f"Added partition columns {partition}"
          f" and State Machine execution_id column {args['execution_id']}")

    # Record final DataFrame schema
    transformed_schema = list(transformed_df.schema)
    print(str(transformed_schema))

    filtered_df = dataquality.run_data_quality(transformed_df, rules_json_data, 'after_transform')
    filtered_df.cache()
    # Post transform and DQ filter numeric audit
    lineage.update_lineage(filtered_df, args['source_key'], 'numericaudit')

    storage_location = args['target_bucket'] + '/' + args['source_key']

    # The combination of Glue Catalog API upserts and Spark saveAsTable using Hive/Parquet
    # allows the lake to manage the merged/destination schema, and Spark to manage schema
    # consistency without needing to repair tables
    upsert_catalog_table(
        filtered_df,
        args['target_database_name'],
        args['table_name'],
        partition.keys(),
        storage_location,
        table_description=input_spec.get('table_description'),
        # Will raise errors if nonpermissible schema change is detected
        allow_schema_change=input_spec.get('allow_schema_change', allow_schema_change),
    )

    # Explicitly clear the existing partition in S3 and Glue Catalog (i.e. overwrite)
    clear_partition(args['target_database_name'], args['table_name'], partition, glueContext)

    # saveAsTable on new tables fails in strict mode even with only 1 partition
    spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')

    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    spark.conf.set('hive.exec.dynamic.partition', 'true')

    filtered_df.write.partitionBy(*partition.keys()).saveAsTable(
        f"{args['target_database_name']}.{args['table_name']}",
        path=storage_location,
        format='hive',
        fileFormat='parquet',
        mode='append',
    )

    job.commit()
    print('Data successfully written to Cleanse table; job complete')


if __name__ == '__main__':
    main()