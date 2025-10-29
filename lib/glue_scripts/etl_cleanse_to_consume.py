# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import sys
import os
import re
import json

from pyspark.context import SparkContext

from awsglue.utils import getResolvedOptions, GlueArgumentError
from awsglue.context import GlueContext
from awsglue.job import Job

# For running in local Glue container
sys.path.append(os.path.dirname(__file__) + '/lib')
from glue_catalog_helpers import upsert_catalog_table
from dataquery import athena_execute_query, redshift_execute_query
from datalineage import DataLineageGenerator
from dataquality_check import DataQualityCheck


def get_redshift_connection_parameter(args: list):
    connection_types = {
        'redshift_cluster_id': 'cluster_id',        # provisioned
        'redshift_workgroup_name': 'workgroup_name' # serverless
    }

    for arg_name, param_name in connection_types.items():
        if arg_name in args:
            if 'redshift_database' not in args:
                raise GlueArgumentError(f'argument --redshift_database is required when using --{arg_name}')

            return { param_name: args[arg_name] }

    raise GlueArgumentError('argument --redshift_cluster_id or --redshift_workgroup_name is'
                            ' required when executing Amazon Redshift SQL')


expected_arguments = [
    'JOB_NAME',
    'environment',
    'TempDir',
    'txn_bucket',
    'txn_sql_prefix_path',
    'source_bucket',
    'target_bucket',
    'dq_results_table',
    'state_machine_name',
    'execution_id',
    'source_key',
    'source_database_name',
    'target_database_name',
    'table_name',
    'base_file_name',
    'p_year',
    'p_month',
    'p_day',
]

optional_arguments = [
    'data_lineage_table',
    'redshift_cluster_id',
    'redshift_workgroup_name',
    'redshift_database',
]


def main():
    # Make a local copy to help unit testing (the global is shared across tests)
    local_expected_arguments = expected_arguments.copy()

    # Handle optional arguments
    for arg in sys.argv:
        for optional_arg in optional_arguments:
            if f'--{optional_arg}' in arg:
                local_expected_arguments.append(optional_arg)

    args = getResolvedOptions(sys.argv, local_expected_arguments)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Use the Spark DataFrame to Pandas DataFrame optimized conversion
    spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', True)

    # Hive database and table names are case insensitive
    target_table = args['table_name']
    substitution_data = args.copy()
    sql_prefix = args['txn_sql_prefix_path']
    source_key_dashes = args['source_database_name'] + '-' + args['table_name']

    # Job parameter supplied date partition strategy
    partition = {
        # Strongly type job arguments to reduce risk of SQL injection
        'year': f"{int(args['p_year'])}",
        'month': f"{int(args['p_month']):02}",
        'day': f"{int(args['p_day']):02}",
    }

    # Read Data Quality rules
    dq_rules_key = 'etl/dq-rules/dq-' + source_key_dashes + '.json'
    print(f'Using data quality rules from: {dq_rules_key}')
    try:
        rules_data = sc.textFile(f"{args['txn_bucket']}/{dq_rules_key}")
        rules_json_data = json.loads('\n'.join(rules_data.collect()))
    except Exception as e:
        print(f'No data quality rules file exists or error reading: {e.java_exception.getMessage()}, skipping')
        rules_json_data = {}

    spark_sql_key = sql_prefix[1:] + 'spark-' + source_key_dashes + '.sql'
    print(f'Using Spark SQL transformation from: {spark_sql_key}')
    try:
        spark_sql_data = sc.textFile(f"{args['txn_bucket']}/{spark_sql_key}")
        spark_sql = '\n'.join(spark_sql_data.collect())
    except Exception as e:
        message = e.java_exception.getMessage() if hasattr(e, 'java_exception') else str(e)
        print(f'No Spark SQL transformation exists or error reading: {message}, skipping')
        spark_sql = None

    # Spark SQL is used to populate the target database, stored in S3
    if spark_sql is not None:
        re_create_table = re.compile(r'\s*CREATE TABLE\s+["`\']?([\w]+)["`\']?\s+AS(.*)', re.IGNORECASE | re.DOTALL)
        match_create_table = re_create_table.match(spark_sql)
        if match_create_table:
            print(f'Table name override detected: {match_create_table.group(1)}')
            target_table = match_create_table.group(1)
            spark_sql = match_create_table.group(2)

        substitution_data.update({ 'target_table': target_table })
        spark_sql = spark_sql.strip()
        # Make sure there is something to execute
        if not spark_sql:
            raise RuntimeError('Spark SQL file found but appears to be empty')

        # Use last-win strategy to resolve duplicate keys when using map_from_arrays in Spark SQL
        spark.conf.set('spark.sql.mapKeyDedupPolicy', 'LAST_WIN')

        # Use hive schema instead of Parquet to handle schema evolution
        spark.conf.set('spark.sql.hive.convertMetastoreParquet', False)

        print(f'Executing Spark SQL: {spark_sql.format(**substitution_data)}')
        # Spark SQL should query entire table, because the existing consume table will be overwritten
        df = spark.sql(spark_sql.format(**substitution_data))
        df.cache()
        print(f'Retrieved (from Cleanse) dataframe row count: {df.count()}'
              f'  column count: {len(df.columns)}'
              f'  number of partitions: {df.rdd.getNumPartitions()}')

        lineage = DataLineageGenerator(args)
        lineage.update_lineage(df, args['source_key'], 'sparksql', transform=[ spark_sql ])

        dataquality = DataQualityCheck(rules_json_data, partition, args, lineage, sc)
        filtered_df = dataquality.run_data_quality(df, rules_json_data, 'after_sparksql')
        filtered_df.cache()
        # Post Spark SQL and DQ filter numeric audit
        lineage.update_lineage(filtered_df, args['source_key'], 'numericaudit')

        # Folder structure in the consume bucket should match the other buckets
        storage_location = f"{args['target_bucket']}/{args['source_database_name']}/{target_table}"

        # The combination of Glue Catalog API upserts and Spark saveAsTable using Hive/Parquet
        # allows the lake to manage the merged/destination schema, and Spark to manage schema
        # consistency without needing to repair tables
        upsert_catalog_table(
            filtered_df,
            args['target_database_name'],
            target_table,
            partition.keys(),
            storage_location,
            # Permissive schema change because we are rewriting the entire table
            allow_schema_change='permissive',
        )

        # Explicitly clear any existing S3 folder (i.e. overwrite all partitions)
        glueContext.purge_s3_path(storage_location, options={ 'retentionPeriod': 0 })

        # Set dynamic partitioning to write all partitions in DataFrame
        spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
        spark.conf.set('hive.exec.dynamic.partition', 'true')
        spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')

        filtered_df.repartition(*partition.keys()). \
            write.partitionBy(*partition.keys()).saveAsTable(
                f"{args['target_database_name']}.{target_table}",
                path=storage_location,
                format='hive',
                fileFormat='parquet',
                # Use append to not overwrite previous schema versions and to force saveAsTable to
                # use the schema in Glue Catalog (set above)
                mode='append',
            )

        filtered_df.unpersist()
        print('Data successfully written to Consume table')

    # Athena SQL is used to create views
    athena_sql_key = sql_prefix[1:] + 'athena-' + source_key_dashes + '.sql'
    print(f'Using Athena SQL transformation from: {athena_sql_key}')
    try:
        athena_sql_data = sc.textFile(f"{args['txn_bucket']}/{athena_sql_key}")
        # Format with line breaks and preserve whitespace between lines so logging is easier to read
        athena_sql = '\n'.join(athena_sql_data.collect())
    except Exception as e:
        message = e.java_exception.getMessage() if hasattr(e, 'java_exception') else str(e)
        print(f'No Athena SQL transformation exists or error reading: {message}, skipping')
        athena_sql = None

    if athena_sql:
        for sql in athena_sql.split(';'):
            sql = sql.strip()
            # Check if there is actual SQL content since split will return an element after the final ;
            if sql:
                print(f'Executing Athena SQL: {sql.format(**substitution_data)}')
                status = athena_execute_query(
                    args['target_database_name'], sql.format(**substitution_data), args['TempDir'])
                print(f'Athena query execution status: {status}')

    # Amazon Redshift SQL is used to create views and RMS tables as external schema
    redshift_sql_key = sql_prefix[1:] + 'redshift-' + source_key_dashes + '.sql'
    print(f'Using Amazon Redshift SQL transformation from: {redshift_sql_key}')
    try:
        redshift_sql_data = sc.textFile(f"{args['txn_bucket']}/{redshift_sql_key}")
        # Format with line breaks and preserve whitespace between lines so logging is easier to read
        redshift_sql = '\n'.join(redshift_sql_data.collect())
    except Exception as e:
        message = e.java_exception.getMessage() if hasattr(e, 'java_exception') else str(e)
        print(f'No Amazon Redshift SQL transformation exists or error reading: {message}, skipping')
        redshift_sql = None

    if redshift_sql:
        redshift_connection_parameter = get_redshift_connection_parameter(args)
        print(f'Executing Amazon Redshift SQL: {redshift_sql.format(**substitution_data)}')
        status = redshift_execute_query(
            database=args['redshift_database'],
            query=redshift_sql.format(**substitution_data),
            **redshift_connection_parameter)
        print(f'Amazon Redshift query execution status: {status}')

    job.commit()
    print('Job complete')


if __name__ == '__main__':
    main()