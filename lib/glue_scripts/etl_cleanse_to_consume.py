# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import boto3
import sys
import time
import os
import re
import json

from pyspark.context import SparkContext

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# For running in local Glue container
sys.path.append(os.path.dirname(__file__) + '/lib')
from glue_catalog_helpers import upsert_catalog_table
from datalineage import DataLineageGenerator
from dataquality_check import DataQualityCheck

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

# Handle optional arguments
for arg in sys.argv:
    if '--data_lineage_table' in arg:
        expected_arguments.append('data_lineage_table')


def athena_execute_query(database: str, query: str, result_bucket: str, max_attempts: int = 15) -> str:
    """Function to execute query using Athena boto client, loop until
    result is returned, and return status.

    Parameters
    ----------
    aws_account_id
        AWS Account ID
    database
        Athena database in which to run the query
    query
        Single or multi-line query to run
    max_attempts
        Number of loops (1s apart) to attempt to get query status

    Returns
    -------
    str
        Status of query result execution
    """
    athena = boto3.client('athena')

    query_response = athena.start_query_execution(
            QueryExecutionContext = {'Database': database.lower()},
            QueryString = query,
            ResultConfiguration = {'OutputLocation': result_bucket + '/'}
        )
    print(f'Executed query response: {query_response}')

    # Valid "state" Values: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
    attempts = max_attempts
    status = 'QUEUED'
    while status in [ 'QUEUED', 'RUNNING' ]:
        if attempts == 0:
            raise RuntimeError('athena_execute_query() exceeded max_attempts waiting for query')

        query_details = athena.get_query_execution(QueryExecutionId = query_response['QueryExecutionId'])
        print(f'Get query execution response: {query_details}')
        status = query_details['QueryExecution']['Status']['State']
        if status in [ 'FAILED', 'CANCELED' ]:
            # Raise errors that are not raised from StartQueryExecution
            raise RuntimeError("athena_execute_query() failed with query engine error: "
                f"{query_details['QueryExecution']['Status'].get('StateChangeReason', '')}")
        if status != 'SUCCEEDED':
            time.sleep(1)   # nosemgrep
        attempts -= 1

    return status


def main():
    args = getResolvedOptions(sys.argv, expected_arguments)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

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

    # TODO: Check if Lakeformation permissions break Spark SQL query and find workaround
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

    job.commit()
    print('Job complete')


if __name__ == '__main__':
    main()