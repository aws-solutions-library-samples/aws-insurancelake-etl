# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import boto3
import sys
import time
import os
import re

from pyspark.context import SparkContext

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# For running in local Glue container
sys.path.append(os.path.dirname(__file__) + '/lib')
from glue_catalog_helpers import create_database
from datalineage import DataLineageGenerator

expected_arguments = [
    'JOB_NAME',
    'environment',
    'TempDir',
    'txn_bucket',
    'txn_sql_prefix_path',
    'source_bucket',
    'target_bucket',
    'state_machine_name',
    'execution_id',
    'database_name_prefix',
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
            QueryExecutionContext = {'Database': database},
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
            time.sleep(1)
        attempts -= 1

    return status


def main():
    args = getResolvedOptions(sys.argv, expected_arguments)

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Argument passed as database_name_prefix will have mixed case and match the S3 file system
    # Glue Catalog Databases will be lower case only
    source_database = args['database_name_prefix'].lower()
    target_database = f"{args['database_name_prefix'].lower()}_consume"
    target_table = args['table_name']
    substitution_data = args.copy()
    substitution_data.update({
        'source_database': source_database,
        'target_database': target_database,
    })
    sql_prefix = args['txn_sql_prefix_path']

    # TODO: Check if Lakeformation permissions break Spark SQL query and find workaround
    spark_sql_key = f"{sql_prefix[1:]}spark-{args['database_name_prefix']}-{args['table_name']}.sql"
    print(f'Using Spark SQL transformation from: {spark_sql_key}')
    try:
        spark_sql_data = sc.textFile(f"{args['txn_bucket']}/{spark_sql_key}")
        spark_sql = '\n'.join(spark_sql_data.collect())
    except Exception as e:
        print(f'No Spark SQL transformation exists or error reading: {e.java_exception.getMessage()}, skipping')
        spark_sql = None

    # Spark SQL is used to populate the target database, stored in S3
    if spark_sql is not None:
        re_create_table = re.compile(r'\s*CREATE[\s\w]*\s+TABLE\s+["`\']?([\w]+)["`\']?\s+AS(.*)', re.IGNORECASE | re.DOTALL)
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

        print(f'Executing Spark SQL: {spark_sql.format(**substitution_data)}')
        # Spark SQL should query entire table, because the existing consume table will be overwritten
        df = spark.sql(spark_sql.format(**substitution_data))
        print(f'Retrieved (from Cleanse) dataframe row count: {df.count()}'
              f'  column count: {len(df.columns)}'
              f'  number of partitions: {df.rdd.getNumPartitions()}')

        df.cache()
        lineage = DataLineageGenerator(args)
        dataset_name = args['database_name_prefix'] + '/' + args['table_name']
        lineage.update_lineage(df, dataset_name, 'numericaudit')
        lineage.update_lineage(df, dataset_name, 'sparksql', transform=[ spark_sql ])

        storage_location = f"{args['target_bucket']}/{args['database_name_prefix']}/{target_table}"

        create_database(target_database)
        df.write.partitionBy('year', 'month', 'day').saveAsTable(
            f'{target_database}.{target_table}',
            path=storage_location,
            format='parquet',
            mode='overwrite',
        )
        df.unpersist()

    # Athena SQL is used to create views
    athena_sql_key = f"{sql_prefix[1:]}athena-{args['database_name_prefix']}-{args['table_name']}.sql"
    print(f'Using Athena SQL transformation from: {athena_sql_key}')
    try:
        athena_sql_data = sc.textFile(f"{args['txn_bucket']}/{athena_sql_key}")
        # Format with line breaks and preserve whitespace between lines so logging is easier to read
        athena_sql = '\n'.join(athena_sql_data.collect())
    except Exception as e:
        print(f'No Athena SQL transformation exists or error reading: {e.java_exception.getMessage()}, skipping')
        athena_sql = None

    if athena_sql:
        for sql in athena_sql.split(';'):
            sql = sql.strip()
            # Check if there is actual SQL content since split will return an element after the final ;
            if sql:
                print(f'Executing Athena SQL: {sql.format(**substitution_data)}')
                status = athena_execute_query(target_database, sql.format(**substitution_data), args['TempDir'])
                print(f'Athena query execution status: {status}')

    job.commit()


if __name__ == '__main__':
    main()