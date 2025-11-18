# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrameWriter
from awsglue.context import GlueContext

mock_region = 'us-east-1'
mock_database_name = 'TestGlueCatalogDatabase'
mock_table_name = 'TestGlueCatalogTable'

mock_resource_prefix = 'unittest-insurancelake'
# Temp bucket should have a trailing /
mock_temp_bucket = f'file:///tmp/{mock_resource_prefix}-temp-bucket/'
mock_scripts_bucket = f'file:///tmp/{mock_resource_prefix}-etl-scripts-bucket'
mock_collect_bucket = f'file:///tmp/{mock_resource_prefix}-collect-bucket'
mock_cleanse_bucket = f'file:///tmp/{mock_resource_prefix}-cleanse-bucket'
mock_consume_bucket = f'file:///tmp/{mock_resource_prefix}-consume-bucket'

mock_database_uri = f'{mock_cleanse_bucket}/{mock_database_name}'


def write_local_file(path: str, filename: str, data: str):
    path = path.replace('file://', '')
    os.makedirs(path, exist_ok=True)
    with open(path + '/' + filename, 'w') as input_file:
        input_file.writelines(data)

    return path + '/' + filename

def mock_spark_context(conf: SparkConf = None):
    return spark.sparkContext

class mock_lineage:
    def __init__(self, args: list, uniid: str = None):
        # Not implemented/needed for mocks
        pass

    def update_lineage(self, df, dataset, operation, **kwargs):
        # Not implemented/needed for mocks
        pass

def mock_create_database(database_name: str, database_uri: str):
    # Not implemented/needed for mocks
    pass

def mock_upsert_catalog_table(
        df: any, target_database: str, table_name: str, partition_keys: list, storage_location: str,
        table_description: str = None, allow_schema_change: str = 'strict'):
    # Not implemented/needed for mocks
    pass

def mock_put_s3_object(uri: str, data: any):
    # Not implemented/needed for mocks
    pass

def mock_spark_sql(self, query: str):
    # Simplified mock behavior designed to return only the amount of data needed
    return spark.createDataFrame([], schema='year int, month int, day int, data string')

def mock_purge_table(self, database: str, table_name: str, options: dict = {}):
    # Not implemented/needed for mocks
    pass

def mock_purge_s3_path(self, path: str, options: dict = {}):
    # Not implemented/needed for mocks
    pass

def mock_athena_execute_query(database: str, query: str, workgroup: str, max_attempts: int = 15) -> str:
    return 'SUCCEEDED'

def mock_redshift_execute_query(database: str, query: str,
        cluster_id: str = '', workgroup_name: str = '', max_attempts: int = 30) -> str:
    return 'FINISHED'

def mock_dataframe_saveastable(self, target_table: str, **kwargs):
    # Collapse saveAsTable calls to save(), skipping the catalog update
    storage_location = kwargs.pop('path', '')
    save_format = kwargs.pop('format', '')
    self.save(storage_location, format='parquet' if save_format == 'hive' else save_format, **kwargs)

class mock_glue_job:
    def __init__(self, glue_job_module):
        self.glue_job_module = glue_job_module

    def __call__(self, func):
        def inner(monkeypatch, capsys, *args, **kwargs):
            monkeypatch.setattr(SparkSession, 'sql', mock_spark_sql)
            monkeypatch.setattr(DataFrameWriter, 'saveAsTable', mock_dataframe_saveastable)
            # All scripts use these classes/functions
            monkeypatch.setattr(self.glue_job_module, 'SparkContext', mock_spark_context)
            monkeypatch.setattr(self.glue_job_module, 'DataLineageGenerator', mock_lineage)
            # Only collect_to_cleanse uses these functions
            if 'collect_to_cleanse' in self.glue_job_module.__name__:
                monkeypatch.setattr(self.glue_job_module, 'put_s3_object', mock_put_s3_object)
                monkeypatch.setattr(GlueContext, 'purge_table', mock_purge_table)
                monkeypatch.setattr(self.glue_job_module, 'upsert_catalog_table', mock_upsert_catalog_table)
            # Only cleanse_to_consume uses these functions
            if 'cleanse_to_consume' in self.glue_job_module.__name__:
                monkeypatch.setattr(GlueContext, 'purge_s3_path', mock_purge_s3_path)
                monkeypatch.setattr(self.glue_job_module, 'upsert_catalog_table', mock_upsert_catalog_table)
                monkeypatch.setattr(self.glue_job_module, 'athena_execute_query', mock_athena_execute_query)
                monkeypatch.setattr(self.glue_job_module, 'redshift_execute_query', mock_redshift_execute_query)
            func(monkeypatch, capsys, *args, **kwargs)
        return inner


# Shared Spark session for all tests
if 'pyspark' in sys.modules:
    spark_conf = SparkConf()
    spark_conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    spark_conf.set('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog')
    spark_conf.set('spark.sql.catalog.local.type', 'hadoop')
    spark_conf.set('spark.sql.catalog.local.warehouse', mock_consume_bucket + '/iceberg')
    spark_conf.set('spark.sql.iceberg.handle-timestamp-without-timezone', True)
    spark = SparkSession.builder.config(conf=spark_conf).master('local[*]').appName('Unit-tests').getOrCreate()