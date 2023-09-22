# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import pytest
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrameWriter

mock_region = 'us-east-1'
mock_database_name = 'TestGlueCatalogDatabase'
mock_table_name = 'TestGlueCatalogTable'

mock_resource_prefix = 'unittest-insurancelake'
mock_temp_bucket = f'file:///tmp/{mock_resource_prefix}-temp-bucket'
mock_scripts_bucket = f'file:///tmp/{mock_resource_prefix}-etl-scripts-bucket'
mock_collect_bucket = f'file:///tmp/{mock_resource_prefix}-collect-bucket'
mock_cleanse_bucket = f'file:///tmp/{mock_resource_prefix}-cleanse-bucket'
mock_consume_bucket = f'file:///tmp/{mock_resource_prefix}-consume-bucket'


def write_local_file(path: str, filename: str, data: str):
    path = path.replace('file://', '')
    os.makedirs(path, exist_ok=True)
    with open(path + '/' + filename, 'w') as input_file:
        input_file.writelines(data)

def mock_spark_context():
    return spark.sparkContext

class mock_lineage:
    def __init__(self, args: list, uniid: str = None):
        # Not implemented/needed for mocks
        pass

    def update_lineage(self, df, dataset, operation, **kwargs):
        # Not implemented/needed for mocks
        pass

def mock_create_database(database_name: str):
    # Not implemented/needed for mocks
    pass

def mock_upsert_catalog_table(
        df: any, target_database: str, table_name: str, partition_keys: list, storage_location: str,
        allow_schema_change: str = 'strict'):
    # Not implemented/needed for mocks
    pass

def mock_put_s3_object(uri: str, data: any):
    # Not implemented/needed for mocks
    pass

def mock_spark_sql(self, query: str):
    # Simplified mock behavior designed to get past the sql statements
    if 'alter table' in query.lower():
        return None
    else:
        return spark.createDataFrame([], schema='year int, month int, day int, data string')

def mock_athena_execute_query(database: str, query: str, max_attempts: int = 15) -> str:
    return 'SUCCEEDED'

def mock_dataframe_saveastable(self, target_table: str, **kwargs):
    # Collapse saveAsTable calls to save(), skipping the catalog update
    storage_location = kwargs.pop('path', '')
    self.save(storage_location, **kwargs)

class mock_glue_job:
    def __init__(self, glue_job_module):
        self.glue_job_module = glue_job_module

    def __call__(self, func):
        def inner(monkeypatch, *args, **kwargs):
            monkeypatch.setattr(SparkSession, 'sql', mock_spark_sql)
            monkeypatch.setattr(DataFrameWriter, 'saveAsTable', mock_dataframe_saveastable)
            # Both scripts use these classes/functions
            monkeypatch.setattr(self.glue_job_module, 'SparkContext', mock_spark_context)
            monkeypatch.setattr(self.glue_job_module, 'DataLineageGenerator', mock_lineage)
            # Only collect_to_cleanse uses these functions
            if 'collect_to_cleanse' in self.glue_job_module.__name__:
                monkeypatch.setattr(self.glue_job_module, 'upsert_catalog_table', mock_upsert_catalog_table)
                monkeypatch.setattr(self.glue_job_module, 'put_s3_object', mock_put_s3_object)
            if 'cleanse_to_consume' in self.glue_job_module.__name__:
                monkeypatch.setattr(self.glue_job_module, 'create_database', mock_create_database)
            func(monkeypatch, *args, **kwargs)
        return inner


# Shared Spark session for all tests
if 'pyspark' in sys.modules:
    spark = SparkSession.builder.master('local[*]').appName('Unit-tests').getOrCreate()