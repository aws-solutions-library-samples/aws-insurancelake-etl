# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
"""
Stub module to allow successful import of Glue job test scripts so
that tests can be explicitly skipped when there is no Spark
environment; companion to glue_job_mocking_helper.py
"""
mock_region = ''
mock_database_name = ''
mock_table_name = ''
mock_resource_prefix = ''
mock_temp_bucket = ''
mock_scripts_bucket = ''
mock_collect_bucket = ''
mock_cleanse_bucket = ''
mock_consume_bucket = ''

# Stub Spark types to allow successful import of complex mock data
def StructType(*args):
    return args

def StructField(*args):
    return args

def ArrayType(*args):
    return args

def MapType(*args):
    return args

def StringType(arg = ''):
    return str(arg)

def IntegerType(arg = 0):
    return str(arg)


def etl_collect_to_cleanse():
    # Stub function so that decorator call will succeed
    pass

def etl_cleanse_to_consume():
    # Stub function so that decorator call will succeed
    pass

def etl_consume_entity_match():
    # Stub function so that decorator call will succeed
    pass

def mock_glue_job(func):
    # Stub decorator used by unit tests
    def inner(*args):
        func()
    return inner