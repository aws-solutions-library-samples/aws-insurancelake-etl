# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import hashlib
from pyspark.context import SparkContext
from pyspark.sql.functions import udf, lit, col
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

def transform_hash(df: DataFrame, hash_fields: list, args: dict, lineage, *extra) -> DataFrame:
    """Hash specified column values using SHA256 and Spark UDF

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply field hashing
    hash_fields
        List of field names to hash
    args
        Glue job arguments, from which source_key is used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with hashing applied
    """
    tohash = udf(lambda x: hashlib.sha256(str(x).encode('utf-8')).hexdigest())

    cols_map = {}
    for hash_field in hash_fields:
        if hash_field not in df.columns:
            # Raise error if field is missing to prevent unexpected schema changes from exposing
            # sensitive data
            raise RuntimeError(f"Field '{hash_field}' not found in incoming data")

        cols_map.update({ hash_field: tohash(hash_field) })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'hash', transform=hash_fields)
    return df.withColumns(cols_map)


def transform_redact(df: DataFrame, redact_fields: dict, args: dict, lineage, *extra) -> DataFrame:
    """Redact specified column values using supplied redaction string

    Parameters
    ----------
    df
        pySpark DataFrame on which to apply field redaction
    redact_fields
        Dictionary of fields to redact in the form:
            field: redact_string
    args
        Glue job arguments, from which source_key is used
    lineage
        Initialized lineage class object from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with redaction applied
    """
    cols_map = {}
    for redact_field, redact_string in redact_fields.items():
        if redact_field not in df.columns:
            # Raise error if field is missing to prevent unexpected schema changes from exposing
            # sensitive data
            raise RuntimeError(f"Field '{redact_field}' not found in incoming data")

        cols_map.update({ redact_field: lit(redact_string) })

    # No need to unpersist as there is only one reference to the dataframe and it is returned
    lineage.update_lineage(df, args['source_key'], 'redact', transform=redact_fields)
    return df.withColumns(cols_map)


def transform_tokenize(df_with_values: DataFrame, tokenize_fields: list, args: dict, lineage, sc: SparkContext, *extra) -> DataFrame:
    """Replace specified column values with hash and store original value in separate table
    Uses SHA256 and Spark UDF similar to transform_hash() but with original value stored

    Parameters
    ----------
    df_with_values
        pySpark DataFrame on which to apply field hashing
    tokenize_fields
        List of fields to tokenize
    args
        Glue job arguments, from which source_key, execution_id, and hash_value_table are used
    lineage:
        Initialized lineage class object from the calling job
    sc
        Spark Context class from the calling job

    Returns
    -------
    DataFrame
        pySpark DataFrame with hashing applied
    """
    # TODO: Support hash with salt or composite hash value (improves uniqueness of token data)

    tohash = udf(lambda x: hashlib.sha256(str(x).encode('utf-8')).hexdigest())

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Columns needed for the token store table
    token_store_columns = StructType([
        StructField('raw_data', StringType(), False),
        StructField('hash_key', StringType(), False),
    ])

    # Create an empty DataFrame for the token store
    empty_rdd = sc.emptyRDD()
    token_store_df = spark.createDataFrame(
        data = empty_rdd,
        schema = token_store_columns
    )

    # Using a different variable name here ensures that a pointer to
    # the DataFrame with original values persists and can be referenced.
    # Pointers to the DataFrame with hashes will be repeatedly 
    # de-referenced in the loop as the variable is replaced with a new
    # DataFrame after each operation.
    df_with_hashes = df_with_values

    for tokenize_field in tokenize_fields:

        if tokenize_field not in df_with_hashes.columns:
            # Raise error if field is missing to prevent unexpected schema changes from exposing
            # sensitive data
            raise RuntimeError(f"Field '{tokenize_field}' not found in incoming data")

        # Hash specified field and store value in a new column 'hash_key'
        df_with_hashes = df_with_hashes.withColumn('hash_key', tohash(tokenize_field))

        # Select the tokenized column renamed to 'raw_data' and the hash_key column into a temp
        # DataFrame (to merge with the token store table)
        df_token_store_to_merge = df_with_hashes.select(
            col(tokenize_field).alias('raw_data'), 'hash_key'
        )

        # Merge these new original values with the existing token_store DataFrame
        token_store_df = token_store_df.union(df_token_store_to_merge)

        # We are done with this temp DataFrame
        df_token_store_to_merge.unpersist()

        # Drop the original values from the DataFrame that is being transformed
        df_with_hashes = df_with_hashes.drop(tokenize_field)

        # Rename 'hash_key' to the tokenized column name
        # Drop and Rename is faster than tokenizing twice
        df_with_hashes = df_with_hashes.withColumnRenamed('hash_key', tokenize_field)

    # Remove all duplicate values to reduce write operations and because
    # hash_key (which will be duplicated too) is a partition key in the
    # DynamoDB table
    token_store_df = token_store_df.dropDuplicates(['raw_data'])

    token_store_dyf = DynamicFrame.fromDF(token_store_df, glueContext, f"{args['execution_id']}-token_store_to_write")

    # Store the new original values
    glueContext.write_dynamic_frame_from_options(
        frame = token_store_dyf,
        connection_type = 'dynamodb',
        connection_options = {
            'dynamodb.output.tableName': args['hash_value_table'],
        },
        transformation_ctx = f"{args['execution_id']}-store_token_table"
    )

    lineage.update_lineage(df_with_hashes, args['source_key'], 'tokenize', transform=tokenize_fields)

    # Return the transformed DataFrame, no other dataframes are needed
    return df_with_hashes