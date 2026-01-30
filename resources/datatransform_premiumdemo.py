# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from decimal import Decimal
import sys
import uuid
import json
import requests
import time
import pandas
from numpy import arange
from pyspark.context import SparkContext
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DecimalType
try:
    import cspark.sdk as CoherentSpark
except ModuleNotFoundError as e:
    if 'cspark' not in e.msg:
        raise e

api_access_key = ''
tenant_name = ''
tenant_env = 'uat.us'
service_folder = 'Integration'
service_name = 'EarnPremium'
# Leave blank for the latest version
api_version_id = ''
execute_url = f'https://excel.{tenant_env}.coherent.global/{tenant_name}/api/v4/execute'


@pandas_udf(DecimalType(16,2))
def coherent_earned_premium_execute(
            series_period_start_date: pandas.Series,
            series_period_end_date: pandas.Series,
            series_effective_date: pandas.Series,
            series_expiration_date: pandas.Series,
            series_total_premium: pandas.Series
        ) -> pandas.Series:
    """Use Coherent Spark Execute API to process chunk of data equivalent to the partition of data
    passed to the UDF
    """
    if len(series_period_start_date) > 100:
        raise RuntimeError('Partition size cannot exceed 100 records for Coherent Spark Execute API;'
            ' consider repartitioning or using the Batch API')

    pandas_df_combined = pandas.concat({
        # Ensure date columns are just dates when passed to the API
        'EarningStartDate': series_period_start_date.astype('datetime64[ns]').dt.strftime('%Y-%m-%d'),
        'EarningEndDate': series_period_end_date.astype('datetime64[ns]').dt.strftime('%Y-%m-%d'),
        'PolicyEffectiveDate': series_effective_date.astype('datetime64[ns]').dt.strftime('%Y-%m-%d'),
        'PolicyExpirationDate': series_expiration_date.astype('datetime64[ns]').dt.strftime('%Y-%m-%d'),
        # Convert Decimal to string to avoid floating point imprecision
        'WrittenPremium': series_total_premium.astype('string')
    }, axis=1)
    # Set static API parameter
    pandas_df_combined['Offset'] = '1'

    # Add an index so we can drop null rows and merge the result data
    pandas_df_combined['index'] = arange(1, pandas_df_combined.shape[0] + 1)
    pandas_df_cleaned = pandas_df_combined.dropna()
    series_index = pandas_df_cleaned['index'].reset_index(drop=True)

    headers = {
        'Content-Type': 'application/json',
        'x-tenant-name': tenant_name,
        'x-synthetic-key': api_access_key
    }
    payload = {
        "service": service_folder + '/' + service_name,
        "version_id": api_version_id,
        "inputs":
            # Dataframe headers match API required headers
            [pandas_df_cleaned.columns.values.tolist()] + \
            # Filter out any rows with nulls and convert to list
            pandas_df_cleaned.values.tolist()
    }
    response = requests.request('POST', execute_url, headers=headers, data=json.dumps(payload), allow_redirects=False)
    response.raise_for_status()
    response_dict = json.loads(response.text)

    # Prepare the results in an indexed DataFrame
    results = response_dict['outputs']
    result_columns = results.pop(0)
    pandas_df_results = pandas.DataFrame(results, columns=result_columns)
    pandas_df_results['index'] = series_index

    # Join the results back to the original DataFrame on index so that null rows are also returned
    pdf_earned_premium = pandas_df_combined.set_index('index').\
        join(pandas_df_results.set_index('index'), rsuffix='_output')

    # Reset numbering of rows
    pdf_earned_premium = pdf_earned_premium.reset_index()

    # Return EarnedPremium column only with results converted to Decimal
    # Python Decimal type will be converted to Spark DecimalType
    return pdf_earned_premium['EarnedPremium'].apply(lambda x: round(Decimal(x), 2) if x else None)


@pandas_udf(DecimalType(16,2))
def coherent_earned_premium_batch(
            series_period_start_date: pandas.Series,
            series_period_end_date: pandas.Series,
            series_effective_date: pandas.Series,
            series_expiration_date: pandas.Series,
            series_total_premium: pandas.Series
        ) -> pandas.Series:
    """Use Coherent Spark Batch API to process chunk of data equivalent to the partition of data
    passed to the UDF
    """
    pandas_df_combined = pandas.concat({
        # Ensure date columns are just dates when passed to the API
        'EarningStartDate': series_period_start_date.astype('datetime64[ns]').dt.strftime('%Y-%m-%d'),
        'EarningEndDate': series_period_end_date.astype('datetime64[ns]').dt.strftime('%Y-%m-%d'),
        'PolicyEffectiveDate': series_effective_date.astype('datetime64[ns]').dt.strftime('%Y-%m-%d'),
        'PolicyExpirationDate': series_expiration_date.astype('datetime64[ns]').dt.strftime('%Y-%m-%d'),
        # Convert Decimal to string to avoid floating point imprecision
        'WrittenPremium': series_total_premium.astype('string')
    }, axis=1)
    # Set static API parameter
    pandas_df_combined['Offset'] = '1'
    # Add an index so we can drop null rows and merge the result data
    pandas_df_combined['index'] = arange(1, pandas_df_combined.shape[0] + 1)

    spark = CoherentSpark.Client(env=tenant_env, tenant=tenant_name, api_key=api_access_key)
    create_response = spark.batches.create(
        CoherentSpark.UriParams(folder=service_folder, service=service_name, version_id = api_version_id),
        unique_record_key='index',
        # Sizes in MB; use as small a value as possible
        max_input_size=5,
        max_output_size=5,
    )

    chunk_id = str(uuid.uuid4())
    pipeline = spark.batches.of(create_response.data['id'])
    pipeline.push(chunks=[CoherentSpark.BatchChunk(
        id=chunk_id,
        size=len(pandas_df_combined),
        data=CoherentSpark.ChunkData(
            inputs=
                # Dataframe headers match API required headers
                [pandas_df_combined.columns.values.tolist()] + \
                # Filter out any rows with nulls and convert to list
                pandas_df_combined.dropna().values.tolist()
        )
    )])

    # Poll status until 1 chunk is ready
    status_response = pipeline.get_status()
    while int(status_response.data['chunks_completed']) == 0:
        if status_response.data['batch_status'] in ['failed', 'error']:
            raise RuntimeError(f"Batch processing failed with status: {status_response.data['batch_status']}")
        time.sleep(2)
        status_response = pipeline.get_status()

    results_response = pipeline.pull()

    # Match our chunk by ID
    our_chunk = {}
    for chunk in results_response.data['data']:
        if chunk.get('id') == chunk_id:
            our_chunk = chunk
            break

    if not our_chunk:
        raise RuntimeError('Batch processing returned no matching results')

    results = our_chunk['outputs']
    result_columns = results.pop(0)
    pandas_df_results = pandas.DataFrame(results, columns=result_columns)

    # Join the results back to the original dataframe on index so that null rows are also returned
    pdf_earned_premium = pandas_df_combined.set_index('index').\
        join(pandas_df_results.set_index('index'), rsuffix='_output')

    # Close the pipeline
    pipeline.dispose()

    # Reset numbering of rows
    pdf_earned_premium = pdf_earned_premium.reset_index()

    # Return EarnedPremium column only with results converted to Decimal
    # Python Decimal type will be converted to Spark DecimalType
    return pdf_earned_premium['EarnedPremium'].apply(lambda x: round(Decimal(x), 2) if x else None)


def transform_coherentearnedpremium(df: DataFrame, earnedpremium_spec: list, args: list, lineage, sc: SparkContext, *extra) -> DataFrame:
    """
    Calculate monthly earned premium using an external model

    Parameters
    ----------
    earnedpremium_spec:
        A list of dictionary objects in the form:
            field: 'NewFieldName'
            written_premium: field name containing total written premium
            period_start_date: field name containing period start date
            period_end_date: field name containing period end date
            policy_effective_date: field name containing policy effective date
            policy_expiration_date: field name containing policy expiration date
            batch: boolean, to use Coherent Batch API or not
    """
    for spec in earnedpremium_spec:
        batch = spec.get('batch', False)
        if batch:
            print('transform_coherentearnedpremium: Using Coherent Spark Batch API mode')
            if 'cspark' not in sys.modules:
                raise RuntimeError('Coherent Spark SDK module not found; check AWS Glue job --additional-python-modules parameter')

            # Use minimal parallelism so we do not create too many pipelines
            df = df.repartition(4)

            df = df.withColumn(spec['field'], coherent_earned_premium_batch(
                spec['period_start_date'],
                spec['period_end_date'],
                spec['policy_effective_date'],
                spec['policy_expiration_date'],
                spec['written_premium'],
            ))
        else:
            print('transform_coherentearnedpremium: Using Coherent Spark Execute API mode')
            # Repartition for Execute API so rows are less than 100 per partition
            df = df.repartition(int(sc.getConf().get('spark.default.parallelism')) * 2)

            df = df.withColumn(spec['field'], coherent_earned_premium_execute(
                spec['period_start_date'],
                spec['period_end_date'],
                spec['policy_effective_date'],
                spec['policy_expiration_date'],
                spec['written_premium'],
            ))

    lineage.update_lineage(df, args['source_key'], 'coherentearnedpremium', transform=earnedpremium_spec)
    return df