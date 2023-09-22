# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from typing import Callable
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
from pyspark.sql.dataframe import DataFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp
from awsgluedq.transforms import EvaluateDataQuality
from glue_catalog_helpers import upsert_catalog_table


def create_add_dynamodb_columns_func(rule_action: str, args: dict) -> Callable:
    """Create and return DynamicFrame map function to add columns to DQ results
    Use second order function because DynamicFrame.map/RDD.map have no access to global scope
    """
    def _function(rec):
        rec['execution_id'] = args['execution_id']
        rec['job_id_action_rule'] = args['JOB_RUN_ID'] + '-' + rule_action + '-' + rec['Rule']
        rec['rule_action'] = rule_action
        rec['job_id'] = args['JOB_RUN_ID']
        rec['job_name'] = args['JOB_NAME']
        rec['step_function_name'] = args['state_machine_name']
        # DynamoDB Athena connector doesn't handle Null values well
        rec['FailureReason'] = rec['FailureReason'] or ''
        return rec

    return _function

def write_results_to_dynamodb(results_dyf: DynamicFrame, rule_action: str, args: dict, sc: SparkContext):
    """Prepare and write Glue Data Quality results to DynamoDB table

    Parameters
    ----------
    dyf
        Glue DynamicFrame on which to check data quality
    action_name
        abort/warn/quarantine action name to associate with rule results
    args
        Glue job arguments, from which dq_results_table is used
    sc
        Spark Context class from the calling job
    """
    glueContext = GlueContext(sc)

    add_dynamodb_columns = create_add_dynamodb_columns_func(rule_action, args)
    results_with_keys_dyf = results_dyf.map(f = add_dynamodb_columns)

    glueContext.write_dynamic_frame_from_options(
        frame = results_with_keys_dyf,
        connection_type = 'dynamodb',
        connection_options = {
            'dynamodb.output.tableName': args['dq_results_table'],
        }
    )


def check_dataquality_halt(dyf: DynamicFrame, dq_rules: list, args: dict, sc: SparkContext):
    """Evalute Glue Data Quality rules and raise runtime error if any fail

    Parameters
    ----------
    dyf
        Glue DynamicFrame on which to check data quality
    dq_rules
        List of strings containing Glue Data Quality rules (single quotes around field names supported)
    args
        Glue job arguments, from which source_key is used
    sc
        Spark Context class from the calling job
    """
    rule_string = 'Rules = [ ' + ','.join(dq_rules).replace("'", '"') + ' ]'
    dq_results = EvaluateDataQuality.apply(
        frame=dyf,
        ruleset=rule_string,
        publishing_options={
            'dataQualityEvaluationContext': args['source_key'].replace('/', '-') + '-halt-on-failure',
            'enableDataQualityCloudWatchMetrics': True,
            'enableDataQualityResultsPublishing': True,
        },
    )
    write_results_to_dynamodb(dq_results, 'halt-on-failure', args, sc)
    dq_results_df = dq_results.toDF()
    dq_results_df.show(truncate=False)
    failed_df = dq_results_df.filter(col('Outcome').contains('Failed'))
    if failed_df.count() > 0:
        raise RuntimeError('Data quality check failed')


def check_dataquality_warn(dyf: DynamicFrame, dq_rules: list, args: dict, sc: SparkContext):
    """Evalute Glue Data Quality rules and log a warning message if any fail

    Parameters
    ----------
    dyf
        Glue DynamicFrame on which to check data quality
    dq_rules
        List of strings containing Glue Data Quality rules (single quotes around field names supported)
    args
        Glue job arguments, from which source_key is used
    sc
        Spark Context class from the calling job
    """
    rule_string = 'Rules = [ ' + ','.join(dq_rules).replace("'", '"') + ' ]'
    dq_results = EvaluateDataQuality.apply(
        frame=dyf,
        ruleset=rule_string,
        publishing_options={
            'dataQualityEvaluationContext': args['source_key'].replace('/', '-') + '-warn-on-failure',
            'enableDataQualityCloudWatchMetrics': True,
            'enableDataQualityResultsPublishing': True,
        },
    )
    write_results_to_dynamodb(dq_results, 'warn-on-failure', args, sc)
    dq_results_df = dq_results.toDF()
    dq_results_df.show(truncate=False)
    warn_df = dq_results_df.filter(col('Outcome').contains('Failed'))
    if warn_df.count() > 0:
        print('Data quality checks failed: warning only')


def check_dataquality_quarantine(dyf: DynamicFrame, dq_rules: list, args: dict, sc: SparkContext) -> DataFrame:
    """Evalute Glue Data Quality rules and filter/quarantine rows that fail
    Reference: https://docs.aws.amazon.com/glue/latest/ug/data-quality-notebooks.html

    Parameters
    ----------
    dyf
        Glue DynamicFrame on which to check data quality
    dq_rules
        List of strings containing Glue Data Quality rules (single quotes around field names supported)
    args
        Glue job arguments, from which source_key is used
    sc
        Spark Context class from the calling job

    Returns
    -------
    DataFrame
        DataFrame (not DynamicFrame) with failed rows filtered (i.e. only passed rows)
    """
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    rule_string = 'Rules = [ ' + ','.join(dq_rules).replace("'", '"') + ' ]'
    dq_multiframe = EvaluateDataQuality().process_rows(
        frame=dyf,
        ruleset=rule_string,
        publishing_options={
            'dataQualityEvaluationContext': args['source_key'].replace('/', '-') + '-quarantine-on-failure',
            'enableDataQualityCloudWatchMetrics': True,
            'enableDataQualityResultsPublishing': True,
        },
        additional_options={'performanceTuning.caching': 'CACHE_NOTHING'},
    )

    dq_results = SelectFromCollection.apply(dfc=dq_multiframe, key='ruleOutcomes')
    write_results_to_dynamodb(dq_results, 'quarantine-on-failure', args, sc)
    dq_results_df = dq_results.toDF()
    dq_results_df.show(truncate=False)

    dq_rowleveloutcomes = SelectFromCollection.apply(dfc=dq_multiframe, key='rowLevelOutcomes')
    dq_rowleveloutcomes_df = dq_rowleveloutcomes.toDF()

    # Passed records will be returned as DataFrame
    passed_df = dq_rowleveloutcomes_df.filter(col('DataQualityEvaluationResult').contains('Passed'))
    # Remove DQ outcome columns because there are only passed rows in this DataFrame
    passed_df = passed_df.drop('DataQualityRulesPass', 'DataQualityRulesFail', 'DataQualityRulesSkip', 'DataQualityEvaluationResult')

    # Failed records will be quarantined
    failed_df = dq_rowleveloutcomes_df.filter(col('DataQualityEvaluationResult').contains('Failed'))
    if failed_df.count() > 0:
        failed_df = failed_df.withColumn('quarantine_timestamp', current_timestamp())

        storage_location = args['target_bucket'] + '/quarantine/' + args['target_database_name'] + '/' + args['table_name']
        upsert_catalog_table(
            failed_df,
            args['target_database_name'],
            args['table_name'] + '_quarantine',
            ['year', 'month', 'day'],
            storage_location,
            # Allow schema changes, because we don't want a failure when saving quarantined rows
            allow_schema_change='permissive',
        )
        failed_df.write.partitionBy('year', 'month', 'day').format('parquet').save(storage_location, mode='overwrite')
        spark.sql(f"ALTER TABLE {args['target_database_name']}.{args['table_name'] + '_quarantine'} RECOVER PARTITIONS")

        if passed_df.count() == 0:
            raise RuntimeError('Data quality check quarantined all rows')

        print(f'Data quality checks failed: {failed_df.count()} record(s) quarantined to {storage_location}')

    # Return passed rows as new DataFrame
    return passed_df