# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import sys
from typing import Callable
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
from pyspark.sql.dataframe import DataFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, lit
from glue_catalog_helpers import clear_partition, upsert_catalog_table

try:
    # Try/Except block for running in AWS-provided Glue container until Glue DQ support is added
    from awsgluedq.transforms import EvaluateDataQuality
except ModuleNotFoundError:
    pass


def create_add_dynamodb_columns_func(args: dict, ruleset_name: str, rule_action: str) -> Callable:
    """Create and return DynamicFrame map function to add columns to DQ results
    Use second order function because DynamicFrame.map/RDD.map have no access to global scope
    Function _must_ be defined outside of the DataQualityCheck class in order for pickling of the
    returned function (done by map) to succeed

    Parameters
    ----------
    args
        Glue job arguments, from which JOB_RUN_ID, JOB_NAME, state_machine_name, execution_id,
    rule_action
        abort/warn/quarantine action name to associate with rule results
    ruleset_name
        Name of rule set in dq_rules that is being used

    """
    def _function(rec):
        rec['execution_id'] = args['execution_id']
        rec['job_id_action_rule'] = args['JOB_RUN_ID'] \
            + '-' + rule_action + '-' + ruleset_name + '-' + rec['Rule']
        rec['rule_action'] = rule_action
        rec['ruleset_name'] = ruleset_name
        rec['job_id'] = args['JOB_RUN_ID']
        rec['job_name'] = args['JOB_NAME']
        rec['step_function_name'] = args['state_machine_name']
        # DynamoDB Athena connector doesn't handle Null values well
        rec['FailureReason'] = rec['FailureReason'] or ''
        return rec

    return _function


class DataQualityCheck:
    """Class to perform data quality checks on Glue DynamicFrames with built-in actions
    """
    def __init__(self, dq_rules: dict, partition: dict, args: dict, lineage, sc: SparkContext):
        """
        Parameters
        ----------
        dq_rules
            List of strings containing Glue Data Quality rules (single quotes around field names
            are supported)
        args
            Glue job arguments, from which JOB_RUN_ID, JOB_NAME, state_machine_name, source_key,
            execution_id, table_name, p_* are used
        sc
            Spark Context class from the calling job
        """
        self.dq_rules = dq_rules
        self.partition = partition
        self.args = args
        self.lineage = lineage
        self.glueContext = GlueContext(sc)
        self.spark = self.glueContext.spark_session


    def get_rules_string(self, ruleset_name: str, rule_action: str) -> str:
        """Return string of rules to pass to Glue DQ check
        Parameters
        ----------
        rule_action
            abort/warn/quarantine action name to associate with rule results
        ruleset_name
            Name of rule set in dq_rules that is being used
        """
        return 'Rules = [ ' \
            + ','.join(self.dq_rules[ruleset_name][rule_action]).replace("'", '"') \
            + ' ]'


    def _write_results_to_dynamodb(self, results_dyf: DynamicFrame, ruleset_name: str, rule_action: str):
        """Prepare and write Glue Data Quality results to DynamoDB table

        Parameters
        ----------
        dyf
            Glue DynamicFrame on which to check data quality
        rule_action
            abort/warn/quarantine action name to associate with rule results
        ruleset_name
            Name of rule set in dq_rules that is being used
        """
        add_dynamodb_columns = create_add_dynamodb_columns_func(self.args, ruleset_name, rule_action)
        results_with_keys_dyf = results_dyf.map(f = add_dynamodb_columns)

        self.glueContext.write_dynamic_frame_from_options(
            frame = results_with_keys_dyf,
            connection_type = 'dynamodb',
            connection_options = {
                'dynamodb.output.tableName': self.args['dq_results_table'],
            }
        )


    def check_dataquality_halt(self, dyf: DynamicFrame, ruleset_name: str):
        """Evalute Glue Data Quality rules and raise runtime error if any fail

        Parameters
        ----------
        dyf
            Glue DynamicFrame on which to check data quality
        ruleset_name
            Name of key in dq_rules to use for rules (usually before_transform or after_transform)
        """
        rule_string = self.get_rules_string(ruleset_name, 'halt_rules')
        dq_results = EvaluateDataQuality.apply(
            frame=dyf,
            ruleset=rule_string,
            publishing_options={
                'dataQualityEvaluationContext': self.args['source_key'].replace('/', '-') + '-halt-on-failure',
                'enableDataQualityCloudWatchMetrics': True,
                'enableDataQualityResultsPublishing': True,
            },
        )
        self._write_results_to_dynamodb(dq_results, ruleset_name, 'halt-on-failure')
        dq_results_df = dq_results.toDF()
        dq_results_df.show(truncate=False)
        failed_df = dq_results_df.filter(col('Outcome').contains('Failed'))
        if failed_df.count() > 0:
            raise RuntimeError('Data quality check failed')


    def check_dataquality_warn(self, dyf: DynamicFrame, ruleset_name: str):
        """Evalute Glue Data Quality rules and log a warning message if any fail

        Parameters
        ----------
        dyf
            Glue DynamicFrame on which to check data quality
        ruleset_name
            Name of key in dq_rules to use for rules (usually before_transform or after_transform)
        """
        rule_string = self.get_rules_string(ruleset_name, 'warn_rules')
        dq_results = EvaluateDataQuality.apply(
            frame=dyf,
            ruleset=rule_string,
            publishing_options={
                'dataQualityEvaluationContext': self.args['source_key'].replace('/', '-') + '-warn-on-failure',
                'enableDataQualityCloudWatchMetrics': True,
                'enableDataQualityResultsPublishing': True,
            },
        )
        self._write_results_to_dynamodb(dq_results, ruleset_name, 'warn-on-failure')
        dq_results_df = dq_results.toDF()
        dq_results_df.show(truncate=False)
        warn_df = dq_results_df.filter(col('Outcome').contains('Failed'))
        if warn_df.count() > 0:
            print('Data quality checks failed: warning only')


    def check_dataquality_quarantine(self, dyf: DynamicFrame, ruleset_name: str) -> DataFrame:
        """Evalute Glue Data Quality rules and filter/quarantine rows that fail
        Reference: https://docs.aws.amazon.com/glue/latest/ug/data-quality-notebooks.html

        Parameters
        ----------
        dyf
            Glue DynamicFrame on which to check data quality
        ruleset_name
            Name of key in dq_rules to use for rules (usually before_transform or after_transform)

        Returns
        -------
        DataFrame
            DataFrame (not DynamicFrame) with failed rows filtered (i.e. only passed rows)
        """
        rule_string = self.get_rules_string(ruleset_name, 'quarantine_rules')
        dq_multiframe = EvaluateDataQuality().process_rows(
            frame=dyf,
            ruleset=rule_string,
            publishing_options={
                'dataQualityEvaluationContext': self.args['source_key'].replace('/', '-') + '-quarantine-on-failure',
                'enableDataQualityCloudWatchMetrics': True,
                'enableDataQualityResultsPublishing': True,
            },
            additional_options={'performanceTuning.caching': 'CACHE_NOTHING'},
        )

        dq_results = SelectFromCollection.apply(dfc=dq_multiframe, key='ruleOutcomes')
        self._write_results_to_dynamodb(dq_results, ruleset_name, 'quarantine-on-failure')
        dq_results.toDF().show(truncate=False)

        dq_rowleveloutcomes = SelectFromCollection.apply(dfc=dq_multiframe, key='rowLevelOutcomes')
        dq_rowleveloutcomes_df = dq_rowleveloutcomes.toDF()

        # Passed records will be returned as DataFrame
        passed_df = dq_rowleveloutcomes_df.filter(col('DataQualityEvaluationResult').contains('Passed'))
        # Remove DQ outcome columns because there are only passed rows in this DataFrame
        passed_df = passed_df.drop('DataQualityRulesPass', 'DataQualityRulesFail', 
            'DataQualityRulesSkip', 'DataQualityEvaluationResult')

        # Failed records will be quarantined
        failed_df = dq_rowleveloutcomes_df.filter(col('DataQualityEvaluationResult').contains('Failed'))

        cols_map = {}
        if ruleset_name == 'before_transform':
            # DataFrame before transforms will not have partition or execution_id columns yet
            cols_map.update({
                name: lit(value) for name, value in self.partition.items()
            })
            cols_map.update({'execution_id': lit(self.args['execution_id'])})
        cols_map.update({ 'quarantine_timestamp': current_timestamp() })
        failed_df = failed_df.withColumns(cols_map)

        # Create table and clear partition regardless of number of quarantined rows
        storage_location = \
            f"{self.args['target_bucket']}/quarantine/{ruleset_name}/{self.args['source_key']}"
        upsert_catalog_table(
            failed_df,
            self.args['target_database_name'],
            self.args['table_name'] + '_quarantine_' + ruleset_name,
            self.partition.keys(),
            storage_location,
            # Allow schema changes, because we don't want a failure when saving quarantined rows
            allow_schema_change='permissive',
        )
        clear_partition(self.args['target_database_name'],
            self.args['table_name'] + '_quarantine_' + ruleset_name,
            self.partition, self.glueContext)

        if failed_df.count() > 0:
            # saveAsTable on new tables fails in strict mode even with only 1 partition
            self.spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')
            # Writing with append tells Spark to use the Hive (Glue Catalog) schema
            failed_df.write.partitionBy(*self.partition.keys()).saveAsTable(
                f"{self.args['target_database_name']}.{self.args['table_name']}_quarantine_{ruleset_name}",
                path=storage_location,
                format='hive',
                fileFormat='parquet',
                mode='append',
            )

            if passed_df.count() == 0:
                raise RuntimeError('Data quality check quarantined all rows')

            print(f'Data quality checks failed: {failed_df.count()} record(s) quarantined to {storage_location}')

        # Return passed rows as new DataFrame
        return passed_df


    def run_data_quality(self, df: DataFrame, dq_rules: dict, ruleset_name: str) -> DataFrame:
        """Run data quality checks
        Parameters
        ----------
        df
            DataFrame to check data quality
        dq_rules
            Dictionary of data quality rules in the form:
                'before_transform': {
                    'warn_rules': [],
                    'quarantine_rules': [],
                    'halt_rules': []
                },
                'after_transform': {},
                'after_sparksql': {}
        ruleset_name
            Name of the ruleset to use

        Returns
        -------
        DataFrame
            Filtered DataFrame, only changed if quarantine rules filtered rules
        """
        dq_ruleset = dq_rules.get(ruleset_name, {})
        if not dq_ruleset:
            return df

        if 'awsgluedq.transforms' not in sys.modules:
            print('Skipping data quality checks because awsgluedq library is not available')
            return df

        print(f'Using {ruleset_name} data quality rules: {dq_ruleset}')
        fordq_dyf = DynamicFrame.fromDF(df, self.glueContext, f"{self.args['execution_id']}-fordq_dyf")

        if 'warn_rules' in dq_ruleset:
            print('Glue Data Quality Warn-on-Failure results:')
            self.check_dataquality_warn(fordq_dyf, ruleset_name)

        if 'quarantine_rules' in dq_ruleset:
            print('Glue Data Quality Quarantine-on-Failure results:')
            df.unpersist()
            df = self.check_dataquality_quarantine(fordq_dyf, ruleset_name)
            self.lineage.update_lineage(df, self.args['source_key'], 'dataqualityquarantine',
                transform=dq_ruleset['quarantine_rules'])

        if 'halt_rules' in dq_ruleset:
            # NOTE: Original DynamicFrame is used to process halt rules, so even rows filtered above can trigger abort
            print('Glue Data Quality Halt-on-Failure results:')
            self.check_dataquality_halt(fordq_dyf, ruleset_name)

        return df