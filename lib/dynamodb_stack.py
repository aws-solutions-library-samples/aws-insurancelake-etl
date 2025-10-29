# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from constructs import Construct
import aws_cdk as cdk
import aws_cdk.aws_dynamodb as dynamodb

from .stack_import_helper import ImportedBuckets
from .configuration import (
    DEV, PROD, TEST, LINEAGE,
    get_logical_id_prefix, get_resource_name_prefix, get_environment_configuration,
)

class DynamoDbStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, target_environment: str, **kwargs):
        """CloudFormation stack to create DynamoDB Tables.

        Parameters
        ----------
        scope
            Parent of this stack, usually an App or a Stage, but could be any construct
        construct_id
            The construct ID of this stack; if stackName is not explicitly defined,
            this ID (and any parent IDs) will be used to determine the physical ID of the stack
        target_environment
            The target environment for stacks in the deploy stage
        kwargs: optional
            Optional keyword arguments to pass up to parent Stack class
        """
        super().__init__(scope, construct_id, **kwargs)

        self.mappings = get_environment_configuration(target_environment)
        logical_id_prefix = get_logical_id_prefix()
        resource_name_prefix = get_resource_name_prefix().replace('-', '_')

        buckets = ImportedBuckets(self, logical_id_suffix='DynamoDbStack')

        # DynamoDB stack will allow tables to be destroyed (along with the stack) in the Dev environment only
        self.removal_policy = cdk.RemovalPolicy.DESTROY
        if (target_environment == PROD or target_environment == TEST):
            self.removal_policy = cdk.RemovalPolicy.RETAIN

        # DynamoDB table to store audit log and step function state
        self.job_audit_table = dynamodb.Table(
            self,
            f'{target_environment}{logical_id_prefix}EtlAuditTable',
            table_name=f'{target_environment.lower()}-{resource_name_prefix}-etl-job-audit',
            partition_key=dynamodb.Attribute(name='execution_id', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.DEFAULT,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True),
            removal_policy=self.removal_policy,
            deletion_protection=True if self.removal_policy == cdk.RemovalPolicy.RETAIN else False,
        )

        # NOTE: These GSIs may break future stack updates to the table
        # Reference: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-dynamodb-table.html

        # No other tables require indexes, so there is no need for extra dependencies
        self.job_audit_table.add_global_secondary_index(
            index_name='source_key-job_start_date_int-index',
            partition_key=dynamodb.Attribute(name='source_key', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name='job_start_date_int', type=dynamodb.AttributeType.NUMBER),
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=['job_latest_status']
        )

        self.job_audit_table.add_global_secondary_index(
            index_name='job_latest_status-dependency_key-index',
            partition_key=dynamodb.Attribute(name='job_latest_status', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name='dependency_key', type=dynamodb.AttributeType.STRING),
            projection_type=dynamodb.ProjectionType.INCLUDE,
            non_key_attributes=['execution_id', 'sfn_execution_name', 'sfn_input', 'source_key']
        )

        # DynamoDB table to store raw data to hash value mapping
        # raw data stored in 'raw_data' column
        self.hash_values_table = dynamodb.Table(
            self,
            f'{target_environment}{logical_id_prefix}HashValuesTable',
            table_name=f'{target_environment.lower()}-{resource_name_prefix}-etl-hash-values',
            partition_key=dynamodb.Attribute(name='hash_key', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=buckets.s3_kms_key,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True),
            removal_policy=self.removal_policy,
            deletion_protection=True if self.removal_policy == cdk.RemovalPolicy.RETAIN else False,
        )

        # DynamoDB table to store lookup values for lookup transform
        # Lookup values stored in 'lookup_data' column
        self.value_lookup_table = dynamodb.Table(
            self,
            f'{target_environment}{logical_id_prefix}ValueLookupTable',
            table_name=f'{target_environment.lower()}-{resource_name_prefix}-etl-value-lookup',
            partition_key=dynamodb.Attribute(name='source_system', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name='column_name', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.DEFAULT,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True),
            removal_policy=self.removal_policy,
            deletion_protection=True if self.removal_policy == cdk.RemovalPolicy.RETAIN else False,
        )

        # DynamoDB table to store lookup values for multi-value
        # lookup transform
        # Composite hash of all keys stored in 'lookup_item'
        # Lookup return values stored in user defined columns
        self.multi_lookup_table = dynamodb.Table(
            self,
            f'{target_environment}{logical_id_prefix}MultiValueLookupTable',
            table_name=f'{target_environment.lower()}-{resource_name_prefix}-etl-multi-lookup',
            partition_key=dynamodb.Attribute(name='lookup_group', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name='lookup_item', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.DEFAULT,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True),
            removal_policy=self.removal_policy,
            deletion_protection=True if self.removal_policy == cdk.RemovalPolicy.RETAIN else False,
        )

        # DynamoDB table to store Glue Data Quality results
        self.dq_results_table = dynamodb.Table(
            self,
            f'{target_environment}{logical_id_prefix}DqResultsTable',
            table_name=f'{target_environment.lower()}-{resource_name_prefix}-etl-dq-results',
            partition_key=dynamodb.Attribute(name='execution_id', type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name='job_id_action_rule', type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.DEFAULT,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True),
            removal_policy=self.removal_policy,
            deletion_protection=True if self.removal_policy == cdk.RemovalPolicy.RETAIN else False,
        )

        if self.mappings[LINEAGE]:
            # DynamoDB table to store custom data lineage logging per Step Function exection
            self.data_lineage_table = dynamodb.Table(
                self,
                f'{target_environment}{logical_id_prefix}DataLineageTable',
                table_name=f'{target_environment.lower()}-{resource_name_prefix}-etl-data-lineage',
                partition_key=dynamodb.Attribute(name='step_function_execution_id', type=dynamodb.AttributeType.STRING),
                sort_key=dynamodb.Attribute(name='job_id_operation_seq', type=dynamodb.AttributeType.STRING),
                billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
                encryption=dynamodb.TableEncryption.DEFAULT,
                point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                    point_in_time_recovery_enabled=True),
                removal_policy=self.removal_policy,
                deletion_protection=True if self.removal_policy == cdk.RemovalPolicy.RETAIN else False,
            )
        else:
            self.data_lineage_table = None