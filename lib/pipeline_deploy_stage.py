# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import aws_cdk as cdk
from constructs import Construct
from .step_functions_stack import StepFunctionsStack
from .glue_buckets_stack import GlueBucketsStack
from .glue_jobs_stack import GlueJobsStack
from .data_lake_consumer_stack import DataLakeConsumerStack
from .dynamodb_stack import DynamoDbStack
from .athena_workgroup_stack import AthenaWorkgroupStack
from .tagging import tag
from .configuration import (
    get_logical_id_prefix,
)

class PipelineDeployStage(cdk.Stage):
    def __init__(
        self, scope: Construct, construct_id: str,
        target_environment: str, env: cdk.Environment=None,
        **kwargs
    ):
        """Adds deploy stage to CodePipeline

        Parameters
        ----------
        scope
            Parent of this stack, usually an App or a Stage, but could be any construct
        construct_id
            The construct ID of this stack; if stackName is not explicitly defined,
            this ID (and any parent IDs) will be used to determine the physical ID of the stack
        target_environment
            The target environment for stacks in the deploy stage
        env: optional
            AWS environment definition (account, region) to pass to stacks
        kwargs: optional
            Optional keyword arguments
        """
        super().__init__(scope, construct_id, **kwargs)
        logical_id_prefix = get_logical_id_prefix()

        dynamodb_stack = DynamoDbStack(
            self,
            f'{logical_id_prefix}EtlDynamoDb',
            description='InsuranceLake stack for DynamoDB tables to store pipeline metadata (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            **kwargs,
        )

        glue_buckets_stack = GlueBucketsStack(
            self,
            f'{logical_id_prefix}EtlGlueBuckets',
            description='InsuranceLake stack for S3 buckets used by Glue jobs (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            **kwargs,
        )

        athena_workgroup_stack = AthenaWorkgroupStack(
            self,
            f'{logical_id_prefix}EtlAthenaWorkgroup',
            description='InsuranceLake stack for Athena Workgroup (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
            **kwargs,
        )

        glue_jobs_stack = GlueJobsStack(
            self,
            f'{logical_id_prefix}EtlGlueJobs',
            description='InsuranceLake stack for Glue jobs to support the data pipeline (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            hash_values_table=dynamodb_stack.hash_values_table,
            value_lookup_table=dynamodb_stack.value_lookup_table,
            multi_lookup_table=dynamodb_stack.multi_lookup_table,
            dq_results_table=dynamodb_stack.dq_results_table,
            data_lineage_table=dynamodb_stack.data_lineage_table,
            glue_scripts_bucket=glue_buckets_stack.glue_scripts_bucket,
            glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
            athena_workgroup=athena_workgroup_stack.athena_workgroup,
            **kwargs,
        )

        step_function_stack = StepFunctionsStack(
            self,
            f'{logical_id_prefix}EtlStepFunctions',
            description='InsuranceLake stack for Step Functions and supporting Lambda functions to orchestrate data pipeline steps (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            collect_to_cleanse_job=glue_jobs_stack.collect_to_cleanse_job,
            cleanse_to_consume_job=glue_jobs_stack.cleanse_to_consume_job,
            consume_entity_match_job=glue_jobs_stack.consume_entity_match_job,
            job_audit_table=dynamodb_stack.job_audit_table,
            glue_scripts_bucket=glue_buckets_stack.glue_scripts_bucket,
            **kwargs,
        )

        data_lake_consumer_stack = DataLakeConsumerStack(
            self,
            f'{logical_id_prefix}EtlDataLakeConsumer',
            description='InsuranceLake stack for data lake consumer IAM policy (SO9489) (uksb-1tu7mtee2)',
            target_environment=target_environment,
            env=env,
            glue_scripts_temp_bucket=glue_buckets_stack.glue_scripts_temp_bucket,
            **kwargs,
        )

        tag(step_function_stack, target_environment)
        tag(dynamodb_stack, target_environment)
        tag(glue_buckets_stack, target_environment)
        tag(data_lake_consumer_stack, target_environment)
        tag(athena_workgroup_stack, target_environment)
        tag(glue_jobs_stack, target_environment)