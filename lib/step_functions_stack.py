# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import os
import aws_cdk as cdk
from constructs import Construct
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_logs as logs
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_notifications as s3_notifications
import aws_cdk.aws_sns as sns
import aws_cdk.aws_stepfunctions as stepfunctions
import aws_cdk.aws_stepfunctions_tasks as stepfunctions_tasks
from cdk_nag import NagSuppressions

from .stack_import_helper import ImportedBuckets
from .configuration import (
    DEV, PROD, TEST, STATE_MACHINE, NOTIFICATION_TOPIC,
    get_logical_id_prefix, get_resource_name_prefix, get_environment_configuration,
)


class StepFunctionsStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        target_environment: str,
        collect_to_cleanse_job: glue.CfnJob,
        cleanse_to_consume_job: glue.CfnJob,
        job_audit_table: dynamodb.Table,
        **kwargs
    ):
        """CloudFormation stack to create Step Functions, Lambdas, and SNS Topics

        Parameters
        ----------
        scope
            Parent of this stack, usually an App or a Stage, but could be any construct
        construct_id
            The construct ID of this stack; if stackName is not explicitly defined,
            this ID (and any parent IDs) will be used to determine the physical ID of the stack
        target_environment
            The target environment for stacks in the deploy stage
        collect_to_cleanse_job
            Collect to Cleanse Glue job construct to invoke
        cleanse_to_consume_job
            Cleanse to Consume Glue job construct to invoke
        job_audit_table
            The DynamoDB Table construct for storing Job Audit results
        kwargs: optional
            Optional keyword arguments to pass up to parent Stack class
        """
        super().__init__(scope, construct_id, **kwargs)

        self.target_environment = target_environment
        self.mappings = get_environment_configuration(target_environment)
        self.logical_id_prefix = get_logical_id_prefix()
        self.resource_name_prefix = get_resource_name_prefix()
        if (target_environment == PROD or target_environment == TEST):
            self.removal_policy = cdk.RemovalPolicy.RETAIN
            self.log_retention = logs.RetentionDays.SIX_MONTHS
        else:
            self.removal_policy = cdk.RemovalPolicy.DESTROY
            self.log_retention = logs.RetentionDays.ONE_MONTH

        buckets = ImportedBuckets(self, logical_id_suffix='StepFunctionsStack')

        cloudwatch_step_function_log_group = logs.LogGroup(
            self, 
            f'{target_environment}{self.logical_id_prefix}EtlStateMachineLogGroup',
            retention=self.log_retention,
            removal_policy=self.removal_policy,
        )

        notification_topic = sns.Topic(
            self, 
            f'{target_environment}{self.logical_id_prefix}EtlNotificationTopic',
            topic_name=f'{target_environment.lower()}-{self.resource_name_prefix}-etl-notification-topic',
            display_name='Insurance Lake ETL Notifications Topic',
            master_key=buckets.s3_kms_key,
        )

        status_function = self.lambda_function_for_etl(
            logical_id_suffix='EtlStatusUpdate',
            resource_name_suffix='etl-status-update',
            function_description='ETL Step Functions workflow triggered handler to update DynamoDB in case of success or failure',
            lambda_code_relative_path='etl_job_auditor',
            lambda_environment={
                'DYNAMODB_TABLE_NAME': job_audit_table.table_name,
            },
            job_audit_table=job_audit_table,
        )

        fail_state = stepfunctions.Fail(
            self,
            f'{target_environment}{self.logical_id_prefix}EtlFailedState',
            cause='End of failure path, root cause from Glue task step',
            error='Error'
        )
        success_state = stepfunctions.Succeed(self, f'{target_environment}{self.logical_id_prefix}EtlSucceededState')

        failure_function_task = stepfunctions_tasks.LambdaInvoke(
            self,
            f'{target_environment}{self.logical_id_prefix}EtlFailureStatusUpdateTask',
            lambda_function=status_function,
            retry_on_service_exceptions=True,
            payload=stepfunctions.TaskInput.from_object({'Input.$': '$'}),
            result_path='$.status_update_result',
            output_path='$',
        )
        failure_notification_task = stepfunctions_tasks.SnsPublish(
            self,
            f'{target_environment}{self.logical_id_prefix}EtlFailurePublishTask',
            topic=notification_topic,
            subject='Job Failed',
            message=stepfunctions.TaskInput.from_json_path_at('$')
        )
        failure_function_task.next(failure_notification_task)
        failure_notification_task.next(fail_state)

        success_function_task = stepfunctions_tasks.LambdaInvoke(
            self,
            f'{target_environment}{self.logical_id_prefix}EtlSuccessStatusUpdateTask',
            lambda_function=status_function,
            retry_on_service_exceptions=True,
            payload=stepfunctions.TaskInput.from_object({'Input.$': '$'}),
            result_path='$.status_update_result',
            output_path='$',
        )
        success_task = stepfunctions_tasks.SnsPublish(
            self,
            f'{target_environment}{self.logical_id_prefix}EtlSuccessPublishTask',
            topic=notification_topic,
            subject='Job Completed',
            message=stepfunctions.TaskInput.from_json_path_at('$')
        )
        success_function_task.next(success_task)
        success_task.next(success_state)

        glue_collect_task = stepfunctions_tasks.GlueStartJobRun(
            self,
            f'{target_environment}{self.logical_id_prefix}GlueCollectJobTask',
            glue_job_name=collect_to_cleanse_job.name,
            comment='Collect to Cleanse data load and transform',
            arguments=stepfunctions.TaskInput.from_object({
                # These arguments overlay and/or override base arguments from the Glue Job definition
                '--state_machine_name.$': '$$.StateMachine.Name',
                '--execution_id.$': '$.execution_id',
                '--target_database_name.$': '$.target_database_name',
                '--source_key.$': '$.source_key',
                '--base_file_name.$': '$.base_file_name',
                '--p_year.$': '$.p_year',
                '--p_month.$': '$.p_month',
                '--p_day.$': '$.p_day',
                '--table_name.$': '$.table_name',
            }),
            integration_pattern=stepfunctions.IntegrationPattern.RUN_JOB,
            result_path='$.taskresult',
            output_path='$',
        )
        glue_collect_task.add_catch(failure_function_task, result_path='$.taskresult')

        glue_cleanse_task = stepfunctions_tasks.GlueStartJobRun(
            self,
            f'{target_environment}{self.logical_id_prefix}GlueCleanseJobTask',
            glue_job_name=cleanse_to_consume_job.name,
            comment='Cleanse to Consume data load and transform',
            arguments=stepfunctions.TaskInput.from_object({
                # These arguments overlay and/or override base arguments from the Glue Job definition
                '--state_machine_name.$': '$$.StateMachine.Name',
                '--execution_id.$': '$.execution_id',
                '--database_name_prefix.$': '$.target_database_name',
                '--table_name.$': '$.table_name',
                '--base_file_name.$': '$.base_file_name',
                '--p_year.$': '$.p_year',
                '--p_month.$': '$.p_month',
                '--p_day.$': '$.p_day',
            }),
            integration_pattern=stepfunctions.IntegrationPattern.RUN_JOB,
            result_path='$.taskresult',
            output_path='$',
        )
        glue_cleanse_task.add_catch(failure_function_task, result_path='$.taskresult')

        machine_definition = glue_collect_task.next(
            glue_cleanse_task.next(
                success_function_task
            )
        )


        machine = stepfunctions.StateMachine(
            self,
            f'{target_environment}{self.logical_id_prefix}EtlStateMachine',
            state_machine_name=f'{target_environment.lower()}-{self.resource_name_prefix}-etl-state-machine',
            tracing_enabled=True,
            definition=machine_definition,
            logs=stepfunctions.LogOptions(
                destination=cloudwatch_step_function_log_group,
                level=stepfunctions.LogLevel.ALL,
            ),
        )

        # State Machine needs access to KMS key to publish topics
        machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    'kms:GenerateDataKey',
                ],
                resources=[buckets.s3_kms_key.key_arn],
            )
        )

        # The State Machine generated role is otherwise specific and uses only the needed actions
        # and resources for the Lambda functions, Glue Jobs, and SNS Topic. And builds these
        # policies automatically based on the state machine definition.
        NagSuppressions.add_resource_suppressions(machine.role, [
            {
                'id': 'AwsSolutions-IAM5',
                'reason': 'State Machine generated role uses wildcard permissions for CloudWatch and X-Ray'
            },
        ], apply_to_children=True)


        trigger_function = self.lambda_function_for_etl(
            logical_id_suffix='EtlTrigger',
            resource_name_suffix='etl-trigger',
            function_description='Collect S3 Bucket triggered handler to trigger Step Functions workflow',
            lambda_code_relative_path='state_machine_trigger',
            lambda_environment={
                'DYNAMODB_TABLE_NAME': job_audit_table.table_name,
                'SFN_STATE_MACHINE_ARN': machine.state_machine_arn,
            },
            job_audit_table=job_audit_table,
            state_machine=machine,
        )

        # Will create CustomResource and Lambda to add event handler to imported bucket
        # TODO: Apply rotation and retention policies to Custom Resource Lambda log group
        buckets.raw.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notifications.LambdaDestination(trigger_function),
        )

        NagSuppressions.add_resource_suppressions(self, [
            {
                'id': 'AwsSolutions-IAM4',
                'reason': 'Bucket Notification CustomResource used only during stack deployment and deletion'
            },
            {
                'id': 'AwsSolutions-IAM5',
                'reason': 'Bucket Notification CustomResource used only during stack deployment and deletion'
            },
        ], apply_to_children=True)

        cdk.CfnOutput(
            self,
            f'{target_environment}{self.logical_id_prefix}StateMachineName',
            value=machine.state_machine_name,
            export_name=self.mappings[STATE_MACHINE]
        )

        cdk.CfnOutput(
            self,
            f'{target_environment}{self.logical_id_prefix}SnsTopicName',
            value=notification_topic.topic_name,
            export_name=self.mappings[NOTIFICATION_TOPIC]
        )


    def get_lambda_role(
        self,
        logical_id_suffix: str,
        resource_name_suffix: str,
        log_group: logs.LogGroup,
        job_audit_table: dynamodb.Table,
        state_machine: stepfunctions.StateMachine = None,
    ) -> iam.Role:
        """Creates the role used during Lambda execution

        Parameters
        ----------
        logical_id_suffix
            Suffix to append to Logical ID of IAM Role to differentiate between Lambdas
        resource_name_suffix
            Suffix to append to IAM Role name to differentiate between Lambdas
        log_group
            Log group for the Lambda function
        job_audit_table
            The DynamoDB Table construct for storing Job Audit results
        state_machine: optional
            Optional Step Function StateMachine for execution (if used by the Lambda)

        Returns
        -------
        iam.Role
            The IAM role that was created
        """
        policies = {
            'CloudWatchLogAccess':
            iam.PolicyDocument(statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'logs:CreateLogStream',
                        'logs:PutLogEvents',
                    ],
                    resources=[log_group.log_group_arn]
                )
            ]),
            'JobAuditTableAccess':
            iam.PolicyDocument(statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'dynamodb:PutItem',
                        'dynamodb:GetItem',
                        'dynamodb:UpdateItem',
                    ],
                    resources=[job_audit_table.table_arn]
                )
            ]),
        }

        if state_machine is not None:
            policies.update({
                'StateMachineAccess':
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'states:StartExecution',
                        ],
                        resources=[state_machine.state_machine_arn]
                    )
                ]),
            })

        return iam.Role(
            self,
            f'{self.target_environment}{self.logical_id_prefix}{logical_id_suffix}LambdaRole',
            description='Role for Insurance Lake ETL Lambda Functions',
            role_name=f'{self.target_environment.lower()}-{self.resource_name_prefix}-{self.region}-{resource_name_suffix}-lambda',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            inline_policies=policies,
        )


    def lambda_function_for_etl(
        self,
        logical_id_suffix: str,
        resource_name_suffix: str,
        function_description: str,
        lambda_code_relative_path: str,
        job_audit_table: dynamodb.Table,
        state_machine: stepfunctions.StateMachine = None,
        lambda_environment: dict = {},
    ) -> _lambda.Function:
        """Creates a Lambda Function to support the ETL process

        Parameters
        ----------
        logical_id_suffix
            Suffix to append to Logical ID of IAM Role to differentiate between Lambdas
        resource_name_suffix
            Suffix to append to IAM Role name to differentiate between Lambdas
        function_description
            Description of Lambda to use for deployment
        lambda_code_relative_path
            Relative local path to the Lambda source code
        job_audit_table
            The DynamoDB Table construct for storing Job Audit results
        state_machine: optional
            Optional Step Function StateMachine for execution (if used by the Lambda)
        lambda_environment: optional
            Optional environment variable key, value pairs to pass to Lambda

        Returns
        -------
        lambda.Function
            The Lambda Function that was created
        """
        lambda_function_name = f'{self.target_environment.lower()}-{self.resource_name_prefix}-{resource_name_suffix}'

        # Associate the Log group to the Lambda function by giving it the same name
        cloudwatch_log_group = logs.LogGroup(
            self, 
            f'{self.target_environment}{self.logical_id_prefix}{logical_id_suffix}LambdaLogGroup',
            log_group_name=f'/aws/lambda/{lambda_function_name}',
            retention=self.log_retention,
            removal_policy=self.removal_policy,
        )

        return _lambda.Function(
            self,
            f'{self.target_environment}{self.logical_id_prefix}{logical_id_suffix}',
            function_name=lambda_function_name,
            description=function_description,
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler='lambda_handler.lambda_handler',
            code=_lambda.Code.from_asset(f'{os.path.dirname(__file__)}/{lambda_code_relative_path}'),
            architecture=_lambda.Architecture.ARM_64,
            environment=lambda_environment,
            role=self.get_lambda_role(
                logical_id_suffix,
                resource_name_suffix,
                cloudwatch_log_group,
                job_audit_table,
                state_machine
            ),
        )