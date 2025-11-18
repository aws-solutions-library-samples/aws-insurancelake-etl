# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import aws_cdk as cdk
from constructs import Construct
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_iam as iam
from cdk_nag import NagSuppressions

from .stack_import_helper import ImportedBuckets
from .configuration import (
    get_logical_id_prefix, get_resource_name_prefix, get_environment_configuration,
)

class DataLakeConsumerStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        target_environment: str,
        glue_scripts_temp_bucket: s3.Bucket,
        **kwargs
    ):
        """CloudFormation stack to create consumer policy for data lake access.

        Parameters
        ----------
        scope
            Parent of this stack, usually an App or a Stage, but could be any construct
        construct_id
            The construct ID of this stack; if stackName is not explicitly defined,
            this ID (and any parent IDs) will be used to determine the physical ID of the stack
        target_environment
            The target environment for stacks in the deploy stage
        glue_scripts_temp_bucket
            The S3 bucket used for temporary Glue job storage from GlueBucketsStack
        kwargs: optional
            Optional keyword arguments to pass up to parent Stack class
        """
        super().__init__(scope, construct_id, **kwargs)

        self.target_environment = target_environment
        self.mappings = get_environment_configuration(target_environment)
        self.logical_id_prefix = get_logical_id_prefix()
        self.resource_name_prefix = get_resource_name_prefix()

        self.glue_scripts_temp_bucket = glue_scripts_temp_bucket
        self.buckets = ImportedBuckets(self, logical_id_suffix='DataLakeConsumerStack')

        self.data_lake_consumer_policy = self.get_datalake_consumer_policy()

        cdk.CfnOutput(
            self,
            f'Export{target_environment}{self.logical_id_prefix}ConsumerPolicyArn',
            value=self.data_lake_consumer_policy.managed_policy_arn,
            export_name=f'{target_environment}{self.logical_id_prefix}ConsumerPolicyArn'
        )

        cdk.CfnOutput(
            self,
            f'Export{target_environment}{self.logical_id_prefix}ConsumerPolicy',
            value=self.data_lake_consumer_policy.managed_policy_name,
            export_name=f'{target_environment}{self.logical_id_prefix}ConsumerPolicy'
        )

    def get_datalake_consumer_policy(self) -> iam.ManagedPolicy:
        """Creates a customer managed policy to be attached to data lake consumer roles

        Returns
        -------
        iam.ManagedPolicy
            The IAM Managed Policy that was created
        """
        datalake_consumer_policy = iam.ManagedPolicy(
            self,
            f'{self.target_environment}{self.logical_id_prefix}ConsumerPolicy',
            description='InsuranceLake Consumer IAM Managed Policy',
            managed_policy_name=f'{self.target_environment.lower()}-{self.resource_name_prefix}-{self.region}-consumer-policy',
            document=iam.PolicyDocument(statements=[
                iam.PolicyStatement(
                    sid='S3BucketReadAccess',
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:GetObject',
                        's3:GetObjectVersion',
                        's3:ListBucket',
                    ],
                    resources=[
                        self.buckets.conformed.bucket_arn,
                        self.buckets.conformed.arn_for_objects('*'),
                        self.buckets.purposebuilt.bucket_arn,
                        self.buckets.purposebuilt.arn_for_objects('*'),
                    ]
                ),
                # This is required due to bucket level encryption on S3 Buckets
                iam.PolicyStatement(
                    sid='KmsAccess',
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'kms:Decrypt',
                        'kms:GenerateDataKey',
                    ],
                    resources=[
                        self.buckets.s3_kms_key.key_arn,
                    ]
                ),
                iam.PolicyStatement(
                    sid='AthenaWildcardResourceAccess',
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'athena:ListWorkGroups',
                        'athena:ListDataCatalogs',
                        'athena:GetCatalogs',
                        'athena:GetNamespaces',
                        'athena:GetExecutionEngine',
                        'athena:GetExecutionEngines',
                        'athena:GetTables',
                        'athena:GetTable',
                    ],
                    resources=[
                        '*',
                    ]
                ),
                iam.PolicyStatement(
                    sid='AthenaWorkgroupAccess',
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'athena:GetWorkGroup',
                        'athena:StartQueryExecution',
                        'athena:GetQueryExecution',
                        'athena:BatchGetQueryExecution',
                        'athena:ListQueryExecutions',
                        'athena:StopQueryExecution',
                        'athena:GetQueryResults',
                        'athena:GetQueryResultsStream',
                        'athena:DeleteNamedQuery',
                        'athena:GetNamedQuery',
                        'athena:ListNamedQueries',
                        'athena:CreateNamedQuery',
                        'athena:BatchGetNamedQuery',
                    ],
                    resources=[
                        f'arn:aws:athena:{self.region}:{self.account}:workgroup/*',
                    ]
                ),
                iam.PolicyStatement(
                    sid='AthenaDataCatalogAccess',
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'athena:ListDatabases',
                        'athena:GetDataCatalog',
                        'athena:GetDatabase',
                        'athena:GetTableMetadata',
                        'athena:ListTableMetadata',
                    ],
                    resources=[
                        f'arn:aws:athena:{self.region}:{self.account}:datacatalog/*',
                    ],
                ),
                iam.PolicyStatement(
                    sid='GlueCatalogReadAccess',
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'lakeformation:GetDataAccess',  # *
                        'glue:GetDatabase',             # catalog/database
                        'glue:GetDatabases',            # catalog/database
                        'glue:GetTable',                # catalog/database/table
                        'glue:GetTables',               # catalog/database/table
                        'glue:GetPartition',            # catalog/database/table
                        'glue:GetPartitions',           # catalog/database/table
                        'glue:BatchGetPartition',       # catalog/database/table
                    ],
                    resources=[
                        '*',
                    ]
                ),
                iam.PolicyStatement(
                    sid='GlueTempBucketAccess',
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:GetBucketLocation',
                        's3:GetObject',
                        's3:ListBucket',
                        's3:ListBucketMultipartUploads',
                        's3:ListMultipartUploadParts',
                        's3:AbortMultipartUpload',
                        's3:CreateBucket',
                        's3:PutObject',
                        's3:PutBucketPublicAccessBlock',
                    ],
                    resources=[
                        self.glue_scripts_temp_bucket.bucket_arn,
                        self.glue_scripts_temp_bucket.arn_for_objects('*'),
                    ]
                ),
            ]),
        )

        NagSuppressions.add_resource_suppressions(datalake_consumer_policy, [
            {
                'id': 'AwsSolutions-IAM5',
                'reason': 'Specified Athena and Glue Catalog actions must operate on wildcard resources'
            },
        ])

        return datalake_consumer_policy