# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import aws_cdk as cdk
from constructs import Construct
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_logs as logs

from .stack_import_helper import ImportedBuckets
from .configuration import (
    DEV, PROD, TEST, get_logical_id_prefix, get_resource_name_prefix, get_environment_configuration,
)

class GlueBucketsStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        target_environment: str,
        **kwargs
    ):
        """CloudFormation stack to create S3 buckets used by Glue Jobs.

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

        self.target_environment = target_environment
        self.mappings = get_environment_configuration(target_environment)
        self.logical_id_prefix = get_logical_id_prefix()
        self.resource_name_prefix = get_resource_name_prefix()
        
        if target_environment in [ TEST, PROD ]:
            self.removal_policy = cdk.RemovalPolicy.RETAIN
            self.log_retention = logs.RetentionDays.SIX_MONTHS
            self.temp_object_expiration_days = cdk.Duration.days(365)
            self.noncurrent_version_expiration_days = cdk.Duration.days(365)
        else:
            self.removal_policy = cdk.RemovalPolicy.DESTROY
            self.log_retention = logs.RetentionDays.ONE_MONTH
            self.temp_object_expiration_days = cdk.Duration.days(30)
            self.noncurrent_version_expiration_days = cdk.Duration.days(30)

        self.buckets = ImportedBuckets(self, logical_id_suffix='GlueBucketsStack')

        self.glue_scripts_bucket = self.get_glue_scripts_bucket()
        self.glue_scripts_temp_bucket = self.get_glue_scripts_temporary_bucket()

        # These are all duplicate exports with the built-in bucket construct, but are more easily identifiable
        cdk.CfnOutput(
            self,
            f'Export{target_environment}{self.logical_id_prefix}GlueScriptsBucket',
            value=self.glue_scripts_bucket.bucket_name,
            export_name=f'{target_environment}{self.logical_id_prefix}GlueScriptsBucket'
        )

        cdk.CfnOutput(
            self,
            f'Export{target_environment}{self.logical_id_prefix}GlueScriptsBucketArn',
            value=self.glue_scripts_bucket.bucket_arn,
            export_name=f'{target_environment}{self.logical_id_prefix}GlueScriptsBucketArn'
        )

        cdk.CfnOutput(
            self,
            f'Export{target_environment}{self.logical_id_prefix}GlueScriptsTempBucket',
            value=self.glue_scripts_temp_bucket.bucket_name,
            export_name=f'{target_environment}{self.logical_id_prefix}GlueScriptsTempBucket'
        )

        cdk.CfnOutput(
            self,
            f'Export{target_environment}{self.logical_id_prefix}GlueScriptsTempBucketArn',
            value=self.glue_scripts_temp_bucket.bucket_arn,
            export_name=f'{target_environment}{self.logical_id_prefix}GlueScriptsTempBucketArn'
        )

    def get_glue_scripts_bucket(self) -> s3.Bucket:
        """Creates S3 Bucket that contains glue scripts used in Job execution.

        Returns
        -------
        s3.Bucket
            Glue scripts bucket construct
        """
        bucket_name = f'{self.target_environment.lower()}-{self.resource_name_prefix}-{self.account}-{self.region}-etl-scripts'
        lifecycle_rules = [
            s3.LifecycleRule(
                enabled=True,
                noncurrent_version_expiration=self.noncurrent_version_expiration_days,
            )
        ]
        bucket = s3.Bucket(
            self,
            f'{self.target_environment}{self.logical_id_prefix}GlueScriptsBucket',
            bucket_name=bucket_name,
            access_control=s3.BucketAccessControl.PRIVATE,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            bucket_key_enabled=self.buckets.s3_kms_key is not None,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.buckets.s3_kms_key,
            public_read_access=False,
            removal_policy=self.removal_policy,
            versioned=True,
            lifecycle_rules=lifecycle_rules,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,
            server_access_logs_bucket=self.buckets.access_logs,
            server_access_logs_prefix=f'{bucket_name}-',
        )

        return bucket

    def get_glue_scripts_temporary_bucket(self) -> s3.Bucket:
        """Creates S3 Bucket used as a temporary file store in Job execution

        Returns
        -------
        s3.Bucket
            Glue scripts bucket construct
        """
        bucket_name = f'{self.target_environment.lower()}-{self.resource_name_prefix}-{self.account}-{self.region}-glue-temp'
        lifecycle_rules = [
            s3.LifecycleRule(
                enabled=True,
                expiration=self.temp_object_expiration_days,
                noncurrent_version_expiration=self.noncurrent_version_expiration_days,
            )
        ]
        return s3.Bucket(
            self,
            f'{self.target_environment}{self.logical_id_prefix}GlueScriptsTemporaryBucket',
            bucket_name=bucket_name,
            access_control=s3.BucketAccessControl.PRIVATE,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            bucket_key_enabled=self.buckets.s3_kms_key is not None,
            encryption=s3.BucketEncryption.KMS if self.buckets.s3_kms_key else s3.BucketEncryption.S3_MANAGED,
            encryption_key=self.buckets.s3_kms_key if self.buckets.s3_kms_key else None,
            public_read_access=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True,
            lifecycle_rules=lifecycle_rules,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,
            server_access_logs_bucket=self.buckets.access_logs,
            server_access_logs_prefix=f'{bucket_name}-',
        )