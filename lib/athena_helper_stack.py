# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
from constructs import Construct
import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_athena as athena
from cdk_nag import NagSuppressions

from .stack_import_helper import ImportedBuckets
from .configuration import (
    DEV, PROD, TEST, get_logical_id_prefix, get_resource_name_prefix, get_environment_configuration,
)

class AthenaHelperStack(cdk.Stack):
    def __init__(
            self, 
            scope: Construct, 
            construct_id: str, 
            target_environment: str, 
            glue_scripts_temp_bucket: s3.Bucket,
            **kwargs
        ):
        """CloudFormation stack to create Athena workgroup

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

        if target_environment == PROD or target_environment == TEST:
            recursive_delete = False
        else:
            recursive_delete = True

        buckets = ImportedBuckets(self, logical_id_suffix='AthenaHelperStack')

        athena_workgroup = athena.CfnWorkGroup(
            self,
            f'{target_environment}{logical_id_prefix}AthenaWorkgroup',
            name=resource_name_prefix,
            description='InsuranceLake ETL Maintainer helper workgroup ',
            recursive_delete_option=recursive_delete,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                publish_cloud_watch_metrics_enabled=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f's3://{glue_scripts_temp_bucket.bucket_name}/query-results/',
                    # Glue Scripts Temp Bucket from Glue Stack has server-side encryption enabled
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option='SSE_KMS',
                        kms_key=buckets.s3_kms_key.key_arn,
                    )
                )
            )
        )
        NagSuppressions.add_resource_suppressions(athena_workgroup, [
            {
                'id': 'AwsSolutions-ATH1',
                'reason': 'Glue Scripts Temp Bucket from Glue Stack has server-side encryption enabled; query results encryption would be redundant'
            },
        ])