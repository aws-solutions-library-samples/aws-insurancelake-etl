# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_kms as kms

from .configuration import (
    AVAILABILITY_ZONE_1, AVAILABILITY_ZONE_2, AVAILABILITY_ZONE_3, 
    ROUTE_TABLE_1, ROUTE_TABLE_2, ROUTE_TABLE_3,
	SUBNET_ID_1, SUBNET_ID_2, SUBNET_ID_3, SHARED_SECURITY_GROUP_ID, VPC_ID, VPC_CIDR,
    S3_RAW_BUCKET, S3_CONFORMED_BUCKET, S3_PURPOSE_BUILT_BUCKET, S3_ACCESS_LOG_BUCKET, S3_KMS_KEY,
)

class ImportedBuckets():
     def __init__(
        self,
        stack: cdk.Stack,
        logical_id_suffix: str,
    ):
        """Add imported buckets from Infrastructure Cloudformation exports to provided stack

        Parameters
        ----------
        stack
            Stack in which to add buckets and use for mappings
        logical_id_suffix
            Suffix to differentiate construct IDs of imported resources
        """
        # Mapping should be a defined property in the calling stack
        s3_kms_key_parameter = cdk.Fn.import_value(stack.mappings[S3_KMS_KEY])
        self.s3_kms_key = kms.Key.from_key_arn(stack, f'ImportedKmsKey{logical_id_suffix}', s3_kms_key_parameter)

        access_logs_bucket_name = cdk.Fn.import_value(stack.mappings[S3_ACCESS_LOG_BUCKET])
        self.access_logs = s3.Bucket.from_bucket_attributes(
            stack,
            f'ImportedAccessLogsBucket{logical_id_suffix}',
            bucket_name=access_logs_bucket_name
        )
        raw_bucket_name = cdk.Fn.import_value(stack.mappings[S3_RAW_BUCKET])
        self.raw = s3.Bucket.from_bucket_name(
            stack,
            id=f'ImportedCollectBucket{logical_id_suffix}',
            bucket_name=raw_bucket_name
        )
        conformed_bucket_name = cdk.Fn.import_value(stack.mappings[S3_CONFORMED_BUCKET])
        self.conformed = s3.Bucket.from_bucket_name(
            stack,
            id=f'ImportedCleanseBucket{logical_id_suffix}',
            bucket_name=conformed_bucket_name
        )
        purposebuilt_bucket_name = cdk.Fn.import_value(stack.mappings[S3_PURPOSE_BUILT_BUCKET])
        self.purposebuilt = s3.Bucket.from_bucket_name(
            stack,
            id=f'ImportedConsumeBucket{logical_id_suffix}',
            bucket_name=purposebuilt_bucket_name
        )

class ImportedVpc():
    def __init__(
        self,
        stack: cdk.Stack,
        logical_id_suffix: str,
    ):
        """Add imported VPC and related resources from Infrastructure Cloudformation exports
        to provided stack

        Parameters
        ----------
        stack
            Stack in which to add VPC and and related constructs, and use for mappings
        logical_id_suffix
            Suffix to differentiate construct IDs of imported resources
        """
        # Mapping should be a defined property in the calling stack
        shared_security_group_parameter = cdk.Fn.import_value(stack.mappings[SHARED_SECURITY_GROUP_ID])
        self.shared_security_group = ec2.SecurityGroup.from_security_group_id(
            stack,
            f'ImportedSecurityGroup{logical_id_suffix}',
            shared_security_group_parameter
        )

        self.subnets = []

        if VPC_CIDR not in stack.mappings:
            # Environment configuration does not have a VPC CIDR defined, so VPC exports are not expected
            return

        vpc_id = cdk.Fn.import_value(stack.mappings[VPC_ID])

        # VPC Stack from Infrastructure ensures that 3 AZs and associated valid outputs are always created
        vpc_azs = []
        for az_number in range(3):
            az_mapping_element = globals()[f'AVAILABILITY_ZONE_{az_number + 1}']
            vpc_azs.append(cdk.Fn.import_value(stack.mappings[az_mapping_element]))

        vpc_subnet_ids = []
        for subnet_number in range(3):
            subnet_mapping_element = globals()[f'SUBNET_ID_{subnet_number + 1}']
            vpc_subnet_ids.append(cdk.Fn.import_value(stack.mappings[subnet_mapping_element]))

        vpc_route_tables = []
        for rt_number in range(3):
            rt_mapping_element = globals()[f'ROUTE_TABLE_{rt_number + 1}']
            vpc_route_tables.append(cdk.Fn.import_value(stack.mappings[rt_mapping_element]))

        # Manually construct the VPC because it lives in the target
        # account, not the Deployment account where the synth is ran
        self.vpc = ec2.Vpc.from_vpc_attributes(
            stack,
            f'ImportedVpc{logical_id_suffix}',
            vpc_id=vpc_id,
            availability_zones=vpc_azs,
            private_subnet_ids=vpc_subnet_ids,
            private_subnet_route_table_ids=vpc_route_tables,
        )

        # Some resources require specific subnets to access the VPC, such a Glue Connections
        self.subnets = [
            ec2.Subnet.from_subnet_attributes(
                stack,
                f'ImportedSubnet{logical_id_suffix}{az_number + 1}',
                subnet_id=vpc_subnet_ids[az_number],
                availability_zone=vpc_azs[az_number],
                route_table_id=vpc_route_tables[az_number]
            )
            for az_number in range(len(vpc_azs))
        ]