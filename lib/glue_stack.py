# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import aws_cdk as cdk
from constructs import Construct
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_logs as logs
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
#import aws_cdk.aws_kms as kms
import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_s3_deployment as s3_deployment
import os
from cdk_nag import NagSuppressions

from .stack_import_helper import ImportedBuckets, ImportedVpc
from .configuration import (
    DEV, PROD, TEST, get_logical_id_prefix, get_resource_name_prefix, get_environment_configuration,
)

class GlueStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        target_environment: str,
        hash_values_table: dynamodb.Table,
        value_lookup_table: dynamodb.Table,
        multi_lookup_table: dynamodb.Table,
        dq_results_table: dynamodb.Table,
        data_lineage_table: dynamodb.Table = None,
        **kwargs
    ):
        """CloudFormation stack to create Glue Jobs, Connections,
        Script Bucket, Temporary Bucket, and an IAM role for permissions.

        Parameters
        ----------
        scope
            Parent of this stack, usually an App or a Stage, but could be any construct
        construct_id
            The construct ID of this stack; if stackName is not explicitly defined,
            this ID (and any parent IDs) will be used to determine the physical ID of the stack
        target_environment
            The target environment for stacks in the deploy stage
        hash_values_table
            The DynamoDB Table for storing original values from hashing function
        value_lookup_table
            The DynamoDB Table for looking up values in the multi lookup transform
        multi_lookup_table
            The DynamoDB Table for looking up values in the multi lookup transform
        dq_results_table
            The DynamoDB Table for storing Glue Data Quality results
        data_lineage_table: optional
            Optional DynamoDB Table for storing custom data lineage information for Glue job
            transforms; omitting the parameter disables custom data lineage tracking
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

        self.buckets = ImportedBuckets(self, logical_id_suffix='GlueStack')
        vpc = ImportedVpc(self, logical_id_suffix='GlueStack')

        self.glue_scripts_bucket = self.get_glue_scripts_bucket()
        self.glue_scripts_temp_bucket = self.get_glue_scripts_temporary_bucket()
        self.glue_role = self.get_glue_role(
            buckets=[
                self.buckets.raw,
                self.buckets.conformed,
                self.buckets.purposebuilt,
                self.glue_scripts_bucket,
                self.glue_scripts_temp_bucket,
            ],
            dynamodb_tables=[
                hash_values_table,
                value_lookup_table,
                multi_lookup_table,
                dq_results_table,
                data_lineage_table,
            ],
        )

        # Each module must be listed specifically for --extra-py-files and --extra-jars (or Zip)
        glue_libraries = [
            f's3://{self.glue_scripts_bucket.bucket_name}/etl/lib/{dirent.name}'
                for dirent in os.scandir('lib/glue_scripts/lib')
                    if os.path.splitext(dirent.path)[1] == '.py'
        ]
        spark_libraries = [
            f's3://{self.glue_scripts_bucket.bucket_name}/etl/lib/{dirent.name}'
                for dirent in os.scandir('lib/glue_scripts/lib')
                    if os.path.splitext(dirent.path)[1] == '.jar'
        ]

        job_connections = [
            glue.CfnConnection(
                self,
                f'{target_environment}{self.logical_id_prefix}GlueETLConnection{subnet_number + 1}',
                catalog_id=self.account,
                connection_input=glue.CfnConnection.ConnectionInputProperty(
                    connection_type="NETWORK",
                    name=f'{target_environment.lower()}-{self.resource_name_prefix}-glue-etl-connection-{subnet_number + 1}',
                    description=f'Data Catalog Connection for InsuranceLake VPC Subnet {subnet_number + 1}',
                    physical_connection_requirements=glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                        availability_zone=vpc.subnets[subnet_number].availability_zone,
                        subnet_id=vpc.subnets[subnet_number].subnet_id,
                        security_group_id_list=[vpc.shared_security_group.security_group_id]
                    )
                )
            )
            for subnet_number in range(len(vpc.subnets))
        ]

        # Consume-Entity-Match AWS Glue job cannot use extra-jars until it is upgraded to Gluev5
        common_default_arguments = {
                '--enable-auto-scaling': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-continuous-log-filter': 'true',
                '--enable-metrics': 'true',
                '--enable-observability-metrics': 'true',
                '--enable-spark-ui': 'true',
                '--enable-glue-datacatalog': 'true',
                '--user-jars-first': 'true',
                '--extra-py-files': ','.join(glue_libraries),
                '--environment': self.target_environment,
                '--txn_bucket': f's3://{self.glue_scripts_bucket.bucket_name}',
                '--data_lineage_table': data_lineage_table.table_name if data_lineage_table else None,
        }

        self.collect_to_cleanse_job = glue.CfnJob(
            self,
            f'{target_environment}{self.logical_id_prefix}CollectToCleanseJob',
            name=f'{target_environment.lower()}-{self.resource_name_prefix}-collect-to-cleanse-job',
            description='PySpark Glue job data processing logic to cleanse and curate collected source system data',
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{self.glue_scripts_bucket.bucket_name}/etl/etl_collect_to_cleanse.py'
            ),
            # Used if Glue job needs connections to resources in VPCs (incurs VPC costs and may trigger IP limitations)
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[ job_connection.connection_input.name for job_connection in job_connections ],
            ) if job_connections else None,
            # These arguments are common to all Glue job runs and are overlayed by the arguments
            # definition in the calling Step Functions GlueStartJobRun
            default_arguments=common_default_arguments | {
                '--additional-python-modules': 'rapidfuzz',
                '--extra-jars': ','.join(spark_libraries) if spark_libraries else None,
                '--TempDir': f's3://{self.glue_scripts_temp_bucket.bucket_name}/etl/collect_to_cleanse/',
                '--spark-event-logs-path': f's3://{self.glue_scripts_temp_bucket.bucket_name}/spark-ui/collect_to_cleanse/',
                '--txn_spec_prefix_path': '/etl/transformation-spec/',
                '--source_bucket': f's3://{self.buckets.raw.bucket_name}',
                '--target_bucket': f's3://{self.buckets.conformed.bucket_name}',
                '--hash_value_table': hash_values_table.table_name,
                '--value_lookup_table': value_lookup_table.table_name,
                '--multi_lookup_table': multi_lookup_table.table_name,
                '--dq_results_table': dq_results_table.table_name,
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=10,
            ),
            glue_version='5.0',
            max_retries=0,
            # With auto-scaling, this represents the maximum number of workers
            # If using a Connection, there must be enough IP addresses for each worker
            number_of_workers=25,
            role=self.glue_role.role_arn,
            worker_type='G.1X',
            # TODO: Allow the user to specify a user-managed, out-of-stack security group name
            # Glue security configurations cannot be updated by Cloudformation and break all stack updates
            #security_configuration='',
        )

        self.cleanse_to_consume_job = glue.CfnJob(
            self,
            f'{target_environment}{self.logical_id_prefix}CleanseToConsumeJob',
            name=f'{target_environment.lower()}-{self.resource_name_prefix}-cleanse-to-consume-job',
            description='PySpark Glue job data processing logic to prepare cleansed Data Lake tables for specific analytics consumption',
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{self.glue_scripts_bucket.bucket_name}/etl/etl_cleanse_to_consume.py'
            ),
            # Used if Glue job needs connections to resources in VPCs (incurs VPC costs and may trigger IP limitations)
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[ job_connection.connection_input.name for job_connection in job_connections ],
            ) if job_connections else None,
            # These arguments are common to all Glue job runs and are overlayed by the arguments
            # definition in the calling Step Functions GlueStartJobRun
            default_arguments=common_default_arguments | {
                '--extra-jars': ','.join(spark_libraries) if spark_libraries else None,
                '--TempDir': f's3://{self.glue_scripts_temp_bucket.bucket_name}/etl/cleanse_to_consume/',
                '--spark-event-logs-path': f's3://{self.glue_scripts_temp_bucket.bucket_name}/spark-ui/cleanse_to_consume/',
                '--txn_sql_prefix_path': '/etl/transformation-sql/',
                '--source_bucket': f's3://{self.buckets.conformed.bucket_name}',
                '--target_bucket': f's3://{self.buckets.purposebuilt.bucket_name}',
                '--dq_results_table': dq_results_table.table_name,
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=10,
            ),
            glue_version='5.0',
            max_retries=0,
            # With auto-scaling, this represents the maximum number of workers
            # If using a Connection, there must be enough IP addresses for each worker
            number_of_workers=25,
            role=self.glue_role.role_arn,
            worker_type='G.1X',
            # TODO: Allow the user to specify a user-managed, out-of-stack security group name
            # Glue security configurations cannot be updated by Cloudformation and break all stack updates
            #security_configuration='',
        )

        self.consume_entity_match_job = glue.CfnJob(
            self,
            f'{target_environment}{self.logical_id_prefix}ConsumeEntityMatchJob',
            name=f'{target_environment.lower()}-{self.resource_name_prefix}-consume-entity-match-job',
            description='PySpark Glue job data processing logic to match records in Consume layer with primary set',
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{self.glue_scripts_bucket.bucket_name}/etl/etl_consume_entity_match.py'
            ),
            # Used if Glue job needs connections to resources in VPCs (incurs VPC costs and may trigger IP limitations)
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[ job_connection.connection_input.name for job_connection in job_connections ],
            ) if job_connections else None,
            # These arguments serve as defaults and base values that are overlayed and/or overriden
            # by the arguments definition in the calling Step Functions GlueStartJobRun
            default_arguments=common_default_arguments | {
                '--additional-python-modules': 'recordlinkage',
                '--TempDir': f's3://{self.glue_scripts_temp_bucket.bucket_name}/etl/consume_entity_match/',
                '--spark-event-logs-path': f's3://{self.glue_scripts_temp_bucket.bucket_name}/spark-ui/consume_entity_match/',
                '--source_bucket': f's3://{self.buckets.conformed.bucket_name}',
                '--target_bucket': f's3://{self.buckets.purposebuilt.bucket_name}',
                '--txn_spec_prefix_path': '/etl/transformation-spec/',
                '--iceberg_catalog': 'glue_catalog',
                '--datalake-formats': 'iceberg',
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=10,
            ),
            glue_version='4.0', # Glue v5 does not yet have this fix: https://issues.apache.org/jira/browse/SPARK-48871
            max_retries=0,
            # With auto-scaling, this represents the maximum number of workers
            # If using a Connection, there must be enough IP addresses for each worker
            number_of_workers=25,
            role=self.glue_role.role_arn,
            worker_type='G.1X',
        )
        # Recommended encryption settings for account Glue Data Catalog
        # Applies to all databases and tables in the account; uncomment to apply
        # glue.CfnDataCatalogEncryptionSettings(
        #     self,
        #     f'{target_environment}{self.logical_id_prefix}DataCatalogEncryptionSettings',
        #     catalog_id=f'{self.account}',
        #     data_catalog_encryption_settings=glue.CfnDataCatalogEncryptionSettings.DataCatalogEncryptionSettingsProperty(
        #         connection_password_encryption=glue.CfnDataCatalogEncryptionSettings.ConnectionPasswordEncryptionProperty(
        #             kms_key_id=buckets.s3_kms_key.key_arn,
        #             return_connection_password_encrypted=True,
        #         ),
        #         encryption_at_rest=glue.CfnDataCatalogEncryptionSettings.EncryptionAtRestProperty(
        #             catalog_encryption_mode="SSE-KMS",
        #             sse_aws_kms_key_id=buckets.s3_kms_key.key_arn,
        #         )
        #     )
        # )

        NagSuppressions.add_resource_suppressions(self, [
            {
                'id': 'AwsSolutions-GL1',
                'reason': 'Creating a Glue security configuration in CDK prevents the stack from updating; log messages are free from sensitive data'
            },
            {
                'id': 'AwsSolutions-GL3',
                'reason': 'Creating a Glue security configuration in CDK prevents the stack from updating; the Glue job has bookmarks disabled'
            }
        ], apply_to_children=True)

        # Customer Managed Policy to attach to roles needing data lake read access
        self.data_lake_consumer_policy = self.get_datalake_consumer_policy()

        cdk.CfnOutput(
            self,
            f'Export{target_environment}{self.logical_id_prefix}GlueRole',
            value=self.glue_role.role_name,
            export_name=f'{target_environment}{self.logical_id_prefix}GlueRole'
        )

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


    def get_glue_scripts_bucket(self) -> s3.Bucket:
        """Creates S3 Bucket that contains glue scripts used in Job execution

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

        # Dynamically upload resources to the script target
        # TODO: Apply resource retention policy to Custom Resource Lambda log group
        s3_deployment.BucketDeployment(
            self,
            'DeployGlueJobScript',
            # This path is relative to the root of the project
            sources=[ s3_deployment.Source.asset('./lib/glue_scripts') ],
            destination_bucket=bucket,
            destination_key_prefix='etl',
            prune=self.removal_policy == cdk.RemovalPolicy.DESTROY,
            retain_on_delete=self.removal_policy == cdk.RemovalPolicy.RETAIN,
            log_retention=self.log_retention,
            memory_limit=256,
        )

        NagSuppressions.add_resource_suppressions(self, [
            {
                'id': 'AwsSolutions-IAM4',
                'reason': 'S3 Deployment CustomResource used only during stack deployment and deletion'
            },
            {
                'id': 'AwsSolutions-IAM5',
                'reason': 'S3 Deployment CustomResource used only during stack deployment and deletion'
            },
            {
                'id': 'AwsSolutions-L1',
                'reason': 'Deployment CustomResource Lambda is maintained by the CDK team and that team needs to update the runtime'
            },
        ], apply_to_children=True)

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


    def get_glue_role(
        self,
        buckets: list,
        dynamodb_tables: list,
    ) -> iam.Role:
        """Creates the role used during Glue Job execution

        Parameters
        ----------
        buckets
            List of S3 Bucket constructs to use for S3 policy resources
        dynamodb_tables
            List of DynamoDB Table constructs to use for DynamoDB policy resources

        Returns
        -------
        iam.Role
            The IAM role that was created
        """
        bucket_object_resources = [ bucket.arn_for_objects('*') for bucket in buckets ]
        bucket_resources = [ bucket.bucket_arn for bucket in buckets ]
        dynamodb_resources = [ table.table_arn for table in dynamodb_tables if table is not None ]

        glue_role = iam.Role(
            self,
            f'{self.target_environment}{self.logical_id_prefix}GlueRole',
            description='Role for InsuranceLake ETL pipeline Glue Jobs',
            role_name=f'{self.target_environment.lower()}-{self.resource_name_prefix}-{self.region}-glue-role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            inline_policies={
                'S3BucketAccess':
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            's3:ListBucketVersions',
                            's3:ListBucket',
                            's3:GetBucketNotification',
                            's3:GetBucketLocation',
                        ],
                        resources=bucket_resources
                    ),
                    # TODO: Remove glue-scripts bucket from the resource list
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            's3:ReplicateObject',
                            's3:GetObjectVersion',
                            's3:PutObject',
                            's3:GetObject',
                            's3:DeleteObject',
                        ],
                        resources=bucket_object_resources
                    ),
                    # This is required due to bucket level encryption on S3 Buckets
                    # TODO: key_arn is optional in the parent stack, make this conditional
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'kms:Decrypt',
                            'kms:GenerateDataKey',
                        ],
                        resources=[
                            self.buckets.s3_kms_key.key_arn,
                        ]
                    )
                ]),
                'DynamoDBAccess':
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'dynamodb:PutItem',
                            'dynamodb:GetItem',
                            'dynamodb:DeleteItem',
                            'dynamodb:UpdateItem',
                            'dynamodb:DescribeTable',
                            'dynamodb:BatchWriteItem',
                            'dynamodb:BatchReadItem',
                            'dynamodb:Query',
                        ],
                        resources=dynamodb_resources,
                    )
                ]),
                # This is required to emit lineage events to a DataZone domain
                'DataZoneAccess':
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'datazone:PostLineageEvent',
                        ],
                        resources=[
                            f'arn:aws:datazone:{self.region}:{self.account}:domain/*',
                        ]
                    )
                ]),
                'AthenaAccess':
                iam.PolicyDocument(statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'athena:GetQueryExecution',
                            'athena:GetQueryResults',
                            'athena:StartQueryExecution',
                        ],
                        resources=[
                            f'arn:aws:athena:{self.region}:{self.account}:workgroup/*',
                        ]
                    )
                ]),
            },
            # TODO: Convert this attached policy to specifically needed permissions and remove nag suppression
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
            ]
        )

        NagSuppressions.add_resource_suppressions(glue_role, [
            {
                'id': 'AwsSolutions-IAM4',
                'reason': 'Glue Job should use built-in Glue Service Role'
            },
            {
                'id': 'AwsSolutions-IAM5',
                'reason': 'AWS provided Glue Service Role uses wildcard permissions'
            },
        ])

        return glue_role


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