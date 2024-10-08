---
title: Cleanup
parent: Getting Started
nav_order: 4
last_modified_date: 2024-09-26
---
# Clean-up Instructions
{: .no_toc }

This page explains how to clean up resources and data from an InsuranceLake deployment. These instructions apply to all deployment methods (Quickstart, Quickstart with CI/CD, and Full Deployment).

## Contents
{: .no_toc }

* TOC
{:toc}

## Clean-up Workflow-created Resources

1. Use the `etl_cleanup.py` script to clear the S3 buckets, Data Catalog entries, logs, and DynamoDB tables:
   ```bash
   AWS_DEFAULT_REGION=us-east-2 resources/etl_cleanup.py --mode allbuckets
   ```

   You can also manually empty all six InsuranceLake S3 buckets (cleanse, collect, consume, etl-scripts, glue-temp, access-logs) before cleaning up the stacks (as detailed below).

   {: .important }
   If you want to retain Data Catalog entries, logs, and S3 bucket contents, **do not run the script above**. Follow the instructions below to remove all stack-created resources except the S3 buckets (which will fail due to them containing objects). The buckets follow the defined retention policy in [s3_bucket_zones_stack.py](https://github.com/aws-samples/aws-insurancelake-infrastructure/blob/main/lib/s3_bucket_zones_stack.py#L45).

## Clean-up ETL Stacks

{:style="counter-reset:none"}
1. Delete stacks using the command `cdk destroy --all`. When you see the following text, enter **y**, and press enter/return.

   ```bash
   Are you sure you want to delete: Test-InsuranceLakeEtlPipeline, Prod-InsuranceLakeEtlPipeline, Dev-InsuranceLakeEtlPipeline (y/n)?
   ```

   {: .note }
   This operation deletes the infrastructure pipeline stacks only in the central deployment account.

1. To delete stacks in **development** account, log onto the Dev account, go to [CloudFormation console](https://console.aws.amazon.com/cloudformation) and delete the following stacks in the order listed:

   {: .note }
   For each environment below, be sure to delete the stacks in the order they are listed, so that stack dependencies do not prevent deletion.

   1. Dev-InsuranceLakeEtlAthenaHelper
   1. Dev-InsuranceLakeEtlStepFunctions
   1. Dev-InsuranceLakeEtlGlue
   1. Dev-InsuranceLakeEtlDynamoDb

1. To delete stacks in **test** account, log onto the Test account, go to [CloudFormation console](https://console.aws.amazon.com/cloudformation) and delete the following stacks in the order listed:

   1. Test-InsuranceLakeEtlAthenaHelper
   1. Test-InsuranceLakeEtlStepFunctions
   1. Test-InsuranceLakeEtlGlue
   1. Test-InsuranceLakeEtlDynamoDb

1. To delete stacks in **prod** account, log onto the Prod account, go to [CloudFormation console](https://console.aws.amazon.com/cloudformation) and delete the following stacks in the order listed:

   1. Prod-InsuranceLakeEtlAthenaHelper
   1. Prod-InsuranceLakeEtlStepFunctions
   1. Prod-InsuranceLakeEtlGlue
   1. Prod-InsuranceLakeEtlDynamoDb

## Clean-up Infrastructure Stacks

{:style="counter-reset:none"}
1. Delete stacks using the command `cdk destroy --all`.

1. When you see the following text, enter **y**, and press enter:

   ```bash
   Are you sure you want to delete: Test-InsuranceLakeInfrastructurePipeline, Prod-InsuranceLakeInfrastructurePipeline, Dev-InsuranceLakeInfrastructurePipeline (y/n)?
   ```

   {: .note }
   This operation deletes the ETL pipeline stacks only in the central deployment account.

1. To delete stacks in **development** account, log onto the Dev account, go to [CloudFormation console](https://console.aws.amazon.com/cloudformation) and delete the following stacks:

   1. Dev-InsuranceLakeInfrastructureVpc
   1. Dev-InsuranceLakeInfrastructureS3BucketZones

1. To delete stacks in **test** account, log onto the Test account, go to [CloudFormation console](https://console.aws.amazon.com/cloudformation) and delete the following stacks:

   1. Test-InsuranceLakeInfrastructureVpc
   1. Test-InsuranceLakeInfrastructureS3BucketZones

1. To delete stacks in **prod** account, log onto the Prod account, go to [CloudFormation console](https://console.aws.amazon.com/cloudformation) and delete the following stacks:

   1. Prod-InsuranceLakeInfrastructureVpc
   1. Prod-InsuranceLakeInfrastructureS3BucketZones

## Clean-up CDK Bootstrap (optional)

If you are not using AWS CDK for other purposes, you can also delete the `CDKToolkit` stack in each target account.