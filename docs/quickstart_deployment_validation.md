---
title: Deployment Validation
parent: Getting Started
nav_order: 2
layout: home
last_modified_date: 2024-09-26
---
# InsuranceLake Deployment Validation

1. Transfer the sample claim data to the Collect bucket (Source system: SyntheticData, Table: ClaimData).
   ```bash
   aws s3 cp resources/syntheticgeneral-claim-data.csv s3://<Collect S3 bucket>/SyntheticGeneralData/ClaimData/
   ```

1. Transfer the sample policy data to the Collect bucket (Source system: SyntheticData, Table: PolicyData).
   ```bash
   aws s3 cp resources/syntheticgeneral-policy-data.csv s3://<Collect S3 bucket>/SyntheticGeneralData/PolicyData/
   ```

1. Upon successful transfer of the file, an event notification from S3 will trigger the state-machine-trigger Lambda function.

1. The Lambda function will insert a record into the DynamoDB table `{environment}-{resource_name_prefix}-etl-job-audit` to track job start status.

1. The Lambda function will also trigger the Step Functions State Machine. The State Machine execution name will be `<filename>-<YYYYMMDDHHMMSSxxxxxx>` and have the required metadata as input parameters.

1. The State Machine will trigger the AWS Glue job for Collect to Cleanse data processing.

1. The Collect to Cleanse AWS Glue job will execute the transformation logic defined in configuration files.

1. The AWS Glue job will load the data into the Cleanse bucket using the provided metadata. The data will be stored in S3 as `s3://{environment}-{resource_name_prefix}-{account}-{region}-cleanse/syntheticgeneraldata/claimdata/year=YYYY/month=MM/day=DD` in Apache Parquet format.

1. The AWS Glue job will create or update the AWS Glue Catalog table using the table name passed as a parameter based on the folder name (`PolicyData` and `ClaimData`).

1. After the Collect to Cleanse AWS Glue job completes, the State Machine will trigger the Cleanse to Consume AWS Glue job.

1. The Cleanse to Consume AWS Glue job will execute the SQL logic defined in configuration files.

1. The Cleanse to Consume AWS Glue job will store the resulting data set in S3 as `s3://{environment}-{resource_name_prefix}-{account}-{region}-consume/syntheticgeneraldata/claimdata/year=YYYY/month=MM/day=DD` in Apache Parquet format.

1. The Cleanse to Consume AWS Glue job will create or update the AWS Glue Catalog table.

1. After successful completion of the Cleanse to Consume AWS Glue job, the State Machine will trigger the etl-job-auditor Lambda function to update the DynamoDB table `{environment}-{resource_name_prefix}-etl-job-audit` with the latest status.

1. An Amazon Simple Notification Service (Amazon SNS) notification will be sent to all subscribed users.

1. To validate the data load, use Athena and execute the following query:

    ```sql
    select * from syntheticgeneraldata_consume.policydata limit 100
    ```