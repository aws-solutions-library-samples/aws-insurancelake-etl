<!--
  Title: AWS InsuranceLake
  Description: Serverless modern data lake solution and reference architecture fit for the insurance industry built on AWS
  Author: cvisi@amazon.com
  -->
# InsuranceLake ETL

## Overview

The InsuranceLake solution is comprised of two codebases: [Infrastructure](https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure) and [ETL](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl). This codebase is specific to the ETL features (both infrastructure and application code), but the documentation that follows applies to the solution as a whole. For documentation with specific details on the Infrastructure, refer to the [InsuranceLake Infrastructure with CDK Pipeline README](https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure/blob/main/README.md).

This solution helps you deploy ETL processes and data storage resources to create InsuranceLake. It uses Amazon S3 buckets for storage, [AWS Glue](https://docs.aws.amazon.com/glue/) for data transformation, and [AWS CDK Pipelines](https://docs.aws.amazon.com/cdk/latest/guide/cdk_pipeline.html). The solution is originally based on the AWS blog [Deploy data lake ETL jobs using CDK Pipelines](https://aws.amazon.com/blogs/devops/deploying-data-lake-etl-jobs-using-cdk-pipelines/).

[CDK Pipelines](https://docs.aws.amazon.com/cdk/api/latest/docs/pipelines-readme.html) is a construct library module for painless continuous delivery of CDK applications. CDK stands for Cloud Development Kit. It is an open source software development framework to define your cloud application resources using familiar programming languages.

Specifically, this solution helps you to:

* Deploy a "3 Cs" (Collect, Cleanse, Consume) architecture InsuranceLake
* Deploy ETL jobs needed make common insurance industry data souces available in a data lake
* Use pySpark Glue jobs and supporting resoures to perform data transforms in a modular approach
* Build and replicate the application in multiple environments quickly
* Deploy ETL jobs from a central deployment account to multiple AWS environments such as Dev, Test, and Prod
* Leverage the benefit of self-mutating feature of CDK Pipelines; specifically, the pipeline itself is infrastructure as code and can be changed as part of the deployment
* Increase the speed of prototyping, testing, and deployment of new ETL jobs

![InsuranceLake High Level Architecture](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/insurancelake-highlevel-architecture.png)

## Contents

* [Cost](#cost)
    * [Sample Cost Table](#sample-cost-table)
* [Quickstart](#quickstart)
    * [Python/CDK Basics](#pythoncdk-basics)
    * [Deploy the Application](#deploy-the-application)
    * [Try out the ETL Process](#try-out-the-etl-process)
    * [Next Steps](#next-steps)
* [Quickstart with CI/CD](#quickstart-with-cicd)
* [Deployment Validation](#deployment-validation)
* [Cleanup](#cleanup)
* [Architecture](#architecture)
    * [InsuranceLake](#insurancelake-3-cs)
    * [ETL](#etl)
* [Security](#security)
    * [Infrastructure Code](#infrastructure-code)
    * [Application Code](#application-code)
* User Documentation
    * [Loading Data with InsuranceLake](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/loading_data.md)
    * [Detailed Collect-to-Cleanse Transform Reference](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/transforms.md)
    * [Schema Mapping Documentation](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/schema_mapping.md)
    * [File Formats and Input Specification Documentation](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/file_formats.md)
    * [Data Quality with Glue Data Quality Reference](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/data_quality.md)
    * [Using SQL for Cleanse-to-Consume](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/using_sql.md)
    * [Schema Evolution Details](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/schema_evolution.md)
* Developer Documentation
    * [Developer Guide](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/developer_guide.md)
    * [Full Deployment Guide](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/full_deployment_guide.md)
    * [AWS CDK Detailed Instructions](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/cdk_instructions.md)
* [Additional resources](#additional-resources)
* [Authors](#authors)
* [License Summary](#license-summary)

## Cost

This solution uses the following services: [Amazon Simple Storage Service (S3)](https://aws.amazon.com/s3/pricing/), [AWS Glue](https://aws.amazon.com/glue/pricing/), [AWS Step Functions](https://aws.amazon.com/step-functions/pricing/), [Amazon CloudWatch](https://aws.amazon.com/cloudwatch/pricing/), [Amazon Athena](https://aws.amazon.com/athena/pricing/), [AWS Cloud9](https://aws.amazon.com/cloud9/pricing/) (for recommended installation process only), [AWS CodePipeline](https://aws.amazon.com/codepipeline/pricing/) (for CI/CD installation only).

An estimated cost for following the [Quickstart](#quickstart) and [Quickstart with CI/CD](#quickstart-with-cicd) instructions, assuming a total of 8 Glue DPU hours and cleaning all resources when finished, **your cost will not be higher than $2**. This cost could be less as some services are included in the Free Tier.

_We recommend creating a [Budget](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html) through [AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/) to help manage costs. Prices are subject to change. For full details, refer to the pricing webpage for each AWS service used in this solution._

### Sample Cost Table

The following table provides a sample cost breakdown for deploying this Guidance with the default parameters in the US East (Ohio) Region for one month with pricing as of _9 July 2024_.

|AWS service    |Dimensions |Cost [USD]
|---    |---    |---
|AWS Glue   |per DPU-Hour for each Apache Spark or Spark Streaming job, billed per second with a 1-minute minimum   |$0.44
|Amazon S3  |per GB of storage used, Frequent Access Tier, first 50 TB per month<br>PUT, COPY, POST, LIST requests (per 1,000 requests)<br>GET, SELECT, and all other requests (per 1,000 requests)   |$0.023<br>$0.005<br>$0.0004
|Amazon Athena  |per TB of data scanned   |$5.00
|Amazon DynamoDB    |per million Write Request Units (WRU)<br>per million Read Request Units (RRU)  |$1.25<br>$0.25

## Quickstart

If you'd like to get started quickly transforming some sample raw insurance data and running SQL on the resulting dataset, and without worrying about CI/CD, follow this guide.

### Python/CDK Basics

1. Open the AWS Console and navigate to [AWS Cloud9](https://console.aws.amazon.com/cloud9control/home)
1. Select the region to create the Cloud9 environment (should be the same region as the stack; us-east-2 by default)
1. Select Create environment
1. Enter an environment name, for example, InsuranceLakeDemo
1. Select the t3.small instance size (CDK deployment requires more than 1 GB RAM)
1. Leave the Platform selected as Amazon Linux 2023
1. Adjust the timeout to your preference
1. Click Create
1. Open the environment you created and wait until it is available for use
1. Clone the repositories
    ```bash
    git clone https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure.git
    git clone https://github.com/aws-solutions-library-samples/aws-insurancelake-etl.git
    ```
1. Use a terminal or command prompt and change the working directory to the location of the _infrastructure_ code
    ```bash
    cd aws-insurancelake-infrastructure
    ```
1. Create a Python virtual environment
    ```bash
    python3 -m venv .venv
    ```
1. Activate the virtual environment
    ```bash
    source .venv/bin/activate
    ```
1. Install required Python libraries
    - NOTE: You may see a warning stating that a newer version is available; it is safe to ignore this for the Quickstart
    ```bash
    pip install -r requirements.txt
    ```
1. Bootstrap CDK in your AWS account
    - By default the solution will deploy resources to the `us-east-2` region
    ```bash
    cdk bootstrap
    ```

### Deploy the Application

1. Ensure you are still in the `aws-insurancelake-infrastructure` directory
1. Deploy infrastructure resources in the development environment (1 stack)
    ```bash
    cdk deploy Dev-InsuranceLakeInfrastructurePipeline/Dev/InsuranceLakeInfrastructureS3BucketZones
    ```
1. Review and accept IAM credential creation for the S3 bucket stack
    - Wait for deployment to finish (approx. 5 mins)
1. Copy the S3 bucket name for the Collect bucket to use later
    - Bucket name will be in the form: `dev-insurancelake-<AWS Account ID>-<Region>-collect`
1. Switch the working directory to the location of the _etl_ code
    ```bash
    cd ../aws-insurancelake-etl
    ```
1. Deploy the ETL resources in the development environment (4 stacks)
    ```bash
    cdk deploy Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlDynamoDb Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlGlue Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlStepFunctions Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlAthenaHelper
    ```
    - Wait for approximately 1 minute for DynamoDB deployment to finish
1. Review and accept IAM credential creation for the Glue jobs stack
    - Wait approximately 3 minutes for deployment to finish
1. Review and accept IAM credential creation for the Step Functions stack
    - Wait approximately 7 minutes for deployment of Step Functions and Athena Helper stacks to finish

### Try out the ETL Process

1. Populate the DynamoDB lookup table with sample lookup data
    ```bash
    AWS_DEFAULT_REGION=us-east-2 resources/load_dynamodb_lookup_table.py SyntheticGeneralData dev-insurancelake-etl-value-lookup resources/syntheticgeneral_lookup_data.json
    ```
1. Transfer the sample claim data to the Collect bucket
    ```bash
    aws s3 cp resources/syntheticgeneral-claim-data.csv s3://<Collect S3 Bucket>/SyntheticGeneralData/ClaimData/
    ```
1. Transfer the sample policy data to the Collect bucket
    ```bash
    aws s3 cp resources/syntheticgeneral-policy-data.csv s3://<Collect S3 Bucket>/SyntheticGeneralData/PolicyData/
    ```
1. Open [Step Functions](https://console.aws.amazon.com/states/home) in the AWS Console and select `dev-insurancelake-etl-state-machine`
    ![AWS Step Functions Selecting State Machine](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/step_functions_select_state_machine.png)
1. Open the state machine execution in progress and monitor the status until completed
    ![AWS Step Functions Selecting Running Execution](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/step_functions_select_running_execution.png)
1. Open [Athena](https://console.aws.amazon.com/athena/home) in the AWS Console
1. Select Launch Query Editor, and change the Workgroup to `insurancelake`
1. Run the following query to view a sample of prepared data in the consume bucket:
    ```sql
    select * from syntheticgeneraldata_consume.policydata limit 100
    ```

### Next Steps

* Take the [InsuranceLake Deep Dive Workshop](https://catalog.us-east-1.prod.workshops.aws/workshops/0a85653e-07e9-41a8-960a-2d1bb592331b)
    * You may skip to the [Modify and test a transform](https://catalog.us-east-1.prod.workshops.aws/workshops/0a85653e-07e9-41a8-960a-2d1bb592331b/en-US/modify-a-transform) step, as the prior steps overlap with the Quickstart instructions
* Try out [loading your own data](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/loading_data.md#landing-source-data)
* Try the [Quickstart with CI/CD](#quickstart-with-cicd)
* Dive deeper with the included [user documentation](#contents)
* Contact your AWS account team for a solution deep dive, workshops, or Professional Services support

## Quickstart with CI/CD

If you've determined the AWS CDK InsuranceLake is a good starting point for your own InsuranceLake, and would like to rapidly iterate through development cycles with one or more teams, we recommend deploying with a CI/CD pipeline. Follow this guide to create your CodePipeline stack and to use it to deploy the InsuranceLake resources:

1. If this is your first time using the application, follow the [Python/CDK Basics](#pythoncdk-basics) steps
1. Use a terminal or command prompt and change the working directory to the location of the infrastruture code
    ```bash
    cd aws-insurancelake-infrastructure
    ```
1. In `lib/configuration.py`, review the `local_mapping` structure in the `get_local_configuration()` function
    - Specifically, the regions and account IDs should make sense for your environments. These values, in the repository (not locally), will be used by CodeCommit and need to be maintained in the repository.
    - The values for the Test and Production environments can be ommitted at this time, because we will only be deploying the Deployment and Development environments.
    - We want to explicitly specify the account and region for each deployment environment so that the infrastructure VPCs get 3 Availability Zones (if the region has them available). [Reference](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_ec2.Vpc.html#maxazs)
1. Deploy CodeCommit repository stack
    ```bash
    cdk deploy Deploy-InsuranceLakeInfrastructureMirrorRepository
    ```
    - While this stack is designed for a mirror repository, it can also be used as a main repository for your InsuranceLake code. You can follow links to help setup other repository types here:
        - [Github](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs/developer_guide.md##aws-codepipeline-and-github-integration)
        - [Bitbucket](https://complereinfosystem.com/2021/02/26/atlassian-bitbucket-to-aws-codecommit-using-bitbucket-pipelines/)
        - [Gitlab](https://klika-tech.com/blog/2022/07/12/repository-mirroring-gitlab-to-codecommit/)
1. If you plan to use CodeCommit as the main repository, [install the Git CodeCommit Helper](https://docs.aws.amazon.com/codecommit/latest/userguide/setting-up-git-remote-codecommit.html):
    ```bash
    sudo pip install git-remote-codecommit
    ```
1. Initialize git, create a develop branch, perform initial commit, and push to remote
    - We are using the develop branch because the Dev environment deployment is triggered by commits to the develop branch.
    - Edit the repository URL to correspond to your version control system if you are not using CodeCommit
    ```bash
    git init
    git branch -M develop
    git add .
    git commit -m 'Initial commit'
    git remote add origin codecommit::us-east-2://aws-insurancelake-infrastructure
    git push --set-upstream origin develop
    ```
1. Deploy Infrastructure CodePipeline resource in the development environment (1 stack)
    ```bash
    cdk deploy Dev-InsuranceLakeInfrastructurePipeline
    ```
1. Review and accept IAM credential creation for the CodePipeline stack
    - Wait for deployment to finish (approx. 5 mins)
1. Open CodePipeline in the AWS Console and select the `dev-insurancelake-infrastructure-pipeline` Pipeline
    - The first run of the pipeline starts automatically after the Pipeline stack is deployed.
    ![Select Infrastructure CodePipeline](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/codepipeline_infrastructure_select_pipeline.png)
1. Monitor the status of the pipeline until completed
    ![Infrastructure CodePipeline progress](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/codepipeline_infrastructure_monitor_progress.png)
1. Switch the working directory to the location of the etl code
    ```bash/
    cd ../aws-insurancelake-etl
    ```
1. In `lib/configuration.py`, review the `local_mapping` structure in the `get_local_configuration()` function, ensure this matches the Infrastructure configuration, or differs if specifically needed.
1. Deploy CodeCommit repository stack
    ```bash
    cdk deploy Deploy-InsuranceLakeEtlMirrorRepository
    ```
1. Initialize git, create a develop branch, perform initial commit, and push to remote
    - We are using the develop branch because the Dev environment deployment is triggered by commits to the develop branch.
    - Edit the repository URL to correspond to your version control system if you are not using CodeCommit
    ```bash
    git init
    git branch -M develop
    git add .
    git commit -m 'Initial commit'
    git remote add origin codecommit::us-east-2://aws-insurancelake-etl
    git push --set-upstream origin develop
    ```
1. Deploy ETL CodePipeline resource in the development environment (1 stack)
    ```bash
    cdk deploy Dev-InsuranceLakeEtlPipeline
    ```
1. Review and accept IAM credential creation for the CodePipeline stack
    - Wait for deployment to finish (approx. 5 mins)
1. Open CodePipeline in the AWS Console and select the `dev-insurancelake-etl-pipeline` Pipeline
    - The first run of the pipeline starts automatically after the Pipeline stack is deployed.
   ![Select ETL CodePipeline](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/codepipeline_etl_select_pipeline.png)
1. Monitor the status of the pipeline until completed
    ![ETL CodePipeline progress](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/codepipeline_etl_monitor_progress.png)

## Deployment Validation

1. Transfer the sample claim data to the Collect bucket (Source system: SyntheticData, Table: ClaimData)
   ```bash
   aws s3 cp resources/syntheticgeneral-claim-data.csv s3://<Collect S3 Bucket>/SyntheticGeneralData/ClaimData/
   ```

1. Transfer the sample policy data to the Collect bucket (Source system: SyntheticData, Table: PolicyData)
   ```bash
   aws s3 cp resources/syntheticgeneral-policy-data.csv s3://<Collect S3 Bucket>/SyntheticGeneralData/PolicyData/
   ```

1. Upon successful load of file S3 event notification will trigger the state-machine-trigger Lambda function

1. This Lambda function will insert a record into the DynamoDB table `{environment}-{resource_name_prefix}-etl-job-audit` to track job start status

1. The Lambda function will also trigger the Step Functions State Machine. The State Machine execution name will be `<filename>-<YYYYMMDDHHMMSSxxxxxx>` and have the required metadata as input parameters

1. The State Machine will trigger the Glue job for Collect to Cleanse data processing

1. The Collect to Cleanse Glue job will execute the transformation logic defined in configuration files

1. Glue job will load the data into the Cleanse bucket using the provided metadata and data will be stored in S3 as `s3://{environment}-{resource_name_prefix}-{account}-{region}-cleanse/syntheticgeneraldata/claimdata/year=YYYY/month=MM/day=DD` in Apache Parquet format

1. Glue job will create/update the Glue Catalog table using the table name passed as parameter based on folder name (`PolicyData` and `ClaimData`)

1. After the Collect to Cleanse job completes, the State Machine will trigger the Cleanse to Consume Glue job

1. The Cleanse to Consume Glue job will execute the SQL logic defined in configuration files

1. The Cleanse to Consume Glue job will store the resulting data set in S3 as `s3://{environment}-{resource_name_prefix}-{account}-{region}-consume/syntheticgeneraldata/claimdata/year=YYYY/month=MM/day=DD` in Apache Parquet format

1. The Cleanse to Consume Glue job will create/update the Glue Catalog table

1. After successful completion of the Cleanse to Consume Glue job, the State Machine will trigger the etl-job-auditor Lambda function to update the DynamoDB table `{environment}-{resource_name_prefix}-etl-job-audit` with the latest status

1. An SNS notification will be sent to all subscribed users

1. To validate the data, use the Athena and execute the following query:

    ```sql
    select * from syntheticgeneraldata_consume.policydata limit 100
    ```

## Cleanup

Refer to the [CDK Instructions, Cleanup section](docs/cdk_instructions.md#clean-up-workflow-created-resources).

## Architecture

In this section we talk about the overall InsuranceLake architecture and the ETL component.

### InsuranceLake 3 Cs

As shown in the figure below, we use Amazon S3 for storage. We use three S3 buckets:
    1. Collect bucket to store raw data in its original format
    1. Cleanse/Curate bucket to store the data that meets the quality and consistency requirements of the lake
    1. Consume bucket for data that is used by analysts and data consumers of the lake (for example, Amazon Quicksight, Amazon Sagemaker)

InsuranceLake is designed to support a number of source systems with different file formats and data partitions. To demonstrate, we have provided a CSV parser and sample data files for a source system with two data tables, which are uploaded to the Collect bucket.

We use AWS Lambda and AWS Step Functions for orchestration and scheduling of ETL workloads. We then use AWS Glue with pySpark for ETL and data cataloging, Amazon DynamoDB for transformation persistence, Amazon Athena for interactive queries and analysis. We use various AWS services for logging, monitoring, security, authentication, authorization, notification, build, and deployment.

**Note:** [AWS Lake Formation](https://aws.amazon.com/lake-formation/) is a service that makes it easy to set up a secure data lake in days. [Amazon QuickSight](https://aws.amazon.com/quicksight/) is a scalable, serverless, embeddable, machine learning-powered business intelligence (BI) service built for the cloud. [Amazon DataZone](https://aws.amazon.com/datazone/) is a data management service that makes it faster and easier for customers to catalog, discover, share, and govern data stored across AWS, on premises, and third-party sources. These three services are not used in this solution but can be added.

![Conceptual Data Lake](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/Aws-cdk-insurancelake-data_lake.png)

### ETL

The figure below represents the ETL resources we provision for the data lake.

1. A file server uploads files to S3 collect bucket of InsuranceLake; file server is a data producer/source for the data lake
2. Amazon S3 triggers an ObjectCreated event notification to AWS Lambda Function
3. AWS Lambda function inserts job information in DynamoDB table
4. AWS Lambda function starts an execution of AWS Step Functions State machine
5. Runs the first Glue job: initiates data processing from Collect to Cleanse
6. Glue job: Spark Glue job will process the data from Collect to Cleanse; source data is assumed to be in CSV format and will be converted to Parquet format
7. DynamoDB: Glue job tokenization will store original values, and lookup tables reside in database
8. After creating Parquet data, update the Glue Data Catalog table
9. Runs the second Glue job: initiates data processing from Cleanse to Consume
10. Glue job: Cleanse to Consume fetches data transformation rules from Glue scripts bucket, and runs transformations
11. Stores the result in Parquet format in Consume bucket
12. Glue job updates the Data Catalog table
13. Updates DynamoDB table with job status
14. Sends SNS notification
15. Data engineers or analysts analyze data using Amazon Athena

![Data Lake Infrastructure Architecture](https://raw.githubusercontent.com/aws-solutions-library-samples/aws-insurancelake-etl/main/docs/Aws-cdk-insurancelake-etl.png)

---

## Security

### Infrastructure Code

InsuranceLake uses [CDK-nag](https://github.com/cdklabs/cdk-nag) to ensure AWS resource security recommendations are followed. CDK-nag can generate warnings, which may need to be fixed depending on the context, and errors, which will interrupt the stack synthesis and prevent any deployment.

To force synthesis of all stacks (including the CodePipeline deployed stacks), which will check all code and generate all reports, use the following command:

```bash
cdk synth '**'
```

When this operation is complete, you will also have access to the CDK-nag reports in CSV format in the `cdk.out` directory and assembly directories.

By default the [AWS Solutions Rules Pack](https://github.com/cdklabs/cdk-nag/blob/main/RULES.md#aws-solutions) is used, but any combination of CDK Nag Rules packs can be selected by adjusting the source code **in four locations** (two for both the Infrastructure and ETL codebases):

[Infrastructure app.py Line 21](https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure/blob/main/app.py#L21), [ETL app.py Line 20](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/app.py#L20):

```python
# Enable CDK Nag for the Mirror repository, Pipeline, and related stacks
# Environment stacks must be enabled on the Stage resource
cdk.Aspects.of(app).add(AwsSolutionsChecks())
```

[Infrastructure pipeline_stack.py Line 148](https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure/blob/main/lib/pipeline_stack.py#L148), [ETL pipeline_stack.py Line 147](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/pipeline_stack.py#L147)
```python
        # Enable CDK Nag for environment stacks before adding to
        # pipeline, which are deployed with CodePipeline
        cdk.Aspects.of(pipeline_deploy_stage).add(AwsSolutionsChecks())
```

### Application Code

InsuranceLake uses [Bandit](https://bandit.readthedocs.io/en/latest) and [Amazon CodeGuru](https://docs.aws.amazon.com/codeguru/latest/reviewer-ug/welcome.html) for static code analysis of all helper scripts, Lambda functions, and PySpark Glue Jobs.

To configure CodeGuru Code Reviews, follow the [AWS Documentation on creating Code Reviews](https://docs.aws.amazon.com/codeguru/latest/reviewer-ug/create-code-reviews.html).

To scan all application code using bandit, use the following command:

```bash
bandit -r --ini .bandit
```

When this operation is complete, you will also have access to the CDK-nag reports in CSV format in the `cdk.out` directory and assembly directories.

## Additional Resources

- [InsuranceLake Quickstart AWS Workshop](https://catalog.workshops.aws/insurancelake)
- [InsuranceLake Deep Dive AWS Workshop](https://catalog.us-east-1.prod.workshops.aws/workshops/0a85653e-07e9-41a8-960a-2d1bb592331b)
- [General Insurance dashboard](https://democentral.learnquicksight.online/#Dashboard-DashboardDemo-General-Insurance) on Quicksight's DemoCentral using Consume-ready-data
- [Life Insurance dashboard](https://democentral.learnquicksight.online/#Dashboard-DashboardDemo-Life-Insurance) also on Quicksight's DemoCentral

## Authors

The following people are involved in the design, architecture, development, testing, and review of this solution:

1. **Cory Visi**, Senior Solutions Architect, Amazon Web Services
1. **Ratnadeep Bardhan Roy**, Senior Solutions Architect, Amazon Web Services
1. **Isaiah Grant**, Cloud Consultant, 2nd Watch, Inc.
1. **Muhammad Zahid Ali**, Data Architect, Amazon Web Services
1. **Ravi Itha**, Senior Data Architect, Amazon Web Services
1. **Justiono Putro**, Cloud Infrastructure Architect, Amazon Web Services
1. **Mike Apted**, Principal Solutions Architect, Amazon Web Services
1. **Nikunj Vaidya**, Senior DevOps Specialist, Amazon Web Services

## License Summary

This sample code is made available under the MIT-0 license. See the LICENSE file.

Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.