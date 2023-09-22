# InsuranceLake ETL with CDK Pipeline

The Insurance Lake solution is comprised of two codebases: [Infrastructure](https://github.com/aws-samples/aws-insurancelake-infrastructure) and [ETL](./). This codebase and the documentation that follows is specific to the ETL. For documentation specific to the Infrastructure, refer to the [InsuranceLake Infrastructure with CDK Pipeline README](https://github.com/aws-samples/aws-insurancelake-infrastructure/blob/main/README.md).

This solution helps you deploy ETL processes and data storage resources to create an Insurance Lake. It uses Amazon S3 buckets for storage, [AWS Glue](https://docs.aws.amazon.com/glue/) for data transformation, and [AWS CDK Pipelines](https://docs.aws.amazon.com/cdk/latest/guide/cdk_pipeline.html). The solution is originally based on the AWS blog [Deploy data lake ETL jobs using CDK Pipelines](https://aws.amazon.com/blogs/devops/deploying-data-lake-etl-jobs-using-cdk-pipelines/).

[CDK Pipelines](https://docs.aws.amazon.com/cdk/api/latest/docs/pipelines-readme.html) is a construct library module for painless continuous delivery of CDK applications. CDK stands for Cloud Development Kit. It is an open source software development framework to define your cloud application resources using familiar programming languages.

Specifically, this solution helps you to:

1. Deploy a 3 Cs (Collect, Cleanse, Consume) Insurance Lake
1. Deploy ETL jobs needed make common insurance industry data souces available in a data lake
1. Use pySpark Glue jobs and supporting resoures to perform data transforms in a modular approach
1. Build and replicate the application in multiple environments quickly
1. Deploy ETL jobs from a central deployment account to multiple AWS environments such as Dev, Test, and Prod
1. Leverage the benefit of self-mutating feature of CDK Pipelines; specifically, the pipeline itself is infrastructure as code and can be changed as part of the deployment
1. Increase the speed of prototyping, testing, and deployment of new ETL jobs

---

## Contents

* [Quickstart](#quickstart)
   * [Python/CDK Basics](#pythoncdk-basics)
   * [Deploy the Application](#deploy-the-application)
   * [Try out the ETL Process](#try-out-the-etl-process)
* [Quickstart with CI/CD](#quickstart-with-cicd)
* [Architecture](#architecture)
   * [Insurance Lake](#insurance-lake)
   * [ETL](#etl)
* [Pipeline Usage](#pipeline-usage)
   * [Bucket Layout](#bucket-layout)
   * [Transformation Modules](#transformation-modules)
* [Codebase](#codebase)
   * [Source Code Structure](#source-code-structure)
   * [Unit Testing](#unit-testing)
   * [Integration Testing](#integration-testing)
* [Additional resources](#additional-resources)
* [Authors](#authors)
* [License Summary](#license-summary)

## Quickstart

If you'd like to get started quickly transforming some sample raw insurance data and running SQL on the resulting dataset, and without worrying about CI/CD, follow this guide:

### Python/CDK Basics

Skip steps in this section as needed if you've worked with CDK and Python before.

1. Install [Python](https://www.python.org/downloads/) on your local computer
1. Install [Node.js](https://nodejs.org/en/download/package-manager/) on your local computer
   - CDK uses Node.js under the hood; the code will be in Python for this application
1. Install [CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) on your local computer
   ```bash
   sudo npm install -g aws-cdk
   ```
1. Clone the repository or unpack the source code from the distribution ZIP file
1. Use a terminal or command prompt and change the working directory to the location of the infrastructure code
   ```bash
   cd aws-cdk-insurancelake-infrastructure
   ```
1. Create a virtual environment
   ```bash
   python3 -m venv .venv
   ```
1. Activate the virtual environment
   ```bash
   source .venv/bin/activate
   ```
1. Install required Python libraries
   ```bash
   pip install -r requirements.txt
   ```
1. Setup your AWS account credentials in the terminal or command prompt environment
   ```bash
   export AWS_ACCESS_KEY_ID=<id here>
   export AWS_ACCESS_ACCESS_KEY=<key here>
   ```
1. Bootstrap CDK in your AWS account
   - By default the solution will deploy resources to the `us-east-2` region
   ```bash
   cdk bootstrap
   ```

### Deploy the Application

1. Use a terminal or command prompt and change the working directory to the location of the infrastruture code
   ```bash
   cd aws-cdk-insurancelake-infrastructure
   ```
1. Deploy infrastructure resources in the development environment (2 stacks)
   ```bash
   cdk deploy Dev-InsuranceLakeInfrastructurePipeline/Dev/InsuranceLakeInfrastructureS3BucketZones
   ```
1. Review and accept IAM credential creation for the S3 bucket stack
   - Wait for deployment to finish (approx. 5 mins)
1. Copy the S3 bucket name for the Collect bucket to use later
   - Bucket name will be in the form: `dev-insurancelake-<AWS Account ID>-<Region>-collect`
1. Switch the working directory to the location of the etl code
   ```bash
   cd ../aws-cdk-insurancelake-etl
   ```
1. Deploy the ETL resources in the development environment (3 stacks)
   ```bash
   cdk deploy Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlDynamoDb Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlGlue Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlStepFunctions Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeAthenaHelper
   ```
   - Wait for DynamoDB deployment to finish (approx. 1 min)
1. Review and accept IAM credential creation for the Glue jobs stack
   - Wait for deployment to finish (approx. 3 mins)
1. Review and accept IAM credential creation for the Step Functions stack
   - Wait for deployment of Step Functions and Athena Helper stacks to finish (approx. 7 mins)

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
1. Open Step Functions in the AWS Console and select `dev-insurancelake-etl-state-machine`
   ![AWS Step Functions Selecting State Machine](./resources/step_functions_select_state_machine.png)
1. Open the state machine execution in progress and monitor the status until completed
   ![AWS Step Functions Selecting Runing Execution](./resources/step_functions_select_running_execution.png)
1. Using Athena in the AWS Console select the Query Editor, and the `insurancelake` Workgroup
1. Run the following query:
   ```sql
   select * from syntheticgeneraldata_consume.policydata limit 100
   ```

## Quickstart with CI/CD

If you've determined the AWS CDK Insurance Lake is a good starting point for your own Insurance Lake, and would like to rapidly iterate through development cycles with one or more teams, we recommend deploying with a CI/CD pipeline. Follow this guide to create your CodePipeline stack and to use it to deploy the Insurance Lake resources:

1. If this is your first time using the application, follow the [Python/CDK Basics](#pythoncdk-basics) steps
1. Use a terminal or command prompt and change the working directory to the location of the infrastruture code
   ```bash
   cd aws-cdk-insurancelake-infrastructure
   ```
1. In `lib/configuration.py`, review the `local_mapping` structure in the `get_local_configuration()` function
   - Specifically, the regions and account IDs should make sense for your environments. These values, in the repository (not locally), will be used by CodeCommit and need to be maintained in the repository.
   - The values for the Test and Production environments can be ommitted at this time, because we will only be deploying the Deployment and Development environments.
   - We want to explicitly specify the account and region for each deployment environment so that the infrastructure VPCs get 3 Availability Zones (if the region has them available). [Reference](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_ec2.Vpc.html#maxazs)
1. Deploy CodeCommit repository stack
   ```bash
   cdk deploy Deploy-InsuranceLakeInfrastructureMirrorRepository
   ```
   - While this stack is designed for a mirror repository, it can also be used as a main repository for your Insurance Lake code. You can follow links to help setup other repository types here:
      - [Github](resources/github_guide.md)
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
   git remote add origin codecommit::us-east-2://aws-cdk-insurancelake-infrastructure
   git push --set-upstream origin develop
   ```
1. Deploy Infrastructure CodePipeline resource in the development environment (1 stack)
   ```bash
   cdk deploy DevInsuranceLakeInfrastructurePipeline
   ```
1. Review and accept IAM credential creation for the CodePipeline stack
   - Wait for deployment to finish (approx. 5 mins)
1. Open CodePipeline in the AWS Console and select the `dev-insurancelake-infrastructure-pipeline` Pipeline
   - The first run of the pipeline starts automatically after the Pipeline stack is deployed.
   ![Select Infrastructure CodePipeline](./resources/codepipeline_infrastructure_select_pipeline.png)
1. Monitor the status of the pipeline until completed
   ![Infrastructure CodePipeline progress](./resources/codepipeline_infrastructure_monitor_progress.png)
1. Switch the working directory to the location of the etl code
   ```bash
   cd ../aws-cdk-insurancelake-etl
   ```
1. In `lib/configuration.py`, review the `local_mapping` structure in the `get_local_configuration()` function, ensure this matches the Infrastructure configuration, or differs if specifically needed.
1. Deploy CodeCommit repository stack
   ```bash
   cdk deploy DeploymentInsuranceLakeEtlMirrorRepository
   ```
1. Initialize git, create a develop branch, perform initial commit, and push to remote
   - We are using the develop branch because the Dev environment deployment is triggered by commits to the develop branch.
   - Edit the repository URL to correspond to your version control system if you are not using CodeCommit
   ```bash
   git init
   git branch -M develop
   git add .
   git commit -m 'Initial commit'
   git remote add origin codecommit::us-east-2://aws-cdk-insurancelake-etl
   git push --set-upstream origin develop
   ```
1. Deploy ETL CodePipeline resource in the development environment (1 stack)
   ```bash
   cdk deploy DevInsuranceLakeEtlPipeline
   ```
1. Review and accept IAM credential creation for the CodePipeline stack
   - Wait for deployment to finish (approx. 5 mins)
1. Open CodePipeline in the AWS Console and select the `dev-insurancelake-etl-pipeline` Pipeline
   - The first run of the pipeline starts automatically after the Pipeline stack is deployed.
   ![Select ETL CodePipeline](./resources/codepipeline_etl_select_pipeline.png)
1. Monitor the status of the pipeline until completed
   ![ETL CodePipeline progress](./resources/codepipeline_etl_monitor_progress.png)

## Architecture

In this section we talk about the overall Insurance Lake architecture and the ETL component.

### InsuranceLake 3 Cs

As shown in the figure below, we use Amazon S3 for storage. We use three S3 buckets:
   1. Collect bucket to store raw data in its original format
   1. Cleanse/Curate bucket to store the data that meets the quality and consistency requirements of the lake
   1. Consume bucket for data that is used by analysts and data consumers of the lake (e.g. Amazon Quicksight, Amazon Sagemaker)

The Insurance Lake is designed to support a number of source systems with different file formats and data partitions. To demonstrate, we have provided a CSV parser and sample data files for a source system with two data tables, which are uploaded to the Collect bucket.

We use AWS Lambda and AWS Step Functions for orchestration and scheduling of ETL workloads. We then use AWS Glue with pySpark for ETL and data cataloging, Amazon DynamoDB for transformation persistence, Amazon Athena for interactive queries and analysis. We use various AWS services for logging, monitoring, security, authentication, authorization, notification, build, and deployment.

**Note:** [AWS Lake Formation](https://aws.amazon.com/lake-formation/) is a service that makes it easy to set up a secure data lake in days. [Amazon QuickSight](https://aws.amazon.com/quicksight/) is a scalable, serverless, embeddable, machine learning-powered business intelligence (BI) service built for the cloud. These two services are not used in this solution but can be added.

![Conceptual Data Lake](./resources/Aws-cdk-insurancelake-data_lake.png)

---

### ETL

The figure below represents the ETL resources we provision for Data Lake.

1. A file server uploads files to S3 collect bucket of the insurance lake; file server is a data producer/source for the data lake
2. Amazon S3 triggers an event notification to AWS Lambda Function
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

![Data Lake Infrastructure Architecture](./resources/Aws-cdk-insurancelake-etl.png)

---

## Pipeline Usage

### Bucket Layout

In order to allow transform specifications to be matched with source system data and organized in groups, each of the three ETL stage buckets (Collect, Cleanse, Consume) have similar directory layouts. The first level represents the source system name or the database that will group the underlying tables. The second layer represents the data set or table containing the uploaded data. In the Collect bucket, the source files are stored at the second layer. In the Cleanse bucket, data is converted to parquet files and stored in partitions at the second layer. In the Consume bucket database and table names may change if data is merged.

![Bucket Layout Example](./resources/bucket-layout-example.png)

### Transformation Modules

  | File / Folder    | Description  |
  |------------------| -------------|
  | [datatransform_lookup](lib/glue_scripts/lib/datatransform_lookup.py) | pySpark logic to perform column value lookup operations |
  | [datatransform_typeconversion](lib/glue_scripts/lib/datatransform_typeconversion.py) | pySpark logic to convert date columns, and other data types to standard format |
  | [datatransform_dataprotection](lib/glue_scripts/lib/datatransform_dataprotection.py) | pySpark logic to redact, hash, and tokenize sensitive data columns |
  | [datatransform_regex](lib/glue_scripts/lib/datatransform_regex.py) | pySpark logic to regex, and Python formatting string operations on data |
| [datatransform_premium](lib/glue_scripts/lib/datatransform_premium.py) | pySpark logic to perform common insurance industry data transforms |
  | [custom_mapping](lib/glue_scripts/lib/custom_mapping.py) | pySpark logic to rename columns according to a map file |
  | [dataquality_checks](lib/glue_scripts/lib/dataquality_checks.py) | Glue logic to run Data Quality rules according to a rules file |
  | [datalineage](lib/glue_scripts/lib/datalineage.py) | Custom data lineage tracking class designed to work with InsuranceLake transforms |

---

## Codebase

### Source Code Structure

- _TODO: Include more folders, scripts, modules, supporting files_

Table below explains how this source ode structured:

  | File / Folder    | Description  |
  |------------------| -------------|
  | [app.py](app.py) | Application entry point |
  | [code_commit_stack](./lib/code_commit_stack.py) | Optional stack to deploy an empty CodeCommit respository for mirroring |
  | [dynamodb_stack](lib/dynamodb_stack.py) | Stack creates DynamoDB Tables for Job Auditing and ETL transformation rules. |
  | [glue_stack](lib/glue_stack.py) | Stack creates Glue Jobs and supporting resources such as Connections, S3 Buckets - script and temporary - and an IAM execution Role |
  | [pipeline_stack](lib/pipeline_stack.py) | Pipeline stack entry point |
  | [pipeline_deploy_stage](lib/pipeline_deploy_stage.py) | Pipeline deploy stage entry point |
  | [step_functions_stack](lib/step_functions_stack.py) | Stack creates an ETL State machine which invokes Glue Jobs and supporting Lambdas - state machine trigger and status notification. |
  | [athena_helper_stack](lib/athena_helper_stack.py) | Stack creates an Athena workgroup with query results bucket ready for demonstration SQL queries. |
  | [Glue Scripts](lib/glue_scripts/etl_collect_to_cleanse.py) | Glue pySpark job data processing logic for Collect bucket data, which stores results in the Cleanse bucket |
  | [Glue Scripts](lib/glue_scripts/etl_cleanse_to_consume.py) | Glue pySpark job data processing logic for Cleanse bucket data, which stores results in the Consume bucket |
  | [Glue Scripts](lib/glue_scripts/lib/) | Glue pySpark job modules that will be automatically available to all Glue jobs |
  | [ETL Job Auditor](lib/etl_job_auditor/lambda_handler.py) | Lambda script to update DynamoDB in case of glue job success or failure |
  | [ETL Trigger](lib/state_machine_trigger/lambda_handler.py) | Lambda script to trigger step function and initiate DynamoDB |
  | [ETL Transformation Mapping and Specification](lib/glue_scripts/transformation-spec/) | Field mapping and transformation specification logic to be used for data processing from Collect to Cleanse |
  | [ETL Transformation SQL](lib/glue_scripts/transformation-sql/) | Transformation SQL logic to be used for data processing from Cleanse to Consume |
  | [ETL Data Quality Rules](lib/glue_scripts/dq-rules/) | Glue Data Quality rules for quality checks from Cleanse to Consume |
  | [test](./test)| This folder contains pytest unit tests |
  | [Resources](./resources) | This folder has architecture, process flow diagrams, and additional documentation |

---

### Unit Testing

The Python CDK unit tests use pytest, which will be installed as part of the solution requirements.
The pySpark Glue Job and Python Lambda function unit tests are still under development.

Run tests with the following command (`--cov` will include a code coverage report):
```bash
python -m pytest --cov
```

Note that without a AWS Glue Docker container, the Glue job tests will be skipped with a message like:
```
test/test_custom_mapping.py::test_custommapping_renames_field SKIPPED (No pySpark environment found)                                    [ 17%]
```

To setup your local environment with a Glue container, retrieve the container image from [the AWS Glue Dockerhub repository](https://hub.docker.com/r/amazon/aws-glue-libs/tags). Ensure you use the right tag for the version of AWS Glue used in the stack (currently v4). Detailed instructions can be found on [Developing AWS Glue ETL jobs locally using a container](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container)

---

### Integration Testing

- _TODO: Automate manual steps_

1. Transfer the sample claim data to the Collect bucket (Source system: SyntheticData, Table: ClaimData)
   ```bash
   aws s3 cp resources/syntheticgeneral-claim-data.csv s3://<Collect S3 Bucket>/SyntheticGeneralData/ClaimData/
   ```

1. Transfer the sample policy data to the Collect bucket (Source system: SyntheticData, Table: PolicyData)
   ```bash
   aws s3 cp resources/syntheticgeneral-policy-data.csv s3://<Collect S3 Bucket>/SyntheticGeneralData/PolicyData/
   ```

1. Upon successful load of file S3 event notification will trigger the lambda

1. Lambda will insert record into the dynamodb table `{environment}-{resource_name_prefix}-etl-job-audit` to track job start status

1. Lambda function will trigger the step function. Step function name will be `<filename>-<YYYYMMDDHHMMSSxxxxxx>` and provided the required metadata input

1. Step functions state machine will trigger the Glue job for Collect to Cleanse data processing.

1. Glue job will load the data into conformed bucket using the provided metadata and data will be loaded to `s3://{environment}-{resource_name_prefix}-{account}-{region}-cleanse/syntheticgeneraldata/claimdata/year=YYYY/month=MM/day=DD` in parquet format

1. Glue job will create/update the catalog table using the tablename passed as parameter based on folder name `claimdata`

1. After Collect to Cleanse job completion, Cleanse to Consume Glue job will get triggered in step function

1. Cleanse to Consume Glue glue job will use the transformation logic being provided in Dynamodb as part of prerequisites for data transformation

1. Cleanse to Consume Glue job will store the result set in S3 bucket under `s3://{environment}-{resource_name_prefix}-{account}-{region}-consume/syntheticgeneraldata/claimdata/year=YYYY/month=MM/day=DD`

1. Cleanse to Consume Glue job will create/update the catalog table

1. After completion of Glue job, Lambda will get triggered in step function to update the Dynamodb table `{environment}-{resource_name_prefix}-etl-job-audit` with latest status

1. SNS notification will be sent to the subscribed users

1. To validate the data, please open Athena service and execute query:

   ```sql
   select * from syntheticgeneraldata_consume.policydata limit 100
   ```

---

## Additional Resources

In this section, we provide some additional resources.

- [AWS CDK Detailed Insutructions](./resources/cdk_instructions.md)
- [Developer Guide](resources/developer_guide.md) for more information on working with this solution
- [Full Deployment Guide](./resources/full_deployment_guide.md) for deploying to 3 environments from 3 code branches
- [Github / CodePipeline Integration Guide](./resources/github_guide.md)
- [Develop and test AWS Glue version 3.0 and 4.0 jobs locally using a Docker container](https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container)
- [General Insurance dashboard](https://democentral.learnquicksight.online/#Dashboard-DashboardDemo-General-Insurance) on Quicksight's DemoCentral using Consume-ready-data

---

## Authors

The following people are involved in the design, architecture, development, testing, and review of this solution:

1. **Cory Visi**, Senior Solutions Architect, Amazon Web Services
1. **Ratnadeep Bardhan Roy**, Senioar Solutions Architect, Amazon Web Services
1. **Isaiah Grant**, Cloud Consultant, 2nd Watch, Inc.
1. **Muhammad Zahid Ali**, Data Architect, Amazon Web Services
1. **Ravi Itha**, Senior Data Architect, Amazon Web Services
1. **Justiono Putro**, Cloud Infrastructure Architect, Amazon Web Services
1. **Mike Apted**, Principal Solutions Architect, Amazon Web Services
1. **Nikunj Vaidya**, Senior DevOps Specialist, Amazon Web Services

---

## License Summary

This sample code is made available under the MIT-0 license. See the LICENSE file.

Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.