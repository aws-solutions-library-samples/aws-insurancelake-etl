---
title: Developer Guide
parent: Developer Documentation
nav_order: 1
last_modified_date: 2025-04-23
---
# InsuranceLake Developer Guide
{: .no_toc }

This section provides details for developing with InsuranceLake.


## Contents
{: .no_toc }

* TOC
{:toc}


## Local AWS CDK Deployment

Reference the [AWS CDK Instructions](./cdk_instructions.md) for standard AWS CDK project setup.

Prerequisites include:
* [Python](https://www.python.org/downloads/) (Python 3.9 or later, Python 3.12 recommended)
* [Node.js](https://nodejs.org/en/download/package-manager/) (CDK uses Node.js)

## Local AWS Glue and Apache Spark Development

To setup a local AWS Glue and Apache Spark environment for testing, use the AWS-provided Glue Docker container image which can be retrieved from the [AWS Glue Dockerhub repository](https://hub.docker.com/r/amazon/aws-glue-libs/tags). Ensure you use the right tag for the version of AWS Glue used in the stack (currently v5).

For detailed instructions on setting up a local AWS Glue and Apache Spark environment, refer to the following guides:

* [AWS Developer Guide: Developing and testing AWS Glue job scripts locally](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html)
* [Develop and test AWS Glue 5.0 jobs locally using a Docker container](https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-5-0-jobs-locally-using-a-docker-container/)
* [Developing AWS Glue ETL jobs locally using a container](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container) (Older versions of AWS Glue)

{: .note }
AWS Glue Data Quality is not available in the AWS Glue Docker images.

### Local Apache Iceberg Development in Glue v4

{: .note }
AWS Glue v5 *already includes* the libraries to enable local development and testing of Apache Iceberg tables.

Local development and testing of Apache Iceberg tables is possible using the AWS-provided Glue Docker container. To enable this capability, the Docker container needs one or two additional libraries.

1. Spark with Scala runtime Jar
    * Required to run [unit tests](#unit-testing)
1. aws-bundle Jar
    * Required for integration testing or local running and debugging with an AWS session

Both JAR files can be downloaded from the [Apache Iceberg Releases page](https://iceberg.apache.org/releases/#downloads). The last versions known to work with AWS Glue 4.0 are [iceberg-spark-runtime-3.3_2.12-1.5.2.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.5.2/iceberg-spark-runtime-3.3_2.12-1.5.2.jar) and [iceberg-aws-bundle-1.5.2.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar).

To enable these libraries, copy them to the `/home/glue_user/spark/jars/` location in the AWS Glue Docker image.

For more detailed information on Iceberg unit testing, refer to the article [Navigating the Iceberg: unit testing Iceberg tables with PySpark](https://medium.com/datamindedbe/navigating-the-iceberg-unit-testing-iceberg-tables-with-pyspark-4d19de51bf57)

If you attempt to run the unit tests without installing the Iceberg library, you will see the following error:
```log
py4j.protocol.Py4JJavaError: An error occurred while calling o119.create.
: org.apache.spark.SparkException: Cannot find catalog plugin class for catalog 'local': org.apache.iceberg.spark.SparkCatalog
```

### Visual Studio Code

To run and debug the ETL AWS Glue jobs in Visual Studio Code, you'll need a launch configuration defined in `launch.json`. The following configuration provides the required parameters for all three of the InsuranceLake ETL AWS Glue jobs. You can also find this sample configuration in [the `launch.json.default` Visual Studio Code configuration file included in the repository](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/.vscode/launch.json.default). Be sure to replace the AWS Account ID placeholders with the AWS Account ID where InsuranceLake is deployed.

```json
    {
        "name": "Python: Glue pySpark jobs",
        "type": "debugpy",
        "request": "launch",
        "program": "${file}",
        "console": "integratedTerminal",
        "justMyCode": true,
        "args": [
            "--JOB_NAME=VisualStudioCode",
            "--enable-glue-datacatalog=true",
            "--TempDir=s3://dev-insurancelake-ACCOUNT_ID-us-east-2-glue-temp/etl/collect-to-cleanse",
            "--enable-metrics=true",
            "--enable-spark-ui=true",
            "--spark-event-logs-path=s3://dev-insurancelake-ACCOUNT_ID-us-east-2-glue-temp/etl/collect-to-cleanse/",
            "--enable-job-insights=true",
            "--enable-continuous-cloudwatch-log=true",
            "--state_machine_name=dev-insurancelake-etl-state-machine",
            "--execution_id=test_execution_id",
            "--environment=Dev",
            "--source_bucket=s3://dev-insurancelake-ACCOUNT_ID-us-east-2-collect",
            "--source_key=SyntheticGeneralData/PolicyData",
            "--source_path=SyntheticGeneralData/PolicyData",
            "--target_database_name=SyntheticGeneralData",
            "--target_bucket=s3://dev-insurancelake-ACCOUNT_ID-us-east-2-cleanse",
            "--base_file_name=syntheticgeneral-policy-data.csv",
            "--p_year=2024",
            "--p_month=07",
            "--p_day=15",
            "--table_name=PolicyData",
            "--txn_bucket=s3://dev-insurancelake-ACCOUNT_ID-us-east-2-etl-scripts",
            "--txn_spec_prefix_path=/etl/transformation-spec/",
            "--txn_sql_prefix_path=/etl/transformation-sql/",
            "--hash_value_table=dev-insurancelake-etl-hash-values",
            "--value_lookup_table=dev-insurancelake-etl-value-lookup",
            "--multi_lookup_table=dev-insurancelake-etl-multi-lookup",
            "--dq_results_table=dev-insurancelake-etl-dq-results",
            "--data_lineage_table=dev-insurancelake-etl-data-lineage",
            "--additional-python-modules=rapidfuzz",
            "--iceberg_catalog=glue_catalog"
        ],
        "env": {
            "AWS_DEFAULT_REGION": "us-east-2",
            "PYDEVD_WARN_EVALUATION_TIMEOUT": "15"
        }
    }
```


## Codebase

### Source Code Structure

The table below explains how this source code is structured.

| File / Folder    | Description
|---    |---
| [app.py](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/app.py) | Application entry point
| [code_commit_stack](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/code_commit_stack.py) | Optional stack to deploy an empty CodeCommit respository for mirroring |
| [pipeline_stack](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/pipeline_stack.py) | CodePipeline stack entry point
| [pipeline_deploy_stage](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/pipeline_deploy_stage.py) | CodePipeline deploy stage entry point
| [dynamodb_stack](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/dynamodb_stack.py) | Stack to create DynamoDB tables for job auditing and ETL transformation rules
| [glue_buckets_stack](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_buckets_stack.py) | Stack to create S3 buckets used by Glue jobs (script and temporary storage)
| [glue_jobs_stack](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_jobs_stack.py) | Stack to create AWS Glue jobs and supporting resources such as AWS Glue Connections and an IAM execution role
| [data_lake_consumer_stack](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/data_lake_consumer_stack.py) | Stack to create IAM policy for data lake consumers
| [step_functions_stack](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/step_functions_stack.py) | Stack to create an ETL Step Function State Machine which invokes AWS Glue jobs and supporting Lambda functions (state machine trigger and status notification)
| [athena_workgroup_stack](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/athena_workgroup_stack.py) | Stack to create an Athena workgroup with query results bucket ready for demonstration SQL queries
| [Collect-to-Cleanse AWS Glue Script](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_collect_to_cleanse.py) | AWS Glue PySpark job data processing logic for Collect bucket data, which stores results in the Cleanse bucket
| [Cleanse-to-Consume AWS Glue Script](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_cleanse_to_consume.py) | AWS Glue PySpark job data processing logic for Cleanse bucket data, which stores results in the Consume bucket
| [Entity Match AWS Glue Script](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_consume_entity_match.py) | AWS Glue PySpark job data processing logic for entity matching, which stores results in the Consume bucket
| [ETL Job Auditor](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/etl_job_auditor/lambda_handler.py) | Lambda function to update DynamoDB in case of AWS Glue job success or failure
| [ETL Trigger](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/state_machine_trigger/lambda_handler.py) | Lambda function to trigger step function and initiate DynamoDB
| [ETL Dependency Trigger](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/dependency_trigger/lambda_handler.py)   | Lambda function to trigger queued Step Functions executions based on SNS topic notifications
| [ETL Transformation Mapping and Specification](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/transformation-spec/) | Field mapping and transformation specification logic to be used for data processing from Collect to Cleanse
| [ETL Transformation SQL](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/transformation-sql/) | Transformation SQL logic to be used for data processing from Cleanse to Consume
| [ETL Data Quality Rules](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/dq-rules/) | AWS Glue Data Quality rules for quality checks from Cleanse to Consume
| [test](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/test)| This folder contains pytest unit tests
| [resources](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/resources) | This folder has sample data and helper scripts
| [docs](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/docs) | This folder contains all the documentation for InsuranceLake, including architecture diagrams, developer documentation, and user documentation

### Transformation Modules

The table below lists the AWS Glue transformation modules.

| File / Folder    | Description
|---    |---
| [datatransform_dataprotection](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_dataprotection.py) | PySpark logic to redact, hash, and tokenize sensitive data columns
| [datatransform_lookup](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_lookup.py) | PySpark logic to perform column value lookup operations
| [datatransform_misc](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_misc.py)  | PySpark logic for miscellaneous data transformation functions, such as filtering rows
| [datatransform_premium](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_premium.py) | PySpark logic to perform common insurance industry data transforms
| [datatransform_stringmanipulation](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_stringmanipulation.py) | PySpark logic to perform regular expression transforms and Python formatting string operations on data
| [datatransform_structureddata](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_structureddata.py)  | PySpark logic to perform operations on nested data structures usually created from JSON files
| [datatransform_typeconversion](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_typeconversion.py) | PySpark logic to convert date columns and other data types to standard format
| [custom_mapping](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/custom_mapping.py) | PySpark logic to rename columns according to a map file
| [dataquality_check](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/dataquality_check.py) | AWS Glue logic to run AWS Glue Data Quality rules according to a rules file
| [datalineage](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datalineage.py) | Custom data lineage tracking class designed to work with InsuranceLake transforms


## Code Quality

### Style Guide for Python Code

AWS InsuranceLake follows [PEP8](https://www.python.org/dev/peps/pep-0008/) enforced through [flake8](https://flake8.pycqa.org/en/latest/) and [pre-commit](https://pre-commit.com/) (instructions are shown below).

To enable pre-commit checks, install and setup the pre-commit library:

```{bash}
pip install pre-commit
pre-commit install
```

The above will create a git hook which will validate code prior to commits using [flake8](https://flake8.pycqa.org/en/latest/). Configuration for standards can be found in:

* [.flake8](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/.flake8)
* [.pre-commit-config.yaml](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/.pre-commit-config.yaml)

### AWS Glue Job/Apache Spark Code

Due to their complexity, InsuranceLake AWS Glue jobs are not editable in the AWS Glue Visual Editor. AWS Glue job development and testing is best done in [AWS Glue Notebooks](https://docs.aws.amazon.com/glue/latest/dg/notebook-getting-started.html) or a local Python-enabled integrated development environment (IDE) in am AWS Glue Docker container. Documentation on setting up an AWS Glue Docker container can be found in the [Local AWS Glue and Apache Spark Development](#local-aws-glue-and-apache-spark-development) section.

To more quickly diagnose issues with features not available in the AWS Glue Docker container, or when working with large datasets that may be too slow to process locally, bypassing the Step Functions workflow to execute specific AWS Glue jobs can be helpful. The following are example executions for each of the three InsuranceLake ETL AWS Glue jobs with the minimum required parameters:

```bash
aws glue start-job-run --job-name dev-insurancelake-cleanse-to-consume-job --arguments '
{
    "--execution_id": "manual_execution_identifier",
    "--source_bucketname": "dev-insurancelake-<Account ID>-us-east-2-glue-temp",
    "--source_key": "MyDB/MyTable",
    "--base_file_name": "input_file.csv",
    "--database_name_prefix": "MyDB",
    "--table_name": "MyTable",
    "--p_year": "2024",
    "--p_month": "01",
    "--p_day": "01",
    "--data_lineage_table": "dev-insurancelake-etl-data-lineage",
    "--state_machine_name": "dev-insurancelake-etl-state-machine"
}'
```

```bash
aws glue start-job-run --job-name dev-insurancelake-cleanse-to-consume-job --arguments '
{
    "--execution_id": "manual_execution_identifier",
    "--source_bucketname": "dev-insurancelake-<Account ID>-us-east-2-glue-temp",
    "--source_key": "MyDB/MyTable",
    "--base_file_name": "input_file.csv",
    "--database_name_prefix": "MyDB",
    "--table_name": "MyTable",
    "--p_year": "2024",
    "--p_month": "01",
    "--p_day": "01",
    "--data_lineage_table": "dev-insurancelake-etl-data-lineage",
    "--state_machine_name": "dev-insurancelake-etl-state-machine"
}'
```

```bash
aws glue start-job-run --job-name dev-insurancelake-consume-entity-match-job --arguments '
{
    "--execution_id": "manual_execution_identifier",
    "--source_key": "MyDB/MyTable",
    "--database_name_prefix": "MyDB",
    "--table_name": "MyTable",
    "--data_lineage_table": "dev-insurancelake-etl-data-lineage",
    "--state_machine_name": "dev-insurancelake-etl-state-machine",
    "--p_year": "2024",
    "--p_month": "01",
    "--p_day": "01"
}'
```

The following are additional code considerations:

* To include third party Python libraries, use the AWS Glue job parameter `--additional-python_modules` specified in the AWS Glue job definition in the AWS Glue stack. Review the [AWS Glue User Guide](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html#extra-py-files-support) for more information.

* To include your own Python libraries, simply create the additional library in `lib/glue_scripts/lib` and redeploy the AWS Glue stack using AWS CDK. The CDK AWS Glue stack will automatically identify the new library and include it in the `--extra-py-files` parameter for all AWS Glue jobs. Ensure you also import the module in the AWS Glue job script.

* To include Spark JAR libraries, simply copy the JAR file to the `lib/glue_scripts/lib` folder and redeploy the AWS Glue stack using AWS CDK. The CDK AWS Glue stack will automatically identify the new library and include it in the `--extra-jars` parameter for all AWS Glue jobs.

* InsuranceLake AWS Glue jobs follow the AWS best practice of getting the Spark session from the AWS Glue context and calling `job.init()`. `job_commit()` is always called at the end of the script. These functions are used to update the state change to the service.

    InsuranceLake AWS Glue jobs do not make use of [AWS Glue job bookmarks](https://docs.aws.amazon.com/glue/latest/dg/programming-etl-connect-bookmarks.html) because almost all transformation of data is done in Spark DataFrames. AWS Glue job bookmarks are **disabled by default for all InsuranceLake AWS Glue jobs**. For future use, the AWS Glue jobs set the `transformation_ctx` for all DynamicFrame operations.

    A difference between the standard AWS Glue job source code and the InsuranceLake code is that the **job initialization** is contained within `main` and **not in the global Python context**. This is done to facilitate unit testing of the AWS Glue jobs in a local environment using the AWS Glue Docker container.

* The suggested skeleton code to construct a new AWS Glue job in the InsuranceLake pipeline follows:

    ```python
    import sys
    import os
    # Other Python imports

    from pyspark.context import SparkContext
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    # Other pyspark and awsglue imports

    # For running in local Glue container
    sys.path.append(os.path.dirname(__file__) + '/lib')

    from glue_catalog_helpers import upsert_catalog_table, clean_column_names, generate_spec, put_s3_object
    from datalineage import DataLineageGenerator
    # Other local libraries needed from lib/glue_scripts/lib

    # Minimum job arguments (add others here if required)
    expected_arguments = [
        'JOB_NAME',
        'environment',
        'TempDir',
        'state_machine_name',
        'execution_id',
        'source_key',
    ]

    def main():
        # Make a local copy to help unit testing (the global is shared across tests)
        local_expected_arguments = expected_arguments.copy()

        # Handle optional arguments
        for arg in sys.argv:
            if '--data_lineage_table' in arg:
                local_expected_arguments.append('data_lineage_table')

        args = getResolvedOptions(sys.argv, expected_arguments)

        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)

        lineage = DataLineageGenerator(args)

        # Job specific code here

        job.commit()

    if __name__ == '__main__':
        main()
    ```

* The majority of InsuranceLake operations are done using Spark-native DataFrames because conversions to AWS Glue DynamicFrames and Pandas DataFrames come with a cost. InsuranceLake was also designed to be as portable as possible to other Spark environments (with the exception of AWS Glue Data Quality). **We recommend you follow the practice of avoiding DataFrame conversions in your AWS Glue jobs**.

* When there is functionality needed from Pandas that is not available in Spark, there are three methods to consider:
    - [Pandas-on-Spark DataFrame](https://spark.apache.org/docs/latest/api/python/tutorial/pandas_on_spark/pandas_pyspark.html#pyspark)

        Using the `DataFrame.pandas_api()` is performant because the data and operations are distributed. Avoid operations like `DataFrame.to_numpy()` that require the data to be collected on the driver (non-distributed). The Pandas API on Spark does not target 100% compatibility, so you may experience errors running your workflow.

    - [Pandas UDFs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html)
        Pandas user-defined functions (UDFs) are executed by Spark using Apache Arrow to transfer data and Pandas to work with the data. When used in combination with `withColumn` or `select`, a Pandas UDF can perform Pandas library vectorized operations in a distributed manner on individual columns supplied as arguments.

    - [PyArrow for Conversions](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)

        If full conversion to a Pandas DataFrame is needed, ensure your AWS Glue job enables Apache Arrow support for data conversion as follows:

        ```python
        spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', True)

        pandas_df = spark_df.toPandas()
        # Pandas operations here
        spark_df = spark.createDataFrame(pandas_df)
        ```

* When implementing custom transforms or custom AWS Glue jobs, developers should carefully consider the use of Spark actions and operations that cause data shuffling or that force immediate evaluation of transforms. Use of these operations can significantly impact the performance of your AWS Glue job.

    * For a list of operations that cause data shuffling, refer to the [Spark Programming Guide list of shuffle operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations).

    * Spark actions cause any unevaluated transformation to immediately execute. For a full list of Spark functions, refer to the [list of transformations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations) and [the list of actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions).

    * Use [cache](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cache.html) when you need to perform multiple operations on the same dataset to avoid reading from storage repeatedly.


## Unit Testing

The InsuranceLake Infrastructure and ETL unit tests for CDK code, Lambda functions, and AWS Glue jobs use [pytest](https://docs.pytest.org/).

To install the Python dependencies for development and testing, use the following `pip` command:
```bash
pip install -r requirements-dev.txt
```

Once the dependencies are installed, run tests with the following command (`--cov` will include a code coverage report):
```bash
python -m pytest --cov
```

{: .note }
> AWS Glue job unit tests will be automatically skipped if no AWS Glue or Spark environment is detected. A message similar to the following will be indicated:
> 
> ```
> test/test_custom_mapping.py::test_custommapping_renames_field SKIPPED (No PySpark environment found)           [ 17%]
> ```

To set up a local AWS Glue and Spark environment for testing, refer to the [Local AWS Glue and Apache Spark Development](#local-aws-glue-and-apache-spark-development) section.


## Copy OpenSource Git Repositories

Follow these instructions if you want to copy the InsuranceLake OpenSource repositories to your own repositories as a starting point. This is an alternative to forking the repository in Github.

1. Clone the OpenSource repositories locally.
    ```bash
    git clone https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure.git
    git clone https://github.com/aws-solutions-library-samples/aws-insurancelake-etl.git
    ```

1. Change the working directory to the location of the infrastruture code.
    ```bash
    cd aws-insurancelake-infrastructure
    ```

1. Use the following git commands copy the infrastructure repository to your own repository using the `develop` branch.

    {: .note}
    We are using the `develop` branch because the Dev environment deployment is triggered by commits to the develop branch.

    {: .important}
    Edit the `origin` URL to correspond to your repository.

    ```bash
    git remote rename origin opensource
    git remote add origin https://path/to/aws-insurancelake-infrastructure
    git checkout -b develop
    git push -u origin develop
    ```

1. Repeat these steps for the ETL repository.


## CodeCommit Instructions

{: .warning }
AWS CodeCommit is no longer available to new AWS customers. Existing customers of AWS CodeCommit can continue to use the service as normal. 

### CodeCommit as a mirror repository

InsuranceLake comes with a CodeCommit mirror repository stack in both the infrastructure and ETL repositories. This mirror repository is meant to act as a source for CodePipeline so that you can configure external repositories to sync with CodeCommit and trigger CodePipeline builds.

* [Instructions for configuring mirroring from Gitlab to CodeCommit](https://klika-tech.com/blog/2022/07/12/repository-mirroring-gitlab-to-codecommit/).

To deploy the CodeCommit mirror repository stacks, follow these steps:

1. Set the repository name parameters in `lib/configuration.py` for each codebase. An example of each follows.

    ```python
                # Name your CodeCommit mirror repo here (recommend matching your external repo)
                # Leave empty if you use CodeConnections or your repository is in CodeCommit already
                CODECOMMIT_MIRROR_REPOSITORY_NAME: 'aws-insurancelake-infrastructure',
                # or
                CODECOMMIT_MIRROR_REPOSITORY_NAME: 'aws-insurancelake-etl',
    ```

1. Deploy the CodeCommit mirror repository stacks for the infrastructure and ETL repositories.

    ```bash
    cdk deploy Deploy-InsuranceLakeEtlMirrorRepository
    cdk deploy Deploy-InsuranceLakeInfrastructureMirrorRepository
    ```

1. Follow the instructions in the [Quickstart with CI/CD](quickstart_cicd.md) or the [Full Deployment Guide](full_deployment_guide.md#deploying-cdk-stacks) for deploying the CodePipeline stacks.

### CodeCommit as a main repository

- This configuration requires you to setup the CodeCommit repository separately and before deploying InsuranceLake.

- Set the repository name parameters in `lib/configuration.py` for each codebase. An example of each follows.

    ```python
            # Use only if your repository is already in CodecCommit, otherwise leave empty!
            CODECOMMIT_REPOSITORY_NAME: 'aws-insurancelake-infrastructure',
            # or
            CODECOMMIT_REPOSITORY_NAME: 'aws-insurancelake-etl',
    ```

- Install the [Git CodeCommit Helper](https://docs.aws.amazon.com/codecommit/latest/userguide/setting-up-git-remote-codecommit.html):

    ```bash
    sudo pip install git-remote-codecommit
    ```


## Known Issues

* **CodeBuild Quotas**

    Ensure you are aware of the [service quotas for AWS CodeBuild](https://docs.aws.amazon.com/codebuild/latest/userguide/limits.html). Exceeding a quota will result in an error similar to the following:

    ```log
    Action execution failed
    Error calling startBuild: Cannot have more than 1 builds in queue for the account (Service: AWSCodeBuild; Status Code: 400; Error Code: AccountLimitExceededException; Request ID: e123456-d617-40d5-abcd-9b92307d238c; Proxy: null)
    ```

* **S3 Object Lock**

    Enabling [S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html) on the Cleanse or Consume S3 buckets breaks ETL data writes to the buckets. This is caused by known limitations to Hadoop's S3A driver used by Spark. These open issues are being tracked as [HADOOP-19080](https://issues.apache.org/jira/browse/HADOOP-19080) and [HADOOP-15224](https://issues.apache.org/jira/browse/HADOOP-15224). Enabling S3 Object Lock on these S3 buckets will result in an error similar to the following:

    ```log
    An error occurred while calling o237.saveAsTable. Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters (Service: Amazon S3; Status Code: 400; Error Code: InvalidRequest; Request ID: <request_id>; S3 Extended Request ID: <extended_request_id>; Proxy: null)
    ```

    It is possible to convert the ETL data write operations to use the [GlueContext getSink method](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-glue-context.html#aws-glue-api-crawler-pyspark-extensions-glue-context-get-sink) which supports writing to S3 buckets with Object Lock. However, this introduces a side effect of creating a new version of the Data Catalog table schema for every write operation.