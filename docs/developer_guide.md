# InsuranceLake Developer Guide

## Contents

* [Local CDK Deployment](#local-cdk-deployment)
* [Local Glue/Spark Development](#local-gluespark-development)
   * [Local Iceberg Development](#local-iceberg-development)
   * [Visual Studio Code](#visual-studio-code)
* [Codebase](#codebase)
    * [Source Code Structure](#source-code-structure)
    * [Transformation Modules](#transformation-modules)
* [Code Quality](#code-quality)
   * [Style Guide for Python Code](#style-guide-for-python-code)
   * [Glue Job/Spark Code](#glue-jobspark-code)
* [Code Security](../README.md#security)
* [Testing](#testing)
   * [Unit Testing](#unit-testing)
* [AWS CodePipeline and GitHub Integration](#aws-codepipeline-and-github-integration)
* [AWS CodeBuild Known Issues](#aws-codebuild-known-issues)


## Local CDK Deployment

Reference the [CDK Instructions](./cdk_instructions.md) for standard CDK project setup.

Prerequisites include:
* [Python](https://www.python.org/downloads/) (Python 3.9 or later, Python 3.12 recommended)
* [Node.js](https://nodejs.org/en/download/package-manager/) (CDK uses Node.js)


## Local Glue/Spark Development

To setup a local Glue/Spark environment for testing, use the AWS-provided Glue Docker container image which can be retrieved from the [AWS Glue Dockerhub repository](https://hub.docker.com/r/amazon/aws-glue-libs/tags). Ensure you use the right tag for the version of AWS Glue used in the stack (currently v4).

For detailed instructions on setting up a local Glue/Spark environment, refer to the following guides:

* [AWS Developer Guide: Developing and testing AWS Glue job scripts locally](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html)
* [AWS Blog: Develop and test AWS Glue version 3.0 and 4.0 jobs locally using a Docker container](https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/)
* [Developing AWS Glue ETL jobs locally using a container](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container)

**NOTE:** Glue Data Quality is not available in the AWS Glue Docker images.

### Local Iceberg Development

Local development and testing of Iceberg tables is possible using the AWS-provided Glue Docker container. To enable this capability, the Docker container needs 1 or 2 additional libraries.

1. Spark with Scala runtime Jar
   * required to run [unit tests](#unit-testing)
1. aws-bundle Jar
   * required for integration testing or local running and debugging with an AWS session

Both JAR files can be downloaded from the [Iceberg Releases page](https://iceberg.apache.org/releases/#downloads). The last versions known to work with AWS Glue 4.0 are [iceberg-spark-runtime-3.3_2.12-1.5.2.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.5.2/iceberg-spark-runtime-3.3_2.12-1.5.2.jar) and [iceberg-aws-bundle-1.5.2.jar](https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar).

To enable these libraries, copy them to the `/home/glue_user/spark/jars/` location in the Glue Docker image.

For more detailed information on Iceberg unit testing, refer to the article [Navigating the Iceberg: unit testing Iceberg tables with PySpark](https://medium.com/datamindedbe/navigating-the-iceberg-unit-testing-iceberg-tables-with-pyspark-4d19de51bf57)

If you attempt to run the unit tests without installing the Iceberg library, you will see the following error:
```log
py4j.protocol.Py4JJavaError: An error occurred while calling o119.create.
: org.apache.spark.SparkException: Cannot find catalog plugin class for catalog 'local': org.apache.iceberg.spark.SparkCatalog
```

### Visual Studio Code

To run and debug the ETL Glue jobs in Visual Studio Code, you'll need a launch configuration defined in `launch.json`. The following configuration provides the required parameters for all three of the InsuranceLake ETL Glue jobs. You can also find this sample configuration in [the `launch.json.default` Visual Studio Code configuration file included in the repository](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/.vscode/launch.json.default). Be sure to replace the AWS Account ID placeholders with the AWS Account ID where InsuranceLake is deployed.

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

Table below explains how this source code is structured:

| File / Folder    | Description
|---    |---
| [app.py](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/app.py) | Application entry point
| [code_commit_stack](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/code_commit_stack.py) | Optional stack to deploy an empty CodeCommit respository for mirroring |
| [pipeline_stack](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/pipeline_stack.py) | Pipeline stack entry point
| [pipeline_deploy_stage](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/pipeline_deploy_stage.py) | Pipeline deploy stage entry point
| [dynamodb_stack](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/dynamodb_stack.py) | Stack creates DynamoDB Tables for Job Auditing and ETL transformation rules
| [glue_stack](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_stack.py) | Stack creates Glue Jobs and supporting resources such as Connections, S3 Buckets (script and temporary) and an IAM execution Role
| [step_functions_stack](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/step_functions_stack.py) | Stack creates an ETL State machine which invokes Glue Jobs and supporting Lambdas - state machine trigger and status notification
| [athena_helper_stack](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/athena_helper_stack.py) | Stack creates an Athena workgroup with query results bucket ready for demonstration SQL queries
| [Collect-to-Cleanse Glue Script](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_collect_to_cleanse.py) | Glue pySpark job data processing logic for Collect bucket data, which stores results in the Cleanse bucket
| [Cleanse-to-Consume Glue Script](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_cleanse_to_consume.py) | Glue pySpark job data processing logic for Cleanse bucket data, which stores results in the Consume bucket
| [Entity Match Glue Script](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_consume_entity_match.py) | Glue pySpark job data processing logic for Entity Matching, which stores results in the Consume bucket
| [ETL Job Auditor](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/etl_job_auditor/lambda_handler.py) | Lambda script to update DynamoDB in case of glue job success or failure
| [ETL Trigger](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/state_machine_trigger/lambda_handler.py) | Lambda script to trigger step function and initiate DynamoDB
| [ETL Transformation Mapping and Specification](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/transformation-spec/) | Field mapping and transformation specification logic to be used for data processing from Collect to Cleanse
| [ETL Transformation SQL](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/transformation-sql/) | Transformation SQL logic to be used for data processing from Cleanse to Consume
| [ETL Data Quality Rules](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/dq-rules/) | Glue Data Quality rules for quality checks from Cleanse to Consume
| [test](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/test)| This folder contains pytest unit tests
| [resources](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/resources) | This folder has architecture, process flow diagrams, sample data, and additional documentation

### Transformation Modules

| File / Folder    | Description
|---    |---
| [datatransform_dataprotection](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_dataprotection.py) | pySpark logic to redact, hash, and tokenize sensitive data columns
| [datatransform_lookup](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_lookup.py) | pySpark logic to perform column value lookup operations
| [datatransform_misc](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_misc.py)  | pySpark logic for miscellaneous data transformation functions, such as filtering rows
| [datatransform_premium](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_premium.py) | pySpark logic to perform common insurance industry data transforms
| [datatransform_stringmanipulation](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_stringmanipulation.py) | pySpark logic to perform regex transforms, and Python formatting string operations on data
| [datatransform_structureddata](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_structureddata.py)  | pySpark logic to perform operations on nested data structures usually created from JSON files
| [datatransform_typeconversion](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datatransform_typeconversion.py) | pySpark logic to convert date columns, and other data types to standard format
| [custom_mapping](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/custom_mapping.py) | pySpark logic to rename columns according to a map file
| [dataquality_check](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/dataquality_check.py) | Glue logic to run Data Quality rules according to a rules file
| [datalineage](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/datalineage.py) | Custom data lineage tracking class designed to work with InsuranceLake transforms


## Code Quality

### Style Guide for Python Code

AWS InsuranceLake follows [PEP8](https://www.python.org/dev/peps/pep-0008/) enforced through [flake8](https://flake8.pycqa.org/en/latest/) and [pre-commit](https://pre-commit.com/) (see below for instructions).

To enable pre-commit checks install and setup the pre-commit library:

```{bash}
pip install pre-commit
pre-commit install
```

The above will create a git hook which will validate code prior to commits using [flake8](https://flake8.pycqa.org/en/latest/). Configuration for standards can be found in:

* [.flake8](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/.flake8)
* [.pre-commit-config.yaml](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/.pre-commit-config.yaml)

### Glue Job/Spark Code

Due to their complexity InsuranceLake Glue jobs are not editable in the Glue Visual Editor. Glue job development and testing is best done in [Glue Notebooks](https://docs.aws.amazon.com/glue/latest/dg/notebook-getting-started.html) or a local Python-enabled IDE in a Glue Docker container. Documentation on setting up a Glue Docker container can be found in the [Local Glue/Spark Development](#local-gluespark-development) section.

To more quickly diagnose issues with features not available in the Glue Docker container, or when working with large datasets that may be too slow to process locally, bypassing the Step Functions workflow to execute specific Glue jobs can be helpful. The following are example executions for each of the three InsuranceLake ETL Glue jobs with the minimum required parameters:

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

Additional code considerations follow:

* To include 3rd party Python libraries, use the Glue job parameter `--additional-python_modules` specified in the Glue job definition in the Glue stack. See the [AWS Glue User Guide](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html#extra-py-files-support) for more information.

* To include your own Python libraries, simply create the additional library in `lib/glue_scripts/lib` and redeploy the Glue stack using CDK. The CDK Glue Stack will automatically identify the new library and include it in the `--extra-py-files` parameter for all Glue jobs. Ensure you also import the module in the Glue job script.

* To include Spark JAR libraries, simply copy the JAR file to the `lib/glue_scripts/lib` folder and redeploy the Glue stack using CDK. The CDK Glue Stack will automatically identify the new library and include it in the `--extra-jars` parameter for all Glue jobs.

* InsuranceLake Glue jobs follow the AWS best practice of getting the Spark session from the Glue context and calling `job.init()`. `job_commit()` is always called at the end of the script. These functions are used to update the state change to the service.

   InsuranceLake Glue jobs do not make use of [Glue job bookmarks](https://docs.aws.amazon.com/glue/latest/dg/programming-etl-connect-bookmarks.html) because almost all transformation of data is done in Spark DataFrames. Glue job bookmarks are **disabled by default for all InsuranceLake Glue jobs**. For future use the Glue jobs set the `transformation_ctx` for all DynamicFrame operations.

   A difference between the standard AWS Glue job source code and the InsuranceLake code is that the **job initialization** is contained within `main` and **not in the global Python context**. This is done to facilitate unit testing of the Glue jobs in a local environment using the Glue Docker container.

* The suggested skeleton code to construct a new Glue job in the InsuranceLake pipeline follows:

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

   # Handle optional arguments
   for arg in sys.argv:
      if '--data_lineage_table' in arg:
         expected_arguments.append('data_lineage_table')

   def main():
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

* The majority of InsuranceLake operations are done using Spark-native DataFrames because conversions to Glue DynamicFrames and Pandas DataFrames come with a cost. InsuranceLake was also designed to be as portable as possible to other Spark environments (with the exception of Glue Data Quality). **We recommend you follow the practice of avoiding DataFrame conversions in your Glue jobs**.

* When there is functionality needed from Pandas that is not available in Spark, there are three methods to consider:
   - [Pandas-on-Spark DataFrame](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/pandas_pyspark.html#pyspark)
      Using the `DataFrame.pandas_api()` is performant because the data and operations are distributed. Avoid operations like `DataFrame.to_numpy()` that require the data to be collected on the driver (non-distributed). The Pandas API on Spark does not target 100% compatibility, so you may experience errors running your workflow.

   - [Pandas UDF](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.pandas_udf.html)
      Pandas UDFs are user defined functions that are executed by Spark using Arrow to transfer data and Pandas to work with the data. When used in combination with `withColumn` or `select`, a Pandas UDF can perform Pandas library vectorized operations in a distributed manner on individual columns supplied as arguments.

   - [PyArrow for Conversions](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
      If full conversion to a Pandas DataFrame is needed, ensure your Glue job enables Apache Arrow support for data conversion as follows:

      ```python
      spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', True)

      pandas_df = spark_df.toPandas()
      # Pandas operations here
      spark_df = spark.createDataFrame(pandas_df)
      ```

* When implementing custom transforms or custom Glue jobs, developers should carefully consider the use of Spark actions and operations that cause data shuffling, or that force immediate evaluation of transforms. Use of these operations can significantly impact the performance of your Glue job.

   * For a list of operations that cause data shuffling, refer to the [Spark Programming Guide list of shuffle operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations).

   * Spark actions cause any unevaluated transformation to immediately execute. For a full list of Spark functions, refer to the [list of transformations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations) and [the list of actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions).

   * Use [cache](#https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cache.html) when you need to perform multiple operations on the same dataset to avoid reading from storage repeatedly.


## Testing

### Unit Testing

The InsuranceLake Infrastructure and ETL unit tests for CDK code, Lambda functions, and Glue jobs use [pytest](https://docs.pytest.org/).

To install the Python dependencies for development and testing, use the following `pip` command:
```bash
pip install -r requirements-dev.txt
```

Once the dependencies are installed, run tests with the following command (`--cov` will include a code coverage report):
```bash
python -m pytest --cov
```

* **NOTE:** Glue Job Unit Tests will be automatically skipped if no Glue/Spark environment is detected. A message similar to the following will be indicated:
   ```
   test/test_custom_mapping.py::test_custommapping_renames_field SKIPPED (No pySpark environment found)           [ 17%]
   ```

To setup a local Glue/Spark environment for testing, refer to the [Local Glue/Spark Development](#local-gluespark-development) section.


## AWS CodePipeline and GitHub Integration

Integration between AWS CodePipeline and GitHub requires a personal access token. This access token is stored in Secrets Manager. This is a one-time setup and is applicable for all target AWS environments and all repositories created under the organization in GitHub.com. Follow the below steps:

1. Create a personal access token in your GitHub. Refer to [Creating a personal access token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token) for details

1. Run the below command:
    ```bash
    python3 ./lib/prerequisites/configure_account_secrets.py
    ```

1. Enter or paste in the Github personal access token when prompted
   * NOTE: The access token value will not appear on the screen

1. Confirm the information for the AWS Secrets Manager Secret is correct and type 'y'

1. Expected output:
    ```log
    Pushing secret: /InsuranceLake/GitHubToken
    A secret is added to AWS Secrets Manager with name **/InsuranceLake/GitHubToken**
    ```


## Known Issues

   * **CodeBuild Quotas**

      Ensure you are aware of the [service quotas for AWS CodeBuild](https://docs.aws.amazon.com/codebuild/latest/userguide/limits.html). Exceeding a quota will result in an error similar to the following:

      ```log
      Action execution failed
      Error calling startBuild: Cannot have more than 1 builds in queue for the account (Service: AWSCodeBuild; Status Code: 400; Error Code: AccountLimitExceededException; Request ID: e123456-d617-40d5-abcd-9b92307d238c; Proxy: null)
      ```

   * **S3 Object Lock**

      Enabling [S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html) on the Cleanse or Consume S3 buckets breaks ETL data writes to the buckets. This is caused by known limitations to Hadoop's S3A driver used by Spark. These open issues are being tracking as [HADOOP-19080](https://issues.apache.org/jira/browse/HADOOP-19080) and [HADOOP-15224](https://issues.apache.org/jira/browse/HADOOP-15224). Enabling S3 Object Lock on these S3 buckets will result in an error similar to the following:

      ```log
      An error occurred while calling o237.saveAsTable. Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters (Service: Amazon S3; Status Code: 400; Error Code: InvalidRequest; Request ID: <request_id>; S3 Extended Request ID: <extended_request_id>; Proxy: null)
      ```

      It is possible to convert the ETL data write operations to use the [GlueContext getSink method](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-glue-context.html#aws-glue-api-crawler-pyspark-extensions-glue-context-get-sink) which supports writing to S3 buckets with Object Lock. However, this introduces a side effect of creating a new version of the Glue Data Catalog table schema for every write operation.