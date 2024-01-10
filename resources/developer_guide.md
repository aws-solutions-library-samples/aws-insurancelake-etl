# Developer Guide

## Local CDK Deployment

Reference the [CDK Instructions](./cdk_instructions.md) for standard CDK project setup.

Prerequisites include:
* [Python](https://www.python.org/downloads/) (Python 3.9 or later, Python 3.11 recommended)
* [Node.js](https://nodejs.org/en/download/package-manager/) (CDK uses Node.js)

## Code Security

[CDK-Nag](https://github.com/cdklabs/cdk-nag) is integrated into the Python CDK stack and uses supressions only when necessary.
[Bandit](https://pypi.org/project/bandit/) and [Amazon CodeGuru](https://docs.aws.amazon.com/codeguru/) are used for static code analysis.

## Code Quality

### Style Guide for Python Code

AWS InsuranceLake follows [PEP8](https://www.python.org/dev/peps/pep-0008/) enforced through [flake8](https://flake8.pycqa.org/en/latest/) and [pre-commit](https://pre-commit.com/) (see below for instructions).

Please install and setup pre-commit before making any commits against this project. Example:

```{bash}
pip install pre-commit
pre-commit install
```

The above will create a git hook which will validate code prior to commits. Configuration for standards can be found in:

* [.flake8](../.flake8)
* [.pre-commit-config.yaml](../.pre-commit-config.yaml)

### Glue Job/Spark Code

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

* Due to their complexity InsuranceLake Glue jobs are not editable in the Glue Visual Editor. Glue job development and testing is best done in [Glue Notebooks](https://docs.aws.amazon.com/glue/latest/dg/notebook-getting-started.html) or a local Python-enabled IDE in the [Glue Docker container](https://aws.amazon.com/blogs/big-data/developing-aws-glue-etl-jobs-locally-using-a-container).

* The majority of InsuranceLake operations are done using Spark-native DataFrames because conversions to Glue DynamicFrames and Pandas DataFrames come with a cost. InsuranceLake was also designed to be as portable as possible to other Spark environments (with the exception of Glue Data Quality). **We recommend you follow the practice of avoiding DataFrame conversions in your Glue jobs**.

* When there is functionality needed from Pandas that is not available in Spark, there are three methods to consider:
   - [Pandas-on-Spark DataFrame](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/pandas_pyspark.html#pyspark)
      Using the `DataFrame.pandas_api()` is performant because the data and operations are distributed. Avoid operations like `DataFrame.to_numpy()` that that require the data to be collected on the driver (non-distributed). The Pandas API on Spark does not target 100% compatibility, so you may experience errors running your workflow.

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

* To include 3rd party Python libraries, use the Glue job parameter `--additional-python_modules` specified in the Glue job definition in the Glue stack. See the [AWS Glue User Guide](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html#extra-py-files-support) for more information.

* To include your own Python libraries, simply create the additional library in `lib/glue_scripts/lib` and redeploy the Glue stack using CDK. The CDK Glue Stack will automatically identify the new library and include it in the `--extra-py-files` parameter for all Glue jobs. Ensure you also import the module in the Glue job script.

* To include Spark JAR libraries, simply copy the JAR file to the `lib/glue_scripts/lib` folder and redeploy the Glue stack using CDK. The CDK Glue Stack will automatically identify the new library and include it in the `--extra-jars` parameter for all Glue jobs.

## Testing

### Unit Testing

The Python CDK unit tests use [pytest](https://docs.pytest.org/), which will be installed as part of the solution requirements.

Run tests with the following command (`--cov` will include a code coverage report):
```bash
python -m pytest --cov
```

Glue Job Unit Tests will be automatically skipped if no Glue/Spark environment is detected. For information on setting up a local Glue/Spark environment, refer to the following guides:

* [AWS Blog: Develop and test AWS Glue version 3.0 and 4.0 jobs locally using a Docker container](https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/)
* [AWS Developer Guide: Developing and testing AWS Glue job scripts locally](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html)
* Note that Hudi support and Glue Data Quality are not available in the AWS Glue Docker image.

## Known Issues

   ```bash
   Action execution failed
   Error calling startBuild: Cannot have more than 1 builds in queue for the account (Service: AWSCodeBuild; Status Code: 400; Error Code: AccountLimitExceededException; Request ID: e123456-d617-40d5-abcd-9b92307d238c; Proxy: null)
   ```

   [Quotas for AWS CodeBuild](https://docs.aws.amazon.com/codebuild/latest/userguide/limits.html)