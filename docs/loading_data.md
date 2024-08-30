# Loading Data with InsuranceLake

## Contents

* [Landing Source Data](#landing-source-data)
* [Bucket Layout](#bucket-layout)
	* [Naming convention and location of configuration files](#naming-convention-and-location-of-configuration-files)
* [Configuration Recommendations](#configuration-recommendations)
* [File Format Identification](#file-format-identification)
	* [Auto Detection Formats](#auto-detection-formats)
	* [TSV, Pipe-delimited, Other Delimiters](#tsv-pipe-delimited-other-delimiter)
	* [Fixed Width](#fixed-width)
* [Corrupt Data](#corrupt-data)
	* [Method 1](#method-1)
	* [Method 2](#method-2)
* [Execute Pipeline without Upload](#execute-pipeline-without-upload)
* [Override Partition Values](#override-partition-values)


## Landing Source Data

The InsuranceLake ETL process is triggered when data is landed in the Collect S3 bucket.

We recommend starting your data preparation process by loading your data right away, without any configuration, so you can begin exploring your data using Athena SQL and Spark-based notebooks. This guide will provide the information needed to get the best results from this process.

You can land your data in the Collect S3 bucket via:

- the [S3 Console](http://console.aws.amazon.com/s3), which can be used to create folders and drag and drop files
- AWS SDKs or REST APIs, which can be used to embed file copying into workflows and applications
- AWS CLI, which can copy objects from local machines or via scripts with a full path
- AWS Transfer Family, which can receive data via an SFTP endpoint

For walkthroughs, examples, and details refer to the AWS Documentation [Uploading objects to S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html).

For information on AWS Transfer Family, refer to the AWS Documentation [What is AWS Transfer Family?](https://docs.aws.amazon.com/transfer/latest/userguide/what-is-aws-transfer-family.html)

**When you load data with no configuration, the ETL will clean all column names so that they can be saved successfully in Parquet format**. 


## Bucket Layout

In order to allow transform specifications to be matched with source system data and organized in groups, each of the three ETL stage buckets (Collect, Cleanse, Consume) have similar directory layouts. The first level represents the source system name or the database that will group the underlying tables. The second layer represents the data set or table containing the uploaded data. In the Collect bucket, the source files are stored at the second layer. In the Cleanse bucket, data is converted to compressed Parquet files and stored in partitions at the second layer.

When you create the folder structure for storing source files in the Collect bucket, you are defining the folder layout for the Cleanse and Consume bucket, as well as defining the database and table organization in the Glue Data Catalog.

* S3 objects landed with *less than two levels of folder depth* will be ignored by the state-machine-trigger Lambda function; no ETL workflow will be triggered. S3 objects landed with *more than two levels of folder depth* will be processed with [Partition Value Override](#override-partition-values).

* The Cleanse-to-Consume Glue job [can be configured](using_sql.md#override-table-name-example) to create multiple tables in the Consume layer, and can be configured to override the table name provided in the folder structure. Therefore, the Consume layer will not always match the Collect bucket layout.

Example bucket layout follows:

![Bucket Layout Example](./bucket-layout-example.png)

The files for the transformation/input configuration, schema mapping, data quality rules, Athena/Spark SQL, and entity matching configuration will also follow a naming convention that matches the bucket layout.

* **Important Note:** This matching is **case sensitive** (Glue Catalog database and table names will lose case sensitivity when created and always appear in all lowercase)

### Naming convention and location of configuration files

|Purpose  |ETL Scripts Bucket Location  |Naming Convention
|---   |---  |---
|[Schema Mapping](schema_mapping.md) |/etl/transformation-spec |`<database name>-<table name>.csv`
|[Transformation](transforms.md)/[Input Config](file_formats.md#input-specification)   |/etl/transformation-spec |`<database name>-<table name>.json`
|[Data Quality Rules](data_quality.md)   |/etl/dq-rules   |`dq-<database name>-<table name>.json`
|[Spark SQL](using_sql.md#spark-sql)   |/etl/transformation-sql  |`spark-<database name>-<table name>.sql`
|[Athena SQL](using_sql.md#athena-sql)  |/etl/transformation-sql  |`athena-<database name>-<table name>.sql`
|[Entity Match Config](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/transformation-spec/Customer-entitymatch.json)   |/etl/transformation-spec |`<database name>-entitymatch.json`


## Configuration Recommendations

When the ETL loads source data with no schema mapping or transformation configuration files, it will create *recommended* configurations. These recommendations are saved to the Glue Temp bucket, which follows the naming convention `<environment>-insurancelake-<account>-<region>-glue-temp`, in the folder `/etl/collect_to_cleanse` which following the naming convention `<database>-<table>` (extension `csv` for schema mapping and `json` for transformations). Simply copy the files to your development environment, edit them as needed, and upload them to the ETL Scripts S3 Bucket in the [appropriate location](#naming-convention-and-location-of-configuration-files).

Using these recommendations help accelerate your creation of configuration files with a syntactically correct template and complete list of fields in the schema.

More details are available on how the ETL generates these recommendations:
* [Behavior When There is No Schema Mapping](schema_mapping.md#behavior-when-there-is-no-schema-mapping)
* [Behavior When There is No Transformation Specification](transforms.md#behavior-when-there-is-no-transformation-specification)

To get started quickly building a set of data quality rules with recommendations, use [Glue Data Quality Recommendations](data_quality.md#getting-started).


## File Format Identification

### Auto Detection Formats

The ETL can load many source file formats for the first time with no configuration (in other words, no schema mapping file, no input/transformation specification file, no data quality file, and no SQL files). We recommend using this minimalistic configuration approach whenever possible, because it reduces the time to load and publish data.

**Comma Separated Value (CSV) is the default file format for the ETL.** If no other file format is identified or configured, the ETL will assume the file is CSV.

Formats that could require no configuration for initial loading follow (with exceptions noted). Follow the provided links for details on configuration options.

|Format	|Exceptions
|---	|---
|[CSV](file_formats.md#csv-comma-separated-value)	|The ETL will default to using the first row of a CSV file as the header, `"` as the quote character, and `"` as the escape character. To change these options, you must provide [an `input_spec` configuration](file_formats.md#csv-comma-separated-value). Other Spark CSV read defaults affect the way the ETL interprets CSV files. Refer to the [Spark CSV File Data Source Option Documentation](https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option) for details.
|[Parquet](file_formats.md#parquet)	|Parquet files that do not end with a `parquet` extension will need to be renamed or you must provide an `input_spec` configuration indicating the file format. Multi-file Parquet data sources will require pipeline customization detailed in [Handling Multi-file Data Sets](file_formats.md#handling-multi-file-data-sets).
|[Excel](file_formats.md#microsoft-excel-format-support)	|Excel files are identified by the extensions `xls`, `xlsx`, `xlm`, `xlsm`. Loading Excel files requires the [installation of the Spark Excel libraries](file_formats.md#obtaining-the-driver). In addition, the ETL will default to using the first sheet in an Excel workbook, the data at cell A1, and assume the first row of data is the header. If your Excel file has data on a different sheet, in a different position, has no header, or requires a password, you must provide [an `input_spec` configuration](file_formats.md#configuration).
|[JSON](file_formats.md#json)	|JSON files are identified by the extensions `json`, `.jsonl`. The ETL will default to loading JSON files as JSON Lines (each line contains one JSON object). If your JSON file uses multiple lines for a single object, you must provide an `input_spec` configuration.

Other formats require some configuration even for the first load.

### TSV, Pipe-delimited, Other Delimiters

The ETL will not attempt to detect source data files containing Tab Seperated Values (TSV), pipe (`|`) delimiters, or other delimiter characters. If your source data file uses a delimiter other than commas, you must specify an `input_spec` configuration indicating the file format (and optionally whether there is a header). Refer to the [Pipe-delimited file format documentation](file_formats.md#pipe-delimited) and [TSV file format documentation](file_formats.md#tsv-tab-separated-value) for configuration details.

### Fixed Width

With no configuration, the ETL will load fixed width data files as single column data sets. In order to load the source data with multiple columns, you must specify the field widths using the schema mapping configuration file. Refer to the [Fixed Width File Format Schema Mapping Documentation](schema_mapping.md#fixed-width-file-format) for details on how to do this.


## Corrupt Data

When loading source data, you will likely encounter files with some corrupt rows of data. Examples of corruption include missing columns (such as a totals row), text in numeric columns, shifted columns, and unrecognized encoding for characters in a field.

Corrupt data in source data files can cause unexpected behavior with Spark's ability to infer schema types, and with performing required aggregate operations, such as a summation of a numeric column. To work around these issues and make your data pipeline more resiliant, we recommend using data quality rules to quarantine corrupt data, or interrupt the pipeline. Configuration details can be found in the [Data Quality with Glue Data Quality Reference](data_quality.md#configuration).

Example data set with a totals row that causes schema problems:

|CoverageCode	|EffectiveDate	|WrittenPremium
|---	|---	|---
|WC	|2024-01-01	|50000
|Auto	|2024-02-01	|2500
|	|Total	|52500

The value of "Total" in the `EffectiveDate` column will cause Spark to infer a field type of `string`, which is not desirable. If we simply use a `date` transform to convert the field type, Spark will convert the value "Total" to `null`, and we will be left with an extra $52,500 of written premium. To work around this issue, we can use one of two methods:

### Method 1

This method uses a [`before_transform` data quality rule](data_quality.md#configuration) to quarantine the row of data with the value "Total" in the `EffectiveDate` column. The data quality rule identifies the row by looking for values that match a standard date pattern, and removing rows that do not match. Because the `before_transform` rule runs before transforms, we can next use a [`date` transform](transforms.md#date) to convert the clean column of `data` to a date field type.

`dq-rules` configuration:
```json
{
    "before_transform": {
        "quarantine_rules": [
            "ColumnDataType 'EffectiveDate' = 'DATE'"
        ]
    }
}
```

`transformation-spec` configuration:
```json
{
	"transform_spec": {
		"date": [
			{
				"field": "EffectiveDate",
				"format": "yyyy-mm-dd"
			}
		]
	}
}
```

### Method 2

This method uses a [`filterrows` transform](transforms.md#filterrows) to remove the row of data with the value "Total" in the `EffectiveDate` column. The `filterrows` condition looks for `null` values in the `CoverageCode` column, and filters out those rows. Because the `filterrows` transform occurs first in the `transfom_spec` section, we can next use a [`date` transform](transforms.md#date) to convert the clean column of data to a `date` field type.

`transformation-spec` configuration:
```json
{
	"transform_spec": {
		"filterrows": [
			{
				"condition": "CoverageCode is not null"
			}
		],
		"date": [
			{
				"field": "EffectiveDate",
				"format": "yyyy-mm-dd"
			}
		]
	}
}
```


## Execute Pipeline without Upload

If local development is not desireable or possible (for example, access to AWS account via API is restricted, or testing of data quality rules is needed), you will need a method to rapidly iterate through additions, improvements, and fixes until you reach your desired state.

Using the Start Execution capability of Step Functions on historical executions allows you to quickly iterate through schema mapping, transform specification, data quality rules, and SQL changes when working with data in development. This method will ensure you replace the data in the original partition and skip the S3 file upload.

1. Navigate to [Step Functions in the AWS Console](http://console.aws.amazon.com/states)
1. Select the ETL State Machine which follows the naming convention `<environment>-insurancelake-etl-state-machine`
1. Click on a prior execution for the workflow you want to rerun, which will open the detailed view
1. Click `New Execution` at the top of the screen
	![Step Functions New Execution](step_functions_new_execution.png)
1. Inspect the execution parameters to ensure you are loading the correct source file with the correct partition values
	![Step Functions Start Execution](step_functions_start_execution.png)
1. Optionally edit the exection name to something easier to identify in the execution history
1. Click `Start Execution`
	* The new execution detail will automatically open in the AWS console.

* Using this procedure allows overriding of the partition parameters in the Start Execution modal form. Simply edit the JSON formatted input parameters for `year`, `month`, and `day`.

* Step Functions execution history entries are kept for 90 days after the execution is closed. Refer to the [Step Functions AWS Documentation on Service Limits](https://docs.aws.amazon.com/step-functions/latest/dg/limits-overview.html#service-limits-state-machine-executions) for the most current details.


## Override Partition Values

For historical data loads or replacing an existing partition with new data, the InsuranceLake ETL supports defining partition values in the folder structure of the Collect bucket.

Specifically, within the table level (2nd level) folder, you can create a folder representing the `year` partition value, another folder representing the `month` partition value, and another folder representing the `day` partition value. The source data file can then be landed in this nested folder structure and folder names will override the values from the created date of the Collect S3 bucket source file.

When you override partition values, keep in mind the behavior of the ETL loading data in the [Cleanse](schema_evolution.md#cleanse-layer) and [Consume](schema_evolution.md#consume-layer) layers.

Example bucket layout with partition value overrides follows:

![Bucket Layout with Partition Override Example](bucket-layout-partition-override-example.png)

NOTE: If you've made changes to the Collect to Cleanse Glue Job in order to support multi-file Parquet data sets, the partition value override functionality may be disabled. More details can be found in the [Handling Multi-file Data Sets Documentation](file_formats.md#handling-multi-file-data-sets).

Other methods to override partition values are covered in other sections of the documentation:

* Using the [Step Functions New Execution capability](#execute-pipeline-without-upload), a pipeline maintainer can repeat a previously completed workflow, skipping the file upload step, and override execution parameters such as partition year, month, day, and source file S3 location. For details see the [Pipeline Usage Documentation](#execute-pipeline-without-upload).

* Glue jobs can be manually executed with override parameters for individual Glue jobs. For details on how to manually initiate Glue jobs and override parameters, see the [InsuranceLake Developer Documentation on Glue Jobs](developer_guide.md#glue-jobspark-code).