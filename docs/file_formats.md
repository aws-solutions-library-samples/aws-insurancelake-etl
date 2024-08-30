# InsuranceLake File Formats and Input Specification

## Contents

* [Input Specification](#input-specification)
* [CSV](#csv-comma-separated-value)
* [TSV](#tsv-tab-separated-value)
* [Pipe-delimited](#pipe-delimited)
* [JSON](#json)
* [Fixed Width](#fixed-width)
    * [Handling Encoding Issues](#handling-encoding-issues)
* [Parquet](#parquet)
    * [Handling Multi-file Data Sets](#handling-multi-file-data-sets)
* [Microsoft Excel Format Support](#microsoft-excel-format-support)
    * [Obtaining the Driver](#obtaining-the-driver)
    * [Driver Installation](#driver-installation)
    * [Configuration](#configuration)
    * [Notes on Excel Support](#notes-on-excel-support)

## Input Specification

Input specification configuration is defined in the `input_spec` section of the workflow's JSON configuration file. The filename follows the convention of `<database name>-<table name>.json` and is stored in the `/etl/transformation-spec` folder in the `etl-scripts` bucket. When using CDK for deployment, the contents of the `/lib/glue_scripts/lib/transformation-spec` directory will be automatically deployed to this location. The `input_spec` section is used to specify input file format configuration and other data pipeline configuration, and co-exists with the `transform_spec` section.

|Parameter  |Description
|---    |---
|table_description  |Text string description to use for the table in the AWS Glue Catalog; only applies to the table in the Cleanse bucket
|allow_schema_change    |Setting to control permitted schema evolution; supported values: `permissive`, `strict`, `reorder`, `evolve`; more information is provided in the [Schema Evolution Documentation](schema_evolution.md#schema-change-setting)
|strict_schema_mapping  |Boolean value that controls whether to halt the pipeline operation if fields specified in the schema mapping are not present in the input file; more information is provided in the [Schema Mapping Dropping Columns Documentation](schema_mapping.md#dropping-columns)
|csv    |Section to specify CSV file specific configuration
|tsv    |Section to specify TSV file specific configuration
|pipe   |Section to specify pipe-delimited file specific configuration
|parquet    |Section to indicate Apache Parquet input file support
|json   |Section to specify JSON file specific configuration
|fixed   |Section to indicate fixed width input file support
|excel  |Section to specify Excel file specific configuration

Example of other data pipeline configuration parameters:

```json
{
	"input_spec": {

        "table_description": "Description will only apply to cleanse bucket",

        "allow_schema_change": "permissive",

        "strict_schema_mapping": true
    }
}
```

## CSV (Comma Separated Value)

Comma separated value file format is **the default file format for the InsuranceLake ETL**. If no input specification configuration is specified, and the file extension is not recognized, the ETL will assume that CSV file format is desired.

The `csv` configuration section can be used to specify additional format options, **including a custom field delimiter**:

|Format Option   |Default    |Description
|---    |---    |---
|header |true   |Specifies whether the first row should be interpreted as a header row; to understand the schema when no header row is present, refer to the [Schema Mapping Files with No Header Documentation](schema_mapping.md#files-with-no-header)
|quote_character    |`"`  |Character used for escaping quoted field values containing the field separator
|escape_character   |`"`  |Character used for escaping quotes inside an already quoted field value
|delimiter  |Dependent on the section header   |Field separator character or characters; overrides the character indicated by the specified file format

* Spark CSV read defaults differ in some ways from [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180) definitions for CSV. For example, multi-line quoted is not supported by default, and the escape character is backslash by default. **InsuranceLake ETL** changes the default escape character to `"` to match RFC 4180. However, multi-line quoted is not supported, due to its [negative impact on parallelization in Spark](https://issues.apache.org/jira/browse/SPARK-22236).

* Other Spark CSV read defaults affect the way the ETL interprets delimited files. Refer to the [Spark CSV File Data Source Option Documentation](https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option) for details.

* Overriding the delimiter indicated by the section header could reduce the self-documenting effectiveness of your input specification configuration.

Example of configuration for a CSV file with no header, single quote for the quote character, and a backslash escape character (instead of the default double-quote character):

```json
{
    "input_spec": {
        "csv": {
            "header": false,
            "quote_character": "'",
            "escape_character": "\\"
        }
    }
}
```

## TSV (Tab Separated Value)

Tab separated value input files are not identified by any file extension; the ETL will only interpret an input file as tab separated value if the file format is specified in the `input_spec` configuration.

The `tsv` configuration section can be used to indicate if a header row is expected, to specify a different quote character, to specify a different escape character, or to specify a custom field delimiter. CSV, TSV, and pipe-delimited formats share the same configuration section options. Complete details can be found in the [CSV format documentation](#csv-comma-separated-value).

Example of a configuration for a TSV file with a header row:

```json
{
    "input_spec": {
        "tsv": {
            "header": true
        }
    }
}
```

## Pipe-delimited

Pipe-delimited input files are not identified by any file extension; the ETL will only interpret an input file as pipe-delimited if the file format is specified in the `input_spec` configuration.

The `pipe` configuration section can be used to indicate if a header row is expected, to specify a different quote character, to specify a different escape character, or to specify a custom field delimiter. CSV, TSV, and pipe-delimited formats share the same configuration section options. Complete details can be found in the [CSV format documentation](#csv-comma-separated-value).

Example of a configuration for a pipe-delimited file with a header row:

```json
{
    "input_spec": {
        "pipe": {
            "header": true
        }
    }
}
```

## JSON

JSON input files are identified with the file extensions `.json` and `.jsonl`.

Single line JSON or [JSONLines formatted](http://jsonlines.org/) multi-record files are supported by default. To support multiline single-record JSON, use the `multiline` parameter.

Example of a configuration for a single-record multiline JSON file:

```json
{
    "input_spec": {
        "json": {
            "multiline": true
        }
    }
}
```

## Fixed Width

The InsuranceLake ETL will only interpret an input file as fixed width if the file format is specified in the `input_spec` configuration; fixed width input files are not identified by any file extension. The `fixed` configuration section has no other parameters; the value should defined as an empty JSON object (for forward compatibility).

To specify the width and position of each column in the file, use the schema mapping file with the extra column `Width`. More details and an example schema mapping file is available in the [Schema Mapping Fixed Width File Format Documentation](schema_mapping.md#fixed-width-file-format).

Example of a configuration for a fixed width file:

```json
{
    "input_spec": {
        "fixed": {}
    }
}
```

### Handling Encoding Issues

Fixed width format data files may have characters encoded in formats that are not expected by the source system (for example, from more modern upsteam systems). Specifically, there may be multi-byte single characters in the fixed width file that are counted as the multiple characters for the width of field. [Spark text file reading](https://spark.apache.org/docs/latest/sql-data-sources-text.html) will detect and encode these characters automatically, which can result in incorrectly calculated widths for specific rows.

To work around this issue, the ETL's fixed width handling can be modified to decode the text data using US-ASCII or ISO-8859-1, and split the line based on the width of fields in the schema mapping. This will allow all bytes of the multi-byte characters to count towards the field width, thus parsing the field width correctly.

To implement this change, modify the fixed width parsing Spark statement in [etl_collect_to_cleanse.py](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_collect_to_cleanse.py#L135) by adding a `decode()` function as shown.

* NOTE: Remember to add the `decode` function to the list of imports from `pyspark.sql.functions`.

```python
        initial_df = spark.read.text(source_path)
        initial_df = initial_df.select(
            [
                # Remove all encoding to calculate width, select column widths, then trim
                trim(decode(initial_df.value, 'US-ASCII').substr(
                    # Calculate start index based on all prior column widths
                    reduce(lambda a, b: a + int(b['width']), mapping_data[:index], 0) + 1,
                    int(field_data['width'])
                )).alias(field_data['destname'])
                    for index, field_data in enumerate(mapping_data)
                        if field_data['destname'].lower() != 'null'
            ]
        )
```

## Parquet

Parquet files are identified either by the input file extension `parquet` or by specifying `parquet` in the `input_spec` configuration (for Parquet files with varying extensions).

By default Parquet files will be read one at a time, each initiating a separate ETL pipeline execution, and no folder structure discovery will be performed. Refer to the next section, [Handling Multi-file Data Sets](#handling-multi-file-data-sets) for instructions on how to change this default behavior.

The following compression formats are supported transparently: uncompressed, snappy, gzip, lzo ([AWS Glue documentation reference](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-parquet-home.html#aws-glue-programming-etl-format-parquet-reference)).

### Handling Multi-file Data Sets

In the future, handling of multi-file Parquet data sources will be better integrated into InsuranceLake. The instructions in this section will detail how to modify the source code of the collect-to-cleanse PySpark Glue Job, and the etl-trigger Python Lambda function to support multi-file Parquet data sources.

Multi-file incoming data sets are often landed in the Collect S3 bucket in a nested folder structure. A nested folder structure for a Parquet data set could look similar to a partition override folder structure. For this reason you must comment out and disable the partition override support in the [etl-trigger Lambda function on line 127](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/state_machine_trigger/lambda_handler.py#L127) as follows:

```python
    # try:
    #     # Check if uploader has provided folders to override the default partitions
    #     p_year = path_components[2]
    #     p_month = path_components[3]
    #     p_day = path_components[4]
    # except IndexError:
    #     # Keep any defaults
    #     pass
```

To support multiple files that belong to the same dataset landed in the Collect bucket, we will expect one file to be added to the collection with a unique prefix that can be used to trigger the workflow; all other files will be ignored by the etl-trigger Lambda function.

Add the following code at [Line 123 of the same Lambda function](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/state_machine_trigger/lambda_handler.py#L123) to skip processing of all files except the "success" or "trigger" file. The code assumes the trigger filename will start with the `_` character.

```python
    if len(path_components) > 2 and not object_base_file_name.startswith('_'):
        logger.error(f'Ignoring individual files in folders beyond 2 levels (assuming collection): {object_full_path}')
        return {
            'statusCode': 400,
            'body': json.dumps('Received PutObject for individual file in collection; will not process')
        }
```

Lastly add the following code to the [collect-to-cleanse PySpark Glue job on Line 194](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_collect_to_cleanse.py#L194) to handle ETL executions initiated by the trigger file. Specifically, we want to load the entire folder in which the trigger file exists, and otherwise load individual files for all other executions.

```python
    elif ext.lower() == '.parquet' or ( 'parquet' in input_spec and args['base_file_name'].startswith('_') ):
        if args['base_file_name'].startswith('_'):
            initial_df = spark.read.format('parquet').load(args['source_bucket'] + '/' + args['source_path'] + '/')
        else:
            initial_df = spark.read.format('parquet').load(source_path)
```

After making these code modifications, you will need to ensure that the `parquet` format is specified in the `input_spec` section of the workflow's JSON configuration file. You will then be able to upload a folder of Parquet files inside the second level of folder structure with the Collect S3 bucket (in other words, you still must have folders representing the database and table name to use), and the ETL will initiate a single execution to process the entire uploaded folder.

## Microsoft Excel Format Support

InsuranceLake has been designed to support Microsoft Excel format input files using the OpenSource [Crealytics Spark Excel driver](https://github.com/crealytics/spark-excel).

### Obtaining the Driver

**Note: This library and the mentioned dependencies are not provided by AWS Glue or AWS; you must download the library and its dependencies from a 3rd party and install them in your environment.**

You can download the Spark Excel library from Crealytics at [MVN Repository](https://mvnrepository.com/artifact/com.crealytics/spark-excel).

The last version known to work with AWS Glue 4.0 is: [2.12 / 3.3.1_0.18.7](https://mvnrepository.com/artifact/com.crealytics/spark-excel_2.12/3.3.1_0.18.7)

The Spark Excel library also has dependencies that are not included with Glue by default. Specifically, you must also download the [Apache POI API Based On OPC and OOXML Schemas](https://mvnrepository.com/artifact/org.apache.poi/poi-ooxml) library and a newer version of [XML Beans](https://mvnrepository.com/artifact/org.apache.xmlbeans/xmlbeans).

The versions that are required for the Spark Excel version 3.3.1_0.18.7 are [poi-ooxml-5.2.3](https://mvnrepository.com/artifact/org.apache.poi/poi-ooxml/5.2.3) and [xmlbeans-5.1.1](https://mvnrepository.com/artifact/org.apache.xmlbeans/xmlbeans/5.1.1).

If you want to work with another version of Spark Excel, follow these steps to ensure AWS Glue compatibility:

1. Verify the version of Spark included with the Glue version [in the AWS documentation](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html).

2. Verify the version of Scala included with the Spark version either from [the Spark package dependencies](https://mvnrepository.com/artifact/org.apache.spark/spark-core) or [the Spark compatibility matrix](https://sparkbyexamples.com/spark/spark-versions-supportability-matrix/).

3. Download the version of Spark Excel corresponding to the desired Spark and Scala version. If the Spark version doesn't exist, find the closest match. For example, Glue 4.0 uses Spark 3.3.0 and Scala 2.12. The closest match from the pre-built Spark Excel packages is Spark 3.3.1 and Scala 2.12.

1. Check the Compile Dependencies of [the Spark Excel library](https://mvnrepository.com/artifact/com.crealytics/spark-excel_2.13/3.3.1_0.18.7), specifically looking for the Apache POI version, XML Beans and Apache Commons. Library version details included with Glue can be found [here](https://docs.aws.amazon.com/glue/latest/dg/migrating-version-40.html#migrating-version-40-appendix-dependencies). Take note of libraries missing from Glue or where a significantly newer version is needed than the version included with Glue. Typically, this is limited to Apache POI and XML Beans.

1. Download the required versions of the dependency libraries.

### Driver Installation

To include all Spark JAR packages in the Glue session, simply copy the JAR files to the `lib/glue_scripts/lib` folder and redeploy the Glue stack using CDK. The CDK Glue Stack will automatically identify the new libraries and include them in the `--extra-jars` parameter for all Glue jobs.

If you attempt to load an Excel file without installing the driver properly, you will see the following error:
```log
py4j.protocol.Py4JJavaError: An error occurred while calling o124.load.: java.lang.ClassNotFoundException: Failed to find data source: excel. Please find packages at https://spark.apache.org/third-party-projects.html
```

### Configuration

InsuranceLake recognizes Excel files by their extension. Currently the following two extensions (case insensative) are supported: `xls`, `xlsx`, `xlm`, `xlsm`. Regardless of your configuration, the source data file must have one of these extensions to be detected as Excel.

With no additional configuration, InsuranceLake will attempt to read tabular data from the 1st sheet in the workbook, starting at cell `A1`.

|Parameter  |Type   |Description    |
|---    |---    |---    |
|`sheet_names`  |optional   |A JSON list object of possible sheet names to try. Only one sheet is matched; the first match will be used. Sheets can be named or referenced by index. For referencing by index, specify the sheet number (starting from 0) as a string (for example, `"1"`). Default: `[ "0" ]`   |
|`data_address` |optional    |Either a Cell Reference representing the top left corner of the data, or a [Range Reference](https://www.techonthenet.com/excel/ranges/index.php) specifying a specific block of data. Default: `A1`   |
|`header`   |optional   |JSON boolean indicating whether the data has a header row or not. Default: `true`  |
|`password` |optional   |Password to use to unlock password-protected Excel workbooks. Default: none   |

Example configuration: 

```json
{
    "input_spec": {
        "excel": {
                "sheet_names": [ "Sheet1" ],
                "data_address": "A2",
                "header": true,
                "password": ""
        }
    }
}
```

**Note: To load multiple sheets of data in the workbook into multiple tables, simply copy the same Excel file to multiple Collect bucket folders, each with their own transformation spec file and Excel configuration (which references each sheet).**

### Notes on Excel Support

* Original date formats in Excel are not always visible in some versions of Excel. In other words, when viewing values of date fields in Excel, the date format you see on the screen may not be what is stored in the file. This is important if you are writing a `date` or `timestamp` transform spec and need to specify the correct date pattern. If you have an incorrect date format, symptoms include:
    * Null/empty date field values
    * An error similar to one of the following:
        ```log
        You may get a different result due to the upgrading of Spark 3.0
        Fail to parse 'YYYY-M-d' in the new parser. You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string.
        ```

        ```log
        Caused by: org.apache.spark.SparkUpgradeException: You may get a different result due to the upgrading of Spark 3.0: 
        writing dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z into Parquet
        files can be dangerous, as the files may be read by Spark 2.x or legacy versions of Hive
        later, which uses a legacy hybrid calendar that is different from Spark 3.0+'s Proleptic
        Gregorian calendar. See more details in SPARK-31404. You can set spark.sql.parquet.datetimeRebaseModeInWrite to 'LEGACY' to rebase the datetime values w.r.t. the calendar difference during writing, to get maximum
        interoperability. Or set spark.sql.parquet.datetimeRebaseModeInWrite to 'CORRECTED' to write the datetime values as it is,
        if you are 100% sure that the written files will only be read by Spark 3.0+ or other
        systems that use Proleptic Gregorian calendar.
        ```

    To avoid this issue, we recommend importing the Excel file first with no date conversion. Specifically:
    1. Import the Excel workbook with no `date` or `timestamp` transform
    1. Note the field type that Spark chooses for your date columns
    1. If the field is already a date or timestamp, it may mean that Spark was able to detect the date format and there is no need for a date transform. To confirm, ensure the date field values are correct.
    1. If the field is a string, note the format of the date field values and add a `date` or `timestamp` transform for the field.

* You may experience issues importing Excel workbooks with macros that are used to write cells containing data you want to import. The Spark Excel library is subject to the [limitations of the Apache POI project](https://poi.apache.org/components/spreadsheet/limitations.html).

* You may encoutner an error `Zip bomb detected` when reading some Excel workbooks. The error message will look similar to this:

    ```log
    py4j.protocol.Py4JJavaError: An error occurred while calling o37.load.
    : java.io.IOException: Zip bomb detected! The file would exceed the max. ratio of compressed file size to the size of the expanded data.
    This may indicate that the file is used to inflate memory usage and thus could pose a security risk.
    You can adjust this limit via ZipSecureFile.setMinInflateRatio() if you need to work with files which exceed this limit.
    Uncompressed size: 106496, Raw/compressed size: 859, ratio: 0.008066
    Limits: MIN_INFLATE_RATIO: 0.010000
    ```

    This is a security feature of the Apache POI library that tries to detect and block malicious Excel files. It has lead to false positives for many users of Spark Excel. You can read details and support a solution in the [Spark Excel Issue Tracker](https://github.com/crealytics/spark-excel/issues/231).

    Beacuse there is no method to access setMinInflateRatio() from pySpark, **we recommend working around the issue** by opening the Excel file, saving the desired tab in a separate file, and uploading the newly saved file into the Collect bucket.

* The Spark Excel driver has a file size limit by default. If you exceed this limit you will see an error message similar to this:
 
    ```log
    Caused by: org.apache.poi.util.RecordFormatException: Tried to allocate an array of length 322,589,970, but the maximum length for this record type is 100,000,000.
    If the file is not corrupt or large, please open an issue on bugzilla to request
    Increasing the maximum allowable size for this record type.
    As a temporary workaround, consider setting a higher override value with IOUtils.setByteArrayMaxOverride()
    ```

    You can correct this issue by increasing the [Apache POI ByteArrayMaxOverride](https://poi.apache.org/apidocs/5.0/org/apache/poi/util/IOUtils.html#setByteArrayMaxOverride-int-) value in the Spark Excel read options in [etl_collect_to_cleanse.py](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_collect_to_cleanse.py#L166).

    Example supporting up to 2 GB files:
    ```python
    initial_df = spark.read.format('excel') \
        .option('header', header) \
        .option('inferSchema', 'true') \
        .option('mode', 'PERMISSIVE') \
        .option("usePlainNumberFormat", 'true') \
        .option('dataAddress', f"'{sheet_name}'!{data_address}") \
        .option('workbookPassword', workbook_password) \
        .option("maxByteArraySize", 2147483647)
        .load(source_path)
    ```