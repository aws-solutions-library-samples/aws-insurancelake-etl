---
title: Schema Evolution
parent: User Documentation
nav_order: 6
last_modified_date: 2025-01-15
---
# InsuranceLake Schema Evolution Documentation
{: .no_toc }

This section describes the capabilities and considerations for managing schema evolution.


## Contents
{: .no_toc }

* TOC
{:toc}


## Overview

The InsuranceLake ETL provides capabilities to detect, control, and configure schema evolution using Data Catalog, Athena, Spark, Parquet, and Iceberg:

* [Schema detection and control using Data Catalog integration and configuration settings](#schema-change-setting)
* [Strict and non-strict schema mapping functionality](./schema_mapping.md#dropping-columns)
* [Data quality checks for column existence](./data_quality.md#configuration)
    * [ColumnExists](https://docs.aws.amazon.com/glue/latest/dg/dqdl-rule-types-ColumnExists.html)
    * [IsComplete](https://docs.aws.amazon.com/glue/latest/dg/dqdl-rule-types-IsComplete.html)
    * [Completeness](https://docs.aws.amazon.com/glue/latest/dg/dqdl-rule-types-Completeness.html)

Behavior and capabilities for allowing schema evolution vary depending on the data lake layer (Cleanse and Consume) and the table storage format.


## Schema Change Setting

The `allow_schema_change` setting for your workflow is defined in the `input-spec` section of the workflow's JSON configuration file. Details on this configuration file can be found in the [File Formats and Input Specification Documentation](file_formats.md#input-specification).

|Setting	|Behavior
|---	|---
|strict	|No schema change is permitted, including reordering of columns (case changes to field names are permitted because Data Catalog is case-insensitive)
|reorder	|Only reordering of fields in the schema is permitted
|evolve	|The following schema changes are permitted: reordering column, adding column, [changing data types](#evolve-setting-data-type-change-details); deleting columns is not permitted
|permissive	|All schema changes are permitted, even changes that could break schema merges in some table formats

If the `allow_schema_change` setting is not specified, the ETL defaults to a value based on the deployment environment:

|Environment	|Default Setting
|---	|---
|Dev	|permissive
|Test	|reorder
|Prod	|strict

To change the environment-based default behavior, modify the conditional expression starting on [Line 85 of the Collect-to-Cleanse AWS Glue job](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_collect_to_cleanse.py#L85).

To extend or change the specific schema change detection and control behavior, modify the `check_schema_change` function starting on [Line 88 of the glue_catalog_helpers module](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/lib/glue_catalog_helpers.py#L88).

### Evolve Setting Data Type Change Details

|Current data type   |Allowed new data types
|---    |---
|string  |byte, tinyint, smallint, int, bigint
|byte   |tinyint, smallint, int, bigint
|tinyint    |smallint, int, bigint
|smallint   |int, bigint
|int    |bigint
|float  |double
|date   |timestamp
|decimal    |decimal with larger precision or scale


## Cleanse Layer

By default, the Collect-to-Cleanse AWS Glue job writes data incrementally by partition to the Cleanse layer table in the Cleanse S3 bucket. The default partition strategy uses the Collect S3 Bucket object creation year, month, and day. When re-loading data for an existing partition, the Collect-to-Cleanse job **clears the specific partition from the Cleanse layer table in the Cleanse S3 bucket, and re-writes all the data** for each ETL pipeline execution.

When data is read from the Cleanse layer table by Spark or Athena, any schema differences will need to be merged. [Parquet/Hadoop schema change support will apply](#parquethadoop).


## Consume Layer

By default, the Cleanse-to-Consume AWS Glue job **clears the Consume layer table in the Consume S3 bucket and re-writes all data** in each ETL pipeline execution. This makes it easy to change the schema, but has the disadvantage of the table being unavailable to consumers while it is being re-written. For details on how this impacts Spark SQL and how to modify this behavior to address performance at scale, refer to the [Spark SQL section of the Using SQL documentation](./using_sql.md#spark-sql).

Because the entire Consume layer table is re-written, all types of schema changes are supported, including changing the partition layout. If you change this behavior to load data incrementally as described in the above link, [Parquet/Hadoop schema change support](#parquethadoop) will apply.


## Parquet/Hadoop

By default, the Collect-to-Cleanse and Cleanse-to-Consume InsuranceLake AWS Glue jobs create Parquet/Hadoop data tables in S3 and the Data Catalog. Athena and Spark support many types of schema changes for a Parquet/Hadoop table.

If an unsupported change is published to the data lake, queries across partitions with those schema differences will fail. You can work around this issue by selectively querying specific partitions, but some partitions will always trigger the schema merge failure. This is because Athena and Spark will always use the schema in the Data Catalog as the target for schema merges.

Tables created by the ETL use **read-by-name** by default. This means that reordering columns, adding columns in any location, and removing columns is supported; however, renaming a column in place is not supported. Renaming a column in place will be recognized as a column addition and a column removal at the same time.

When columns are missing from a partition, rows from that partition will have null values in the missing columns.

### Spark SQL

General information on Spark schema merging can be found in the [Apache Spark Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#schema-merging) documentation.

When data types change, Spark will attempt to coerce the partition data type to the target data type. These data type coercions sometimes generate an error and other times create null values in the data.

{: .important }
We recommend testing all schema changes before publishing data and using AWS Glue Data Quality rules to check for expected values and completeness in columns.

For example, suppose you have a partition with a field of `decimal(10,6)`, and a Data Catalog table definition of `decimal(9,6)`. Spark will attempt to coerce all values, and if some data for the field does not fit in `decimal(9,6)` (in other words, a value of the field has 10 significant digits), _no error will be raised_ and the field values that do not fit will be `null`. In contrast, Athena _will give an error_ when trying to merge the partitions if there are values that do not fit in the new precision and scale.

Details on Spark's data type coercion can be found in the [Apache Spark ANSI Compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#type-coercion) documentation.

An unsupported data type change will generate an error similar to the following example:
```log
org.apache.hadoop.hive.serde2.io.TimestampWritable cannot be cast to org.apache.hadoop.hive.serde2.io.DateWritable
```

### Athena SQL

Athena behaves similarly to Spark when merging partitions with different schemas. AWS documentation is available in the following locations:

* For Athena supported schema changes and an explanation of behavior, refer to the [Updates and data formats in Athena](https://docs.aws.amazon.com/athena/latest/ug/handling-schema-updates-chapter.html#summary-of-updates) documentation and [Updates in tables with partitions](https://docs.aws.amazon.com/athena/latest/ug/updates-and-partitions.html) documentation.

* For compatible data type changes refer to [Change a column data type](https://docs.aws.amazon.com/athena/latest/ug/updates-changing-column-type.html).

    {: .note }
    Some supported data type conversions are not listed, for example, changing decimal precision and scale.

An unsupported data type change will generate an error similar to the following examples:
```log
INVALID_CAST_ARGUMENT: Cannot cast DECIMAL(10, 2) '1494725.62' to DECIMAL(9, 3)
```

```log
HIVE_PARTITION_SCHEMA_MISMATCH: There is a mismatch between the table and partition schemas. The types are incompatible and cannot be coerced. The column 'startdate' in table 'syntheticgeneraldata.claimdata' is declared as type 'date', but partition 'year=2024/month=05/day=10' declared column 'startdate' as type 'timestamp'.
```

## Apache Iceberg

Apache Iceberg tables created by the ETL entity-match AWS Glue job support the following types of schema change:
* add
* drop
* rename
* update
* reorder
* changing the partition layout

Iceberg schema evolution support is independent and free of side-effects without rewriting files. For complete details, refer to the [Iceberg Schema Evolution](https://iceberg.apache.org/docs/latest/evolution/) documentation.