# InsuranceLake Schema Mapping Documentation

Schema mapping in InsuranceLake is configured by using a comma-delimited text file to describe the source and destination field names.

The filename of the schema mapping configuration file follows the convention of `<database name>-<table name>.csv` and is stored in the `/etl/transformation-spec` folder in the `etl-scripts` bucket. When using CDK for deployment, the contents of the `/lib/glue_scripts/lib/transformation-spec` directory will be automatically deployed to this location.

Your schema mapping file should map field names to Parquet and Athena-friendly names. Specifically, this means removing or replacing the invalid characters: `,;{}()\n\r\t=`. We also recommend removing/replacing characters that are valid but require escaping: ` .:[]`.

Field names in the schema mapping file are automatically escaped in Spark using surrounding backtick characters `` ` `` so that all field name characters are considered part of the name. See [Mapping Nested Data](#mapping-nested-data) for cases when you want to override the escape characters with your own escaping.

The schema mapping operation is accomplishing using [Spark DataFrame's alias operation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.alias.html).

## Contents

* [Behavior When There is No Schema Mapping](#behavior-when-there-is-no-schema-mapping)
* [Basic Mapping File Layout (CSV)](#basic-mapping-file-layout-csv)
* [Dropping Columns](#dropping-columns)
* [Files with No Header](#files-with-no-header)
* [Duplicate Columns](#duplicate-columns)
* [Fuzzy Matching Column Names](#fuzzy-matching-column-names)
* [Handling Line Breaks in Column Headers](#handling-line-breaks-in-column-headers)
    * [Exact Matching](#exact-matching)
    * [Fuzzy Matching](#fuzzy-matching)
* [Fixed Width File Format](#fixed-width-file-format)
* [Mapping Nested Data](#mapping-nested-data)


## Behavior When There is No Schema Mapping

When there is no schema mapping file or an empty schema mapping file in the etl-scripts bucket for the workflow, the ETL will clean all column names so that they can be saved successfully in Parquet format. The ETL will also save a *recommended* mapping file to the Glue Temp bucket,  `<environment>-insurancelake-<account>-<region>-glue-temp` bucket, in the folder `/etl/collect_to_cleanse` following the naming convention `<database>-<table>.csv`.

When this behavior occurs, you will see the following log message in the Glue Job Output Logs:
```log
No mapping found, generated recommended mapping to: s3://...
```

This recommended mapping file can be used as a starting point to build your own mapping file, and will help ensure that you have valid column names.


## Basic Mapping File Layout (CSV)

All schema mapping configuration files must contain a header row. The order of columns in the mapping file does not matter, but the name of each column must match the specification below.

|Field	|Type	|Description
|---    |---    |---
|SourceName	|required	|The exact name of the column (spaces and special characters included) in the source file
|DestName	|required	|The desired Spark/Parquet-conforming name of the column; use `null` to explicitly drop the field
|Description	|optional	|Example extra field to use for describing the column or mapping; this value will be ignored, as will any other field not specifically identified in this documentation

If commas are included in the field names, you must surround the field name with quotes `"` following the CSV standard. Similarly, if quotes are in the field names, you must use the doublequotes escape technique (in other words `""`). Other special characters such as spaces can be simply included in the `SourceName` field as they appear in the source data.

Example:
```csv
SourceName,DestName
partyid,customerno
party type,partytype
firstname,firstname
lastname,lastname
""nickname"",Null
"city, state",city_state
 Loss Paid,loss_paid
 Exp Paid,exp_paid
 Fee Paid,fee_paid
Original Claim Currency ,original_claim_currency
```


## Dropping Columns

Fields can be dropped using the schema mapping configuration in two ways:
1. Indicating `Null` (case insensitive) as the DestName (explicitely drop the field)
1. Omiting the field from the schema mapping (implicitly drop the field)

How the ETL behaves in each scenario changes based on the [`strict_schema_mapping` setting](./file_formats.md#input-specification) in the transformation-spec. Refer to the following chart to understand how the ETL handles all the combinations of dropping a field and strict schema mapping:

|Source file    |Mapping file   |Strict Schema Mapping  |Behavior
|---    |---    |---    |---
Field exists    |Field mapped normally  |false  |Field is included in Cleanse bucket, data pipeline succeeds
Field missing   |Field mapped normally  |false  |Field is not included in Cleanse bucket, data pipeline succeeds
Field exists    |Field mapped normally  |true  |Field is included in Cleanse bucket, data pipeline succeeds
Field missing   |Field mapped normally  |true  |Data pipeline fails with schema mapping error
Field exists    |Field mapped to `Null`  |false  |Field is not included in Cleanse bucket, data pipeline succeeds
Field missing   |Field mapped to `Null`  |false  |Field is not included in Cleanse bucket, data pipeline succeeds
Field exists    |Field mapped to `Null`  |true  |Field is not included in Cleanse bucket, data pipeline succeeds
Field missing   |Field mapped to `Null`  |true  |Data pipeline fails with schema mapping error


## Files with No Header

If a file format is configured with `header: false`, the column names will be enumerated by Spark using the prefix `_c` with a number representing the column position in the schema, **starting with 0**.

For files with no header, the ETL will not be aware if columns change order in the source file schema. We recommend using [data quality rules](./data_quality.md) to check field types to catch this situation.

Example:
```csv
SourceName,DestName
_c0,line_number
_c1,description
_c2,amount
```


## Duplicate Columns

If a file contains columns that have the same name, Spark will automatically rename them to unique names by numbering every field name that has a duplicate with a number representing the column position in the schema, **starting with 0**. All occurences, including the first, will be numbered.

For files with duplicate columns, the ETL will not be aware if those duplicate columns change location in the source file schema. We recommend using [data quality rules](./data_quality.md) to check field types to catch this situation.

Example:
```csv
SourceName,DestName
Claim Note,claim_note
Claim AMT1,claim_amt
Country2,country2
In Suit,in_suit
Trial Date,trial_date
Country5,country5
Claim AMT6,Null
```


## Fuzzy Matching Column Names

Fuzzy matching of column names for schema mapping is implemented in InsuranceLake using [the RapidFuzz Python library](https://github.com/rapidfuzz/RapidFuzz).

To use fuzzy matching in your schema mapping file, add the columns `Threshold` and `Scorer`. These columns do not need to be defined for every column in the schema mapping, only for the columns where you want to use fuzzy matching to map schema.

Fuzzy matching is always performed after direct schema mapping of fields. Only the fields from the source file that were not directly mapped are used for fuzzy matching. When a field is successfully fuzzy matched, it is removed from the available fields for subsequent fuzzy matching. This strategy increases the chances you will get the matches you are expecting.

Full documentation of the RapidFuzz library is available on [RapidFuzz Github Pages](https://rapidfuzz.github.io/RapidFuzz/Usage/fuzz.html).

|Field	|Type	|Description
|---    |---    |---
|SourceName	|required	|Text the will be used to fuzzy match columns in the source file
|DestName	|required	|Desired Spark/Parquet-conforming name of the column
|Threshold  |required   |Fuzzy matching score that must be met in order to constitute a match for schema mapping
|Scorer |required   |Fuzzy matching scorer algorithm to use for matching, exactly how it is defined in the RapidFuzz library, case sensitive. Options include `ratio`, `partial_ratio`, `token_sort_ratio`, `token_set_ratio`, `WRatio`, `QRatio`. A description of scorers with examples is available in [the RapidFuzz usage documentation](https://github.com/rapidfuzz/RapidFuzz?tab=readme-ov-file#usage).

Example schema mapping file with fuzzy matching for the `NewOrRenewal` column:
```csv
SourceName,DestName,Threshold,Scorer
PolicyNumber,PolicyNumber
EffectiveDate,EffectiveDate
ExpirationDate,ExpirationDate
NewRenewal,NewOrRenewal,90,ratio
```


## Handling Line Breaks in Column Headers

The InsuranceLake ETL can handle mapping columns with line breaks using exact matching or fuzzy matching.

Consider a data source with the following column headers:

|Date<br>Closed    |Vehicle<br>Make   |Vehicle<br>Model  |Claim/Incident/<br>Report
|---    |---    |---    |---
|   |   |   |

### Exact Matching

Using double-quotes around `SourceName` values allows you to specify line breaks in the CSV mapping file. This method performs an exact match. Specifically this method requires you to match your line break characters exactly in the mapping configuration. If the data sources columns have varying line breaks, this can be a challenge. Consider using [fuzzy matching](#fuzzy-matching) for complex or changing column headers.

Example exact matching to handle line breaks:

```csv
SourceName,DestName
"Date
Closed",dateclosed
"Vehicle
Make",vehiclemake
"Vehicle
Model",vehiclemodel
"Claim/Incident/
Report",claim_incident_report
```

### Fuzzy Matching

Fuzzy matching can be used to match mixed line breaks (Unix, Windows, legacy MacOS) with a high degree of accuracy. Use the ratio scorer with a 90 - 95 threshold, and treat each line break as a space (or other clear separator) in the `SourceName`.

Example fuzzy matching to handle line breaks:

```csv
SourceName,DestName,Threshold,Scorer
Date Closed,dateclosed,95,ratio
Vehicle Make,vehiclemake,95,ratio
Vehicle Model,vehiclemodel,95,ratio
Claim/Incident/Report,claim_incident_report,95,ratio
```


## Fixed Width File Format

Fixed width data sources require a `fixed` JSON object in the `input_schema`section of the transformation spec. See the [file formats documentation](./file_formats.md#fixed-width) for more information.

|Field	|Type	|Description
|---    |---    |---
|SourceName	|required	|Placeholder value that is ignored
|DestName	|required	|The desired Spark/Parquet-conforming name of the column
|Description	|optional	|Example extra field to use for describing the column or mapping; this value will be ignored, as will any other field not specifically identified in this documentation
|Width  |required   |Width of the field in characters, positive integer only

- No start and end position is needed for fields, because **the fields are defined in the order they appear in the file**, and the width of each field is used to calculate the start position of the next field.

- No fuzzy matching of columns is available for fixed width files because the fixed width format does not use column headers. There is no data source field name.

Example:
```csv
SourceName,DestName,Width
record_type,record_type,3
filler1,null,5
claim_number,claim_number,18
client_id,client_id,4
```


## Mapping Nested Data

The schema mapping configuration can be used to flatten nested data structures.

Fuzzy matching is available for nested data and will match on the fully expressed nested entity names (in other words, `contacts.role.name`)

**The schema mapping file cannot be used to rename/map fields in place within nested data structures. When no mapping file is present, the recommended schema mapping file will only contain the first level of the nested data structure. This behavior reflects the ability of Spark's alias function; it can only rename columns in the first level.**

See [changetype documentation](./transforms.md#changetype) for a different method to rename fields in place within a nested data structure.

**When referencing elements within an array structure, it is not necessary to specify the element index. Simply use dot notation to reference any fields within the array.**

Example schema mapping that flattens and renames a nested data structure:
```csv
SourceName,DestName
contacts,null
`contacts`.`cc:303`.`roles`.`role`.`name`,contacts_303_main_role
`contacts`.`cc:302`,contacts_302
`contacts`.`cc:303`,contacts_303
```

Works with the following structure:
```json
"contacts": {
    "cc:303": {
        "name": "Sara Smith",
        "roles": [
            {
                "role": {
                    "name": "Main Contact"
                }
            }
        ]
    },
    "cc:302": {
        "name": "John Smith",
        "roles": [
            {
                "role": {
                    "name": "Secondary Contact"
                }
            }
        ]
    }
}
```