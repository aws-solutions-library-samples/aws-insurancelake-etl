# InsuranceLake Collect-to-Cleanse Transform Index

|Name	|Description	|
|---	|---	|
|[currency](#currency)	|Convert specified numeric field with currency formatting to Decimal (fixed precision)	|
|[changetype](#changetype)	|Convert specified fields to decimal (fixed precision), int, bigint, string, etc.	|
|[date](#date)	|Convert specified date fields to ISO format based on known input format	|
|[implieddecimal](#implieddecimal)	|Convert specified numeric fields to Decimal (fixed precision) type with implied decimal point support (i.e. last 2 digits are to the right of decimal)	|
|[timestamp](#timestamp)	|Convert specified date/time fields to ISO format based on known input format	|
|[addcolumns](#addcolumns)	|Add two or more columns together in a new or existing column	|
|[columnfromcolumn](#columnfromcolumn)	|Add or replace column in DataFrame based on regexp group match pattern	|
|[columnreplace](#columnreplace)	|Add or replace a column in DataFrame with regex substitution on an existing column	|
|[combinecolumns](#combinecolumns)	|Add column to DataFrame using format string and source columns	|
|[filename](#filename)	|Add column in DataFrame based on regexp group match pattern on the filename argument to the Glue job	|
|[filldown](#filldown)  |Fill starting column value down the columns for all null values until the next non-null    |
|[filterrows](#filterrows)	|Filter out rows based on standard SQL WHERE statement	|
|[flipsign](#flipsign)	|Flip the sign of a numeric column in a Spark DataFrame, optionally in a new column	|
|[literal](#literal)	|Add column to DataFrame with static/literal value supplied in specification	|
|[lookup](#lookup)	|Replace specified column values with values looked up from an external table	|
|[merge](#merge)	|Merge columns using coalesce	|
|[multilookup](#multilookup)	|Add columns looked up from an external table using multiple conditions, returning any number of attributes	|
|[multiplycolumns](#multiplycolumns)	|Multiply two or more columns together in a new or existing column	|
|[titlecase](#titlecase)	|Convert specified string column in DataFrame to title or proper case	|
|[hash](#hash)	|Hash specified column values using SHA256	|
|[redact](#redact)	|Redact specified column values using supplied redaction string	|
|[tokenize](#tokenize)	|Replace specified column values with hash and store original value in DynamoDB table	|
|[earnedpremium](#earnedpremium)	|Calculate monthly earned premium	|
|[enddate](#enddate)	|Add a number of months to a specified date to get an ending/expiration date	|
|[expandpolicymonths](#expandpolicymonths)	|Expand dataset to one row for each month the policy is active with a calculated earned premium	|
|[policymonths](#policymonths)	|Calculate number of months between policy start/end dates	|

# Using Transforms

- The order that you enter the transforms into the json file is important, and should be chosen deliberately. Each transform is executed in the order they are defined on the incoming dataset starting from the beginning of the transform_spec section of the file.

- Each transform type can only be used once in the transform specification. This limitation helps the transform implementation reduce the number of Spark withColumn statements (using withColumns or select), and [optimize the workflow performance](https://medium.com/@manuzhang/the-hidden-cost-of-spark-withcolumn-8ffea517c015).

- Except where noted, transforms will overwrite an existing field if specified as the result field. Where available use the ```source``` parameter to indicate that a different column should be used as the source data, and the column specified in the ```field``` parameter should be used for the result value. Specifying a source field to create a new column for transforms is useful for debugging issues with a transform, preserving original data, or having a backup datapoint available when incoming data formats are less clear.

---

## Formatting

### currency
Convert specified numeric field with currnecy formatting to Decimal (fixed precision)

- ```format``` parameter defaults to 16,2 if not specified

```json
"currency": [
    {
        "field": "SmallDollars",
        "format": "6,2"
    },
    {
        "field": "EuroValue",
        "source": "EuroValueFormatted",
        "euro": true
    }
]
```

### changetype
Convert specified fields to decimal (fixed precision), int, bigint, string, etc.

Field type syntax follows the [Spark simpleString](https://spark.apache.org/docs/3.3.0/api/python/_modules/pyspark/sql/types.html) definitions

```json
"changetype": [
    {
        "ExpiringPremiumAmount": "decimal(10,2)",
        "WrittenPremiumAmount": "decimal(10,2)",
        "EarnedPremium": "decimal(10,2)",
        "PrimaryKeyId": "bigint",
        "GrowingCount": "bigint",
        "PolicyKey": "string"
    }
]
```

### date
Convert specified date fields to ISO format based on known input format

Date formats use [Spark datetime patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html).

```json
"date": [
    {
        "field": "StartDate",
        "format": "M/d/yy"
    },
    {
        "field": "EndDate",
        "format": "yy-MM-dd"
    },
    {
        "field": "valuationdate",
        "source": "valuationdatestring",
        "format": "yyyyMMdd"
    }
]
```

### implieddecimal
Convert specified numeric fields to Decimal (fixed precision) type with implied decimal point support (i.e. last 2 digits are to the right of decimal)

- Use this transform to interpret decimal precision data stored in integer format, common in mainframe or flat file data formats.

```json
"implieddecimal": [
    {
        "field": "indemnity_paid_current_period",
        "num_implied": "4",
        "format": "16,4"
    },
    {
        "field": "claim_amount",
        "source": "claim_amount_string",
        "format": "16,2"
    }
]
```

### timestamp
Convert specified date/time fields to ISO format based on known input format

Timestamp formats use [Spark datetime patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html).

```json
"timestamp": [
    {
        "field": "GenerationDate",
        "format": "yyyy-MM-dd HH:mm:ss.SSS+0000"
    },
    {
        "field": "DataLoadTimestamp",
        "source": "DataLoadString",
        "format": "yyyy-MM-dd HH:mm:ss.SSSZ"
    }
]
```

### titlecase
Convert specified string field to title or proper case
```json
"titlecase": [
    "CompanyName",
    "AddressStreet"
]
```

---

## Data Manipulation

### addcolumns
Add two or more columns together in a new column
```json
"addcolumns": [
    {
        "field": "TotalWrittenPremium",
        "source_columns": [ "WrittenPremiumAmount" ]
    }
]
```

### columnfromcolumn
Add or replace column in DataFrame based on regexp group match pattern

- Only 1 (the first) match group will be used per specification block. For multiple groups, use multiple specification blocks and shift the parenthesis.

- Uses [Spark regexp_extract function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_extract.html).

- Regular expressions follow the [Java Pattern syntax](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/regex/Pattern.html).

```json
"columnfromcolumn": [
    {
        "field": "username",
        "source": "emailaddress",
        "pattern": "(\\S+)@\\S+"
    },
    {
        "field": "policyyear",
        "source": "policyeffectivedate",
        "pattern": "(\\d\\d\\d\\d)/\\d\\d/\\d\\d"
    }
]
```

### columnreplace
Add or replace a column in DataFrame with regex substitution on an existing column

- Uses [Spark regexp_replace function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.regexp_replace.html).

- Regular expressions follow the [Java Pattern syntax](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/regex/Pattern.html).

```json
"columnreplace": [
    {
        "field": "clean_date_field",
        "source": "bad_date_field",
        "pattern": "0000-00-00",
        "replacement": ""
    },
    {
        "field": "field_with_extra_data",
        "pattern": "[a-zA-z]{3,5}",
        "replacement": ""
    }
]
```

### combinecolumns
Add column to DataFrame using format string and source columns

- Uses [Python format string syntax](https://docs.python.org/3/library/string.html#format-string-syntax). Keyword arguments are not supported. Implicit references, positional references, and the [format specification mini-language](https://docs.python.org/3/library/string.html#format-specification-mini-language) are supported.

```json
"combinecolumns": [
    {
        "field": "RowKey",
        "format": "{}-{}-{}",
        "source_columns": [ "LOBCode", "PolicyNumber", "StartDate" ]
    }
]
```

### filename
Add column in DataFrame based on regexp group match pattern on the filename argument to the Glue job

- Regular expression group matching syntax follows the [Python regular expression syntax](https://docs.python.org/3/library/re.html)

- Only one (the first) match group will be used per specification block. For multiple groups, use multiple specification blocks and shift the parenthesis.

- Use the ```required``` parameter to optionally halt the workflow if the pattern is not matched. Without ```required```, an unmatched group will be added as a null value string column.

```json
"filename": [
    {
        "field": "valuationdate",
        "pattern": "\\S+-(\\d{8})\\.csv",
        "required": true
    },
    {
        "field": "program",
        "pattern": "([A-Za-z0-9]+)\\S+\\.csv",
        "required": true
    }
]
```

### filldown
Fill starting column value down the columns for all null values until the next non-null

- This is a Spark implementation of [Pandas ffill](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.ffill.html) based on the article [How to utilize nested tables and window functions to fill down over null values by Robert O'Brien](https://towardsdatascience.com/tips-and-tricks-how-to-fill-null-values-in-sql-4fccb249df6f)

- This function is useful for replacing null values created by pivot tables in Excel that have category headers inline with only the first row of data. This will normalize the data ensuring that the headers are on all rows.

- Specify ```sort``` to order the data prior to filling down. Note that this will change the sort order of the data for subsequent transforms.

```json
"filldown": [
    {
        "field": "category"
    },
    {
        "field": "subcategory",
        "sort": "timestamp"
    }
]
```

### filterrows
Filter out rows based on standard SQL WHERE statement

- Follows the [Spark WHERE clause syntax](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-where.html).

- Use only when certain rows can be systematically and confidently discarded. Examples of usage include removing blank rows, removing a totals rows, or removing subtotal rows. If review of filtered data is desired, consider using [data quality quarantine rules](./data_quality.md).

```json
"filterrows": [
    {
        "condition": "claim_number is not null or file_number is not null"
    },
    {
        "condition": "`startdate` >= cast('1970-01-01' as date)"
    }
]
```        

### flipsign
Flip the sign of a numeric column in a Spark DataFrame, optionally in a new column 
```json
"flipsign": [
    {
        "field": "Balance"
    },
    {
        "field": "NewAccountBalance",
        "source": "AccountBalance"
    }
]
```

### literal
Add column to DataFrame with static/literal value supplied in specification 

- Note that the transform is defined using a dictionary of field/value pairs, not a list.

```json
"literal": {
    "source": "syntheticdata",
    "line_of_business": "synthetic"
}
```

### lookup
Replace specified column values with values looked up from an dynamodb table
- Future: use / enhance existing DynamoDB load python code (located here?)
- Future: add a copy between Dev-Test-Prod in DevSecOps Process
```json
"lookup": [
    {
        "field": "smokingclass",
        "lookup": "smokingclass"
    },
    {
        "field": "issuestatename",
        "source": "issuestate",
        "lookup": "StateCd",
        "nomatch": "N/A"
    }
]
```

### merge
Merge column values using coalesce 

- Optionally specify a literal default value to use if all specified columns have a null value. Defaults to null.

```json
"merge": [
    {
        "field": "insuredstatemerge",
        "source_list": [
            "insuredstatename", "insuredstatecode"
        ],
        "default": "Unknown"
    }
]
```

### multilookup
Add columns looked up from an external table using multiple conditions, returning any number of attributes
To setup a multilookup transform, begin by preparing the lookup data. The data should be saved as CSV and include all the match columns and return value columns. It is ok to have some columns that are not used, because the transform specification allows the user to select the specific return columns.
- Future: use / enhance existing DynamoDB load python code
```bash
./load_dynamodb_multilookup_table.py dev-insurancelake-etl-multi-lookup lookups.csv PolicyData-LOBCoverage originalprogram originalcoverage
```
- Future: add a copy between Dev-Test-Prod in DevSecOps Process

Use the included loading script in the resources directory to import the CSV data into etl-multi-lookup DynamoDB table:

Usage: load_dynamodb_multilookup_table.py [-h] table_name data_file lookup_group lookup_columns [lookup_columns ...]
The following arguments are required: table_name, data_file, lookup_group, lookup_columns
- table_name indicates the name of the DynamoDB table deployed by the InsuranceLake CDK stack for multi-lookups, in the form <environment>-<resource prefix>-etl-multi-lookup. All multilookup lookup datasets are stored in the same table and grouped by lookup_group.
-  lookup_group can be any name that is meaninginful to the user and will be specified in the transform spec.
- lookup_columns are listed as parameters last, separated by spaces. At least one lookup column is required.

Use the AWS Console for the DynamoDB service to confirm that the data is loaded correctly. Note that the lookup columns will be concatenated with a hyphen (-) separator and stored as a single string in the sort key. All return columns will be stored as separate attributes. This is important to understand when editing the data in the future.

Now insert the multilookup specification into your dataset’s transformation spec file (in the transform_spec section). An example follows:

```json
"multilookup": [
    {
        "lookup_group": "LOBCoverage",
        "match_columns": [
            "program",
            "coverage"
        ],
        "return_attributes": [
            "coveragenormalized",
            "lob"
        ],
        "nomatch": "N/A"
    }
]
```

- The lookup_group string should match the lookup_group string you used to load the data in DynamoDB.
- match_columns indicates the columns in your incoming data set that must have matching values in the lookup data. Note that the column values only refer to the incoming dataset. The column names in your lookup data (in DynamoDB) do not matter, because all the lookup column values are stored in a concatenated string in the lookup_item sort key.
- return_attributes specifies the attribute names in the DynamoDB lookup table to add to the incoming dataset.

Important Note: if a column already exists, a duplicate column will be created, which will raise an error when saving to Parquet format. Take care to map your incoming dataset correctly so that it has unique column names after performing the multilookup transform. For example. suppose your incoming data has a lineofbusiness column, but it is composed of bespoke values that you want to normalize. Best practice would be to map lineofbusiness to the name originallineofbusiness so the incoming data is preserved, and use the multilookup to return a new (normalized) lineofbusiness attribute value.

### multiplycolumns
Multiply two or more columns together in a new or existing column

- Use for calculating premium splits
- ```empty_value``` parameter is optional and defaults to a value of 1.

```json
"multiplycolumns": [
    {
        "field": "SplitPremium",
        "source_columns": [ "WrittenPremiumAmount", "SplitPercent1", "SplitPercent2" ],
        "empty_value": 0
    }
]
```

---

## Data Security

### hash
Hash specified column values using SHA256
```json
"hash": [
    "InsuredContactCellPhone",
    "InsuredContactEmail"
]
```

### redact
Redact specified column values using supplied redaction string

- Note that the transform is defined using a dictionary of field/value pairs, not a list.

```json
"redact": {
    "CustomerNo": "****"
}
```

### tokenize
Replace specified column values with hash and store original value in DynamoDB table

- The ```<environment>-insurancelake-etl-hash-values``` DynamoDB table will be used for storage of all tokens. Since the hashing is deterministic, each value will only be stored once, regardless of how many columns contain the value.

```json
"tokenize": [
    "EIN"
]
```

---

## Earned Premium

### earnedpremium
Calculate monthly earned premium

- Transform requires four dates fields and one or more written premium fields as inputs. If any of the date inputs have null values, the earned premium will be null. Null written premium values are treated as 0.

- The `period_start_date` and `period_end_date` parameters indicate the date columns for determining the earned premium calculation period for each row of data. These are usually the first and last day of the month, and are created by the [expandpolicymonths](#expandpolicymonths) transform.

- The ```byday``` parameter selects the calculation method. If true, earned premium will be proportional to the number of days in the reporting period. If false, earned premium will be divided evenly across all active policy months.

- For the `policy_effective_date` and ```policy_expiration_date``` parameters indicate the date columns for determining the lifetime of the policy. If ```byday``` is true, they will be used to pro-rate the earned premium for the first and last months of the policy. If ```byday``` is false, they will be used to identify the number of active policy months (always a whole number).

```json
"earnedpremium": [
    {
        "field": "CalcEarnedPremium",
        "written_premium_list": [
                "WrittenPremiumAmount"
        ],
        "policy_effective_date": "EffectiveDate",
        "policy_expiration_date": "ExpirationDate",
        "period_start_date": "StartDate",
        "period_end_date": "EndDate",
        "byday": true
    }
]
```

### enddate
Add a number of months to a specified date to get an ending/expiration date
```json
"enddate": [
    {
        "field": "CalcExpirationDate",
        "start_date": "EffectiveDate",
        "num_months": "Term"
    }
]
```

### expandpolicymonths
Expand dataset to one row for each month the policy is active with a calculated earned premium

- Use this transform to convert a list of insurance policies (one row per policy) to a list of active policy months (one row per month per policy). This transform will change the shape/size of the input data. However, it is possible to recover the original number of rows using a simple WHERE statement (e.g. ```WHERE PolicyMonthIndex = 1```).

- Transform requires two date field parameters as inputs: ```policy_effective_date``` and ```policy_expiration_date```. If either of the date field values are null, the policy row will not be expanded to any additional rows and will have null values for the policy month fields.

- Based on the ```policy_month_start_field```, ```policy_month_end_field```, and ```policy_month_index``` parameters, the transform will add colums to each row containing the first day of the month, the last day of the month, and the policy month number.

- Optionally use the ```uniqueid``` parameter to specify a field name to add with a generated GUID, unique to each policy.

```json
"expandpolicymonths": {
    "policy_effective_date": "EffectiveDate",
    "policy_expiration_date": "ExpirationDate",
    "uniqueid": "generated_policy_number",
    "policy_month_start_field": "StartDate",
    "policy_month_end_field": "EndDate",
    "policy_month_index": "PolicyMonthIndex"
}
```

### policymonths
Calculate number of months between policy start/end dates

- Optionally specify the ```normalized``` parameter as ```true``` to always return a whole number of months. If ommited ```normalized``` defaults to ```false``` and the number of months returned is a fractional number based on the exact number of days between the effective and expiration dates.

```json
"policymonths": [
    {
        "field": "CalcNumMonths",
        "policy_effective_date": "EffectiveDate",
        "policy_expiration_date": "ExpirationDate",
        "normalized": true
    }
]
```