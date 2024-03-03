## How to Manage Data Quality

### Getting Started

We recommend the following approach for building and testing new data quality rules:

1. Load your data using InsuranceLake first
2. Create a temporary Glue job in the Visual Editor
3. Select the cleanse or consume buckets as an input source
4. Add a Data Quality node
5. Use the function browser to build your rules
6. Copy and paste the rules into a JSON editor

### Configuration

Data quality in InsuranceLake is provided using Glue Data Quality rules managed in a per-workflow JSON configuration file. 

The filename of the data quality rules configuration file follows the convention of `dq-<database name>-<table name>.json` and is stored in the `/etl/dq-rules` folder in the `etl-scripts` bucket. When using CDK for deployment, the contents of `/lib/glue_scripts/lib/dq-rules` directory will be automatically deployed to this location.

The rules have the same format as Glue Data Quality rules created using the visual editor (except for the JSON requirements around double quotes).

InsuranceLake data quality configuration supports three locations in the data pipeline to enforce quality rules:

* Before Transforms: `before_transform`
    * Rules are enforced immediately after schema mapping is completed, but before any transforms are run, including the addition of partition and execution ID columns. This stage is within the Collect-to-Cleanse Glue Job run.
    * Quarantined data is published in the Cleanse database with a table name of `<table name>_quarantine_before_transforms`.
    * Quarantined data is stored in S3 in the Cleanse bucket in the folder `/quarantine/before_transforms/<database name>/<table name>`.
    * This is a good pipeline location to check for date formats, currency formats, or other field values that the transforms will expect.
* After Transforms: `after_transform`
    * Rules are enforced after all transforms have been run. This stage is just before saving the data to the Cleanse bucket, during the Collect-to-Cleanse Glue Job run.
    * Quarantined data is published in the Cleanse database with a table name of `<table name>_quarantine_after_transforms`.
    * Quarantined data is stored in S3 in the Cleanse bucket in the folder `/quarantine/after_transforms/<database name>/<table name>`.
    * This is a good pipeline location to compare date values, check calculations, and to create a quarantine that looks similar to the cleanse bucket schema.
* After Spark SQL: `after_sparksql`
    * Rules are enforced after running the Spark SQL command for the workflow, if, and only if, there is one present. This stage is run during the Cleanse-to-Consume Glue Job run before the Athena SQL is run.
    * Rules in this stage can reference multiple tables if those tables are joined or unioned in the Spark SQL. The `primary` table is still the only one available to reference.
    * Quarantined data is published in the Consume database, `<database name>_consume`, with a table name of `<table name>_quarantine_after_sparksql`.
    * Quarantined data is stored in S3 in the Cleanse bucket in the folder `/quarantine/after_sparksql/<database name>/<table name>`.
    * This is a good pipeline location to compare field values from multiple tables that are joined in the Spark SQL.


Keep in mind that the schema of your data at each of the above stages will likely be different. Ensure you are using the right field names and assumptions around the data at each stage of the pipeline.

The schema of the quarantined data will reflect the schema of the data at the respective stage of the data pipeline. To handle these schema differences, quarantined data will be published in different tables and locations in S3.

InsuranceLake data quality configuration supports three types of actions to take when rules fail. All three actions are available for all three locations in the pipeline.

* Warn: `warn_rules`
    - A data quality rule failure message is logged to the Glue job output log. 
* Quarantine: `quarantine_rules`
    - Individual rows that fail the quality check are removed from the incoming data and stored in a separate quarantine storage location and Glue Catalog table. The data quality expression must be able to evaluate individual rows.
* Halt: `halt_rules`
    - The Glue job is halted immediately. This action is run last so that the other rules are always evaluated.


Example data quality configuration file:

```json
{
    "before_transform": {
        "quarantine_rules": [
            "ColumnValues 'StartDate' matches '\\d{1,2}/\\d{1,2}/\\d\\d'",
            "ColumnValues 'EndDate' matches '\\d\\d-\\d{1,2}-\\d{1,2}'",
            "ColumnDataType 'EffectiveDate' = 'DATE'",
            "ColumnDataType 'ExpirationDate' = 'DATE'",
            "ColumnDataType 'GenerationDate' = 'TIMESTAMP'"
        ]
    },
    "after_transform": {
        "warn_rules": [
            "Completeness 'EarnedPremium' > 0.80",
            "ColumnValues \"WrittenPremiumAmount\" >= 0",
            "ColumnValues 'WrittenPremiumAmount' < 10000000",
            "ColumnValues 'NewOrRenewal' in [ 'New', 'Renewal' ]"
        ],
        "quarantine_rules": [
            "ColumnValues 'WrittenPremiumAmount' <= 1500000"
        ],
        "halt_rules": [
            "(ColumnExists 'StartDate') and (IsComplete 'StartDate')",
            "(ColumnExists 'PolicyNumber') and (IsComplete 'PolicyNumber')",
            "CustomSql 'SELECT COUNT(*) FROM primary WHERE EffectiveDate > ExpirationDate' = 0"
        ]
    },
    "after_sparksql": {
        "quarantine_rules": [
            "CustomSql 'SELECT PolicyNumber FROM primary WHERE accidentyeartotalincurredamount <= earnedpremium'"
        ]
    }
}
```

### CustomSQL Notes

* Glue Data Quality CustomSQL is not full ANSI SQL, but shares some syntax. Refer to [the AWS documentation](https://docs.aws.amazon.com/glue/latest/dg/dqdl.html#dqdl-rule-types-CustomSql) for specific examples.
* There are two forms of CustomSQL rules, only the second is suitable for Quarantine rules:
    * `CustomSql 'SELECT COUNT(*) FROM primary WHERE EffectiveDate > ExpirationDate' = 0`
    * `CustomSql 'SELECT PolicyNumber FROM primary WHERE EffectiveDate <= ExpirationDate'`
* The table expression `FROM primary` refers to the incoming tabular data set and must be identified with the name `primary` regardless of the table name in the Spark Hive Catalog / Glue Data Catalog
* Only the incoming data is available as a single table source
* A failing `after_sparksql` rule will not rollback the data saved to the cleanse bucket in the same workflow exection