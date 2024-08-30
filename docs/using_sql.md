# InsuranceLake Cleanse-to-Consume SQL Usage Documentation

The InsuranceLake ETL uses SQL to define the views of data created in the Cleanse-to-Consume Glue Job that are stored in the Consume S3 bucket and database with the suffix `_consume`. There are two kinds of SQL files supported:

* **Spark SQL**: Executed by the Spark session within AWS Glue and used to create a partitioned copy of the data in the Consume S3 bucket and Glue Catalog database

* **Athena SQL**: Executed via Amazon Athena API and used to create views based off the data in the Consume S3 bucket and Glue catalog database

While the ETL recommends certain approaches for the use of Spark and Athena SQL, there are no absolute restrictions on how the SQL is used. Particularlly, Athena SQL can be used for any SQL query that Athena supports.

Both SQL files are **optional** in a data pipeline. A pipeline can succeed with no SQL files defined.

## Contents

* [Spark SQL](#spark-sql)
* [Athena SQL](#athena-sql)
* [Variable Substitution](#variable-substitution)
* [Pattern Library](#pattern-library)
    * [Simplest Method to Populate Consume](#simplest-method-to-populate-consume)
    * [Join Example](#join-example)
    * [Override Table Name Example](#override-table-name-example)
    * [Override Partition Fields in Consume](#override-partition-fields-in-consume)
    * [Partition Snapshot View](#partition-snapshot-view)
    * [Union Example with Literals as Placeholders](#union-example-with-literals-as-placeholders)
    * [Case Statement Examples](#case-statement-examples)
    * [Unpivot / Stack](#unpivot--stack)
    * [Key Value Pivot](#key-value-pivot)
    * [Athena Fixed Width View](#athena-fixed-width-view)
* [FAQ](#frequently-asked-questions)


## Spark SQL

Spark SQL is defined in a file following the naming convention of `spark-<database name>-<table name>.sql` and is stored in the `/etl/transformation-sql` folder in the `etl-scripts` bucket. When using CDK for deployment, the contents of the `/lib/glue_scripts/lib/transformation-sql` directory will be automatically deployed to this location.

A full reference for all the syntax and commands available in Spark SQL can be found in the [Apache Spark SQL Reference documentation](https://spark.apache.org/docs/latest/sql-ref.html).

The AWS Glue Data Catalog is an Apache Hive metastore-compatible catalog ([Reference](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-data-catalog-hive.html)). InsuranceLake ETL AWS Glue jobs are configured to use the Data Catalog as an external Apache Hive metastore. Spark SQL is used to query the Glue Catalog and load data from data lake sources (typically in the Cleanse bucket) into a Spark DataFrame. The DataFrame is subsequently written to the Consume bucket and the Glue Catalog using a database name with an appended `_consume` suffix.

Considerations and requirements for InsuranceLake's integration of Spark SQL:

* Only one Spark SQL query per ETL workflow is supported at this time. Spark queries are 1-to-1 with the data pipeline workflow.

* Spark SQL in the InsuranceLake ETL is ideally suited to perform expensive operations such as joins and pivots, because the results of the operation will be written to the Consume S3 bucket. The operation will be performed again only when source data is updated.

* The most basic form is a `select * from cleanse_table` SQL statement, which will create a copy of the Cleanse bucket table specified and write it to the Consume bucket.

* Queries should be written to pull all data in tables, not just the most recent partition (unless that is the desired view). This is because the entire Consume bucket table is rewritten each time the workflow runs. If this implementation creates performance issues for the workflow, you can modify the behavior on [line 190 of the `etl_cleanse_to_consume.py` script](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_cleanse_to_consume.py#L198) by commenting out the table deletion as follows:

    ```python
    # glueContext.purge_s3_path(storage_location, options={ 'retentionPeriod': 0 })
    ```

* There is no limitation to the databases and tables that can be referenced and joined in your Spark SQL query. Any table in the Glue Catalog is available.

* If no Spark SQL file exists, there will be no error in the workflow. The Spark SQL file is optional; without it, **no table will be created in the Consume bucket**. It is often useful to skip this file initially to facilitate iteratively building a data workflow.

* Spark SQL queries must include the partition columns `year`, `month`, and `day`. It is recommended to also include `execution_id` for full data pipeline traceability. These columns will not be automatically added; the workflow will fail without the partition columns.

* The ETL supports the `CREATE TABLE` prefix to support overriding the default workflow tablename. See [Override Table Name](#override-table-name-example) for example Spark SQL.

    * **Note:** The table name override implementation is based on the following Python regular expression to identify the alternate table name and may not support all variations of query syntax:

        ```python
        r'\s*CREATE TABLE\s+["`\']?([\w]+)["`\']?\s+AS(.*)'
        ```

Example patterns using Spark SQL:
* [Simplest Method to Populate Consume](#simplest-method-to-populate-consume)
* [Join Example](#join-example)
* [Override Table Name Example](#override-table-name-example)
* [Override Partition Fields in Consume](#override-partition-fields-in-consume)
* [Union Example with Literals as Placeholders](#union-example-with-literals-as-placeholders)
* [Case Statement Examples](#case-statement-examples)
* [Unpivot / Stack](#unpivot--stack)
* [Key Value Pivot](#key-value-pivot)


## Athena SQL

Athena SQL is defined in a file following the naming convention of `athena-<database name>-<table name>.sql` and is stored in the `/etl/transformation-sql` folder in the `etl-scripts` bucket. When using CDK for deployment, the contents of the `/lib/glue_scripts/lib/transformation-sql` directory will be automatically deployed to this location.

Athena SQL is based on Trino and Presto SQL. The Athena SQL engine generally supports Trino and Presto syntax and adds its own improvements. Athena does not support all Trino or Presto features. A full reference for all the syntax and commands available in Athena SQL can be found in the [AWS Athena SQL Reference documentation](https://docs.aws.amazon.com/athena/latest/ug/ddl-sql-reference.html).

Considerations and requirements for InsuranceLake's integration with Athena SQL:

* When creating views ensure that each query start with `CREATE OR REPLACE VIEW` to ensure that the first, and all subsequent pipeline executions succeed.

* The default database for Athena-created views and tables is the workflow database with the appended `_consume` suffix. A different database can be specified using a full table name expression in the `CREATE OR REPLACE` statement.

    * **Warning:** Creating views and tables outside of the workflow's default database may create difficult to manage complexity when trying to understand which workflows are responsible for which tables in the Glue Catalog.

* The ETL supports multiple Athena SQL queries in the same file, each separated by a semicolon (`;`). This allows you to update multiple views of your data from a single data workflow (1-to-many).

    * **Note:** The multi-query implementation relies on the Python `split` function and may not support all variations of query syntax.

* Athena SQL supports [dot notation](https://docs.aws.amazon.com/athena/latest/ug/rows-and-structs.html) for working with arrays, maps, and structured data types for nested data.

* The ETL will ignore any returned data from the Athena SQL query. Only success or failure of the query will impact the workflow. You should design your Athena queries to be self-contained operations, such as creating a view or table.

* If no Athena SQL file exists, there will be no error in the workflow. The Athena SQL file is optional, and the execution step will be skipped.

* The intention of the Athena SQL integration is to create views and tables. To that end, the Athena SQL query is expected to complete in **15 seconds** or less. If your query takes longer you will see the following workflow error:

    ```log
    athena_execute_query() exceeded max_attempts waiting for query
    ```

    If your query usage requires more than 15 seconds to execute, you will need to increase the number of Athena status attempts specified in the `athena_execute_query()` function call on [line 228 of the `etl_cleanse_to_consume.py` script](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/glue_scripts/etl_cleanse_to_consume.py#L236) as follows:

    ```python
    status = athena_execute_query(
        args['target_database_name'], sql.format(**substitution_data), args['TempDir'], max_attempts=30)
    ```

Example patterns using Athena SQL:
* [Partition Snapshot View](#partition-snapshot-view)
* [Key Value Pivot](#key-value-pivot)
* [Athena Fixed Width View](#athena-fixed-width-view)


## Variable Substitution

Using Python-style format string variable substitution, you can automatically insert and use Cleanse-to-Consume Glue Job arguments in your SQL queries.

Variable substitution is available in both Athena and Spark SQL queries. The syntax follows the [Python format string syntax](https://docs.python.org/3/library/string.html#format-string-syntax). Use only keyword arguments in your format string, as the order of the arguments passed to the Glue Job may change. The [format specification mini-language](https://docs.python.org/3/library/string.html#format-specification-mini-language) is supported.

Example Spark SQL using variable substitution:
```sql
SELECT 
      claim_number
    , claim_status
    , '{JOB_RUN_ID}' as job_run_id
FROM
    claimdata
```

The following Cleanse-to-Consume Glue Job parameters are available for substitution (all case sensitive):

|Parameter Name	|Description
|---	|---
|JOB_ID	|Unique value assigned to the Glue Job definition at the time InsuranceLake infrastructure is deployed (remains static between Glue job runs)
|JOB_RUN_ID	|Unique value assigned to the Glue Job run; uniquely identifies each ETL pipeline and Glue Job run
|JOB_NAME	|Name of the Glue Job definition, assigned at the time InsuranceLake infrastructure is deployed
|environment	|Name of the deployment environment for the ETL infrastructure, as defined in [configuration.py](https://github.com/aws-samples/aws-insurancelake-etl/blob/main/lib/configuration.py#L6)
|TempDir	|S3 path to the Glue Job temporary storage
|txn_bucket	|S3 path to the Glue Job ETL Scripts bucket
|txn_sql_prefix_path	|Path to the ETL configuration in the Glue Job ETL Scripts bucket
|source_bucket	|S3 path to the Cleanse bucket
|target_bucket	|S3 path to the Consume bucket
|dq_results_table	|DynamoDB table name for Glue Data Quality results
|data_lineage_table	|DynamoDB table name where data lineage logging to be stored
|state_machine_name	|Name of the Step Function State Machine definition, assigned at the time InsuranceLake is deployed
|execution_id	|Unique value assigned to the Step Functions State Machine execution; uniquely identifies the ETL pipeline run
|source_key	|First and second level folder structure as defined in the Collect bucket in the form `database/table`
|source_database_name	|Glue Catalog database name pointing to the Cleanse bucket on which to run the Spark and Athena SQL by default
|target_database_name	|Glue Catalog database name pointing to the Consume bucket where the Spark SQL results will be written
|table_name	|Glue Catalog database name pointing to the common table name used in the Cleanse and Consume databases
|base_file_name	|Name of the source filename as it appears in the Collect bucket
|p_year	|Value used in the year partition as specified in the Step Functions State Machine input parameters (by default, corresponds to the created date of the source file)
|p_month	|Value used in the month partition as specified in the Step Functions State Machine input parameters (by default, corresponds to the created date of the source file)
|p_day	|Value used in the day of month partition as specified in the Step Functions State Machine input parameters (by default, corresponds to the created date of the source file)


## Pattern Library

What follows is a set of SQL patterns for both Spark SQL and Athena SQL designed to solve specific use cases. This library of patterns will be added to over time.


### Simplest Method to Populate Consume

If the data in your Cleanse bucket table is suitable for consumption and you prefer to only grant user access to Consume layer tables, us this simple query to publish a copy of the table in the Consume bucket. The use of `select *` will ensure that any schema changes made in the Cleanse table will also be made in the Consume table.

```sql
SELECT *
FROM syntheticlifedata.claimdata
```


### Join Example

Joining tables from other workflows to the table created in the current workflow is a common pattern used in InsuranceLake Spark SQL. When joining multiple data lake tables, you are likely to have identical fields in more than one table, which can result in ambiguous column reference errors like the following:

```log
AnalysisException: Reference 'a' is ambiguous, could be: a#1333L, a#1335L.
```

To unamiguously reference columns, explicitly name every column to include in the table (do not use `*`) and include a tablename prefix for each column. If multiple tables with the same name are joined, we recommend using table aliases in the join to differentiate the columns and to reduce complexity in the select statement.

```sql
SELECT
      policies.startdate
    , policies.enddate
    , policies.policynumber
    , effectivedate
    , expirationdate
    , claims.accidentyeartotalincurredamount

    , earnedpremium * 10 as claimlimit  -- example calculation expression

    , policies.execution_id
    , policies.year
    , policies.month
    , policies.day

FROM
    syntheticgeneraldata.policydata policies
LEFT OUTER JOIN
    syntheticgeneraldata.claimdata claims
    ON policies.policynumber = claims.policynumber
    AND policies.startdate = claims.startdate

ORDER BY policies.startdate ASC, lobcode ASC, agentname ASC, insuredindustry ASC
```


### Override Table Name Example

The optional starting line `CREATE TABLE <New Table Name> AS` can be used to create a table in the Consume layer with a different name than the one used in the workflow (in other words, different from the Collect bucket level 2 folder name). Changing the database name from the default Consume layer database is not supported and not recommended.

```sql
CREATE TABLE policy_claim_data AS
SELECT
      policies.policynumber
    , effectivedate
    , expirationdate
    , claims.accidentyeartotalincurredamount
    , policies.execution_id
    , policies.year
    , policies.month
    , policies.day
FROM
    syntheticgeneraldata.policydata policies
LEFT OUTER JOIN
    syntheticgeneraldata.claimdata claims
    ON policies.policynumber = claims.policynumber
    AND policies.startdate = claims.startdate
```


### Override Partition Fields in Consume

Loading historical data, depending on the amount, may need a different partition strategy than the loading date of the data (the ETL default). While not easily changeable in the Cleanse layer, a different partition strategy can easily be selected in the Consume layer by using different values from the dataset for the `year`, `month`, and `day` columns.

NOTE: By default, the ETL creates partitions values that are stored as zero-padded strings. In other words, the month of March is stored as `03`. In the below query the year, month, and date functions are used to extract an integer value for each of the partitions, which is a change from the default behavior. This is important to keep in mind if you are concerned about forward compatibility of partition strategy changes.

```sql
SELECT
      program
    , lineofbusiness
    , coveragedetail
    , startdate
    , enddate
    , effectivedate
    , expirationdate
    , policynumber
    , valuationdate

    , year(valuationdate) as year
    , month(valuationdate) as month
    , day(valuationdate) as day

FROM premium_historical
```


### Partition Snapshot View

If your data source provides a full snapshot of its current state for each data pipeline execution, you can create a filtered view using [Variable Substitution](#variable-substitution) of the most recently loaded partition. The query is efficient because it filters only on partition columns, and is a good candidate for an Athena SQL view.

```sql
CREATE OR REPLACE VIEW current_active_claims AS
SELECT
      policynumber
    , calcnummonthsnormalized
    , carrierdailyearnedpremium
    , calcdailyearnedpremium
    , policymonthindex

FROM commercialauto_consume.active_claims

WHERE
    year = '{p_year}' AND month = '{p_month}' AND day = '{p_day}'

```


### Union Example with Literals as Placeholders

Suppose we are getting policy SOI data from multiple producers and each send the data with a different schema. We've normalized the data in the Cleanse layer as much as possible, but there are still data fields from one producer that are not provided by another producer. To create a combined SOI table in the Consume layer, we can use the Spark SQL `UNION` clause.

```sql
CREATE TABLE combined_soi AS
SELECT
      policy_number
    , effective_date
    , expiration_date
    , written_premium
    , agent_code
    , insurance_company_name
    , program
    , coverage_detail
FROM producer_a_soi

UNION

SELECT
      policy_number
    , effective_date
    , expiration_date
    , written_premium
    , 'N/A' as agent_code
    , insurance_company_name
    , program
    , null as coverage_detail
FROM producer_b_soi

UNION

SELECT
      policy_number
    , effective_date
    , expiration_date
    , written_premium
    , 'ProducerC' as agent_code
    , insurance_company_name
    , 'AUTO' as program
    , coverage_detail

FROM producer_c_soi
```

### Case Statement Examples

Case statements can be used to define if/then logic required for some views of data. For example, the policy month index column from an [expandpolicymonths transform](transforms.md#expandpolicymonths) can be used to create a field indicating whether the policy was written in the reporting period represented by in row of data.

The [lookup](transforms.md#lookup) and [multilookup](transforms.md#multilookup) transforms can also be used to refactor if/then logic with many choices in the Collect-to-Cleanse Glue job.

```sql
SELECT
      policy_number
    , effectivedate
    , expirationdate
    , period_start_date
    , period_end_date
    , writtenpremiumamount

    , CASE WHEN policymonthindex = 1 THEN 1 ELSE 0
        END AS writtenpolicy

    , CASE WHEN
            year(expirationdate) = year(period_start_date) 
            AND month(expirationdate) = month(period_start_date)
        THEN 1 ELSE 0
        END AS expiringpolicy

    , CASE WHEN
            year(expirationdate) = year(period_start_date)
            AND month(expirationdate) = month(period_start_date)
        THEN writtenpremiumamount ELSE 0
        END AS expiringpremiumamount

    , execution_id
    , year
    , month
    , day

FROM syntheticgeneraldata.writtenpolicydata
ORDER BY period_start_date ASC, lobcode ASC, agentname ASC, insuredindustry ASC
```


### Unpivot / Stack

Consider a policy data source with split information organized as follows:

|Policy Number  |Total Premium  |Insurance Co. 1    |Insurance Co. 1 % Participation    |Insurance Co. 1 Premium	|Insurance Co. 2	|Insurance Co. 2 % Participation	|Insurance Co. 2 Premium	|Insurance Co. 3	|Insurance Co. 3 % Participation	|Insurance Co. 3 Premium
|---    |---    |---    |---    |---    |---    |---    |---    |---    |---    |---
|19872364   |800000 |CarrierA   |0.4   |320000   |CarrierB   |0.3633    |290666   |CarrierC   |0.2367   |189333.33
|91892734   |40000 |CarrierA   |0.4   |16000   |CarrierC   |0.3633    |14566   |CarrierD   |0.2367   |9466.65

We would like to unpivot this data so that each policy has multiple rows, one for each insurance carrier. This will enable us to recalculate the split premium amount and use a data quality check to ensure it's within our range of tolerance. This structure of the data will also help us to join claim data and do analysis at the carrier level, while still being able to roll-up to the policy level.

* NOTE: We intentionally want to perform this operation in the Cleanse-to-Consume Glue job, saving the resulting data in the Consume bucket. The unpivot operation significantly changes the shape of the data and some future analysis may require access to the original shape (preserved in the Cleanse layer).

This unpivot operation can be accomplished using the [Spark stack generator function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.stack.html).
    
* NOTE: An alternative approach, which would better self-document intent, is to use the [Spark unpivot function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpivot.html). However, this function was introduced in Spark 3.4.0, which is not yet supported by Glue.

```sql
SELECT
      policy_number
    , total_premium

    , insurance_co
    , insurance_co_participation
    , insurance_co_split_premium
    , insurance_co_participation * project_premium AS insurance_co_calc_premium

    , execution_id
    , year
    , month
    , day

FROM (
    SELECT
        -- Select specific columns in the outer SELECT, not here
        *

        -- First stack parameter: (integer) represents the number of column sets to expect. Think of
        -- 		stack() operating on one row of data at a time, transforming that row to the number of
        --		rows specified in this parameter.
        -- Remaining parameters: (list of Spark SQL expressions, usually column names) These parameters
        -- 		are grouped by sets: a row counter, insurance company name, participation %, premium,
        --		and calculated premium, map to 4 columns in the result data set. You can verify this by
        --		dividing the total number of column expression parameters (28 here) by the integer
        --		first parameter (7 here).
        -- Column alias after stack clause aliases 4 columns at once, properly naming the columns in the
        --		result data set.
        , stack( 7
            , 1, insurance_co_1, insurance_co_1_participation, insurance_co_1_premium
            , 2, insurance_co_2, insurance_co_2_participation, insurance_co_2_premium 
            , 3, insurance_co_3, insurance_co_3_participation, insurance_co_3_premium 
            , 4, insurance_co_4, insurance_co_4_participation, insurance_co_4_premium 
            , 5, insurance_co_5, insurance_co_5_participation, insurance_co_5_premium 
            , 6, insurance_co_6, insurance_co_6_participation, insurance_co_6_premium 
            , 7, insurance_co_7, insurance_co_7_participation, insurance_co_7_premium

        ) AS (insurance_co_counter, insurance_co, insurance_co_participation, insurance_co_split_premium)

    FROM mydb.mytable
)

-- Remove rows where some or all of the 7 insurance_co splits are not defined, except for 1 row, so no premiums are skipped
WHERE insurance_co is not null OR insurance_co_counter = 1
```


### Key Value Pivot

Consider policy attribute data coming from a core system as follows:

|Field Label    |Field Value    |Last Updated   |Last Updated By    |PolicyNumber
|---    |---    |---    |---    |---
|SalesTerritory |US Eastern |2024-03-01 |admin  |19872364
|SalesTerritory |US |2024-02-29 |admin  |19872364
|ProducerChannel    |Digital    |2024-02-29 |admin  |19872364
|BusinessUnitCode   |8722   |2024-03-15 |supervisor |19872364
|BusinessUnitCode   |1199   |2024-03-01 |admin  |19872364
|BusinessUnitCode   |N/A    |2024-02-29 |admin  |19872364

Each field label represents a policy attribute that you want to join with the primary policy data. There could be more than one value for each attribute and you want to always select the lastest version (by last updated timestamp). To prepare the attribute data we use the [map_from_arrays](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#map-functions) Spark aggregate function to map the key labels to key values, combined with the [collect_list](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#aggregate-functions) aggregate function to convert the columns of data into a list.

Once the data has been prepared in a map type column, it is saved to the Consume bucket. You can then use an Athena view to combine the attributes you need with the primary policy data using map element expressions in the select. Examples for both queries follow.

Spark SQL:
```sql
SELECT
      policy_number

    , map_from_arrays(
          collect_list(lower(field_label))
        , collect_list(field_value)
    ) kv_data

    , MAX(last_updated) AS last_updated

    , execution_id
    , year
    , month
    , day

FROM (
    -- Order matters for duplicate resolution when grouping map_from_arrays
    -- and sort must occur before group-by
    SELECT * FROM mydb.mytable
    ORDER BY year, month, day
)

-- remove null values because they will be skipped by collect_list and result
-- in arrays of different lengths, which causes an error
WHERE field_label IS NOT NULL AND field_value IS NOT NULL

GROUP BY
      policy_number
    , execution_id
    , year
    , month
    , day
```

Athena SQL:
```sql
CREATE OR REPLACE VIEW policy_with_attributes AS
SELECT
      policynumber
    , kv_data['salesterritory']
    , kv_data['producerchannel']
    , kv_data['businessunitcode']
FROM mydb_consume.mytable
```


### Athena Fixed Width View

Some reporting requirements, such as regulatory, require the use of a fixed width format. Once the data is prepared in the Consume layer, we can use the [lpad](https://prestodb.io/docs/current/functions/string.html#lpad) and [coalesce](https://prestodb.io/docs/current/functions/conditional.html#coalesce) Athena functions to create a view that includes the policy number and report date columns for traceability.

```sql
CREATE OR REPLACE VIEW stat_report AS
SELECT
      policy_line_id
    , report_date

    , concat(
        -- lpad() will drop a field if it is null
          lpad(coalesce(rate_date, ' '), 4, ' ')
        , lpad(coalesce(territory, ' '), 3, ' ')
        , lpad(coalesce(policy_type, ' '), 2, ' ')
        , lpad(coalesce(subline, ' '), 3, ' ')
        , lpad(coalesce(class, ' '), 6, ' ')
        , lpad(coalesce(exposure, ' '), 7, ' ')
        , lpad(coalesce(limit_code_1, '0'), 1, '0')
        , lpad(coalesce(state_exception, ' '), 1, ' ')
        , lpad(coalesce(filler1, ' '), 1, ' ')
        , lpad(coalesce(bodily_injury_limit, ' '), 2, ' ')
        , lpad(coalesce(property_damage, ' '), 2, ' ')
        , lpad(coalesce(medical_payments, ' '), 1, ' ')
        , lpad(coalesce(uninsured_code_a, ' '), 1, ' ')
        , lpad(coalesce(uninsured_code_b, ' '), 1, ' ')
        , lpad(coalesce(liability_deductible, ' '), 2, ' ')
        , lpad(coalesce(rating_id, ' '), 1, ' ')
        , lpad(coalesce(stated_amount_indicator, ' '), 1, ' ')
        , lpad(coalesce(terrorism_code, ' '), 1, ' ')
        , lpad(coalesce(filler2, ' '), 1, ' ')
        , lpad(coalesce(zip, ' '), 5, ' ')
        , lpad(coalesce(limit_id_code, ' '), 1, ' ')
        , lpad(coalesce(rating_zone, '0'), 3, '0')
        , lpad(coalesce(loss_cost_modifier, ' '), 3, ' ')
        , lpad(coalesce(rate_experience, ' '), 3, ' ')
        , lpad(coalesce(rate_schedule, ' '), 2, ' ')
        , lpad(coalesce(rate_package, ' '), 2, ' ')
        , lpad(coalesce(rate_expense, ' '), 2, ' ')
        , lpad(coalesce(rate_commission, ' '), 2, ' ')
        , lpad(coalesce(rate_deductible, ' '), 2, ' ')
        , lpad(coalesce(deductible_code, ' '), 1, ' ')
        , lpad(coalesce(filler3, ' '), 2, ' ')
        , lpad(coalesce(covered_item_manufacture_year, ' '), 2, ' ')
        , lpad(coalesce(limit_code_2, ' '), 1, ' ')
        , lpad(coalesce(covered_item_identifier, ' '), 17, ' ')
        , lpad(coalesce(naics_code, ' '), 6, ' ')
        , lpad(coalesce(price_bracket, '0'), 3, '0')
        , lpad(coalesce(number_covered_items, '0'), 4, '0')
        , lpad(coalesce(filler4, ' '), 47, ' ')
        , lpad(coalesce(filler5, ' '), 100, ' ')
        , lpad(coalesce(filler6, ' '), 150, ' ')
        , lpad(coalesce(filler7, ' '), 98, ' ')
        , lpad(coalesce(type_of_loss, ' '), 2, ' ')
    ) AS fwf_line
FROM syntheticgeneraldata.stat_data
```


## Frequently Asked Questions

1. Why do you place commas in front of field names in your SQL instead of at the end?
    * When the comma is isolated and in front, it is harder to forget adding a new comma when adding a new field.