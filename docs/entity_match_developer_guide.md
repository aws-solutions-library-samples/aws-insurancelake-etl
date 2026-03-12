---
title: Entity Matching Developer Guide
parent: Developer Documentation
nav_order: 5
last_modified_date: 2026-03-11
---
# InsuranceLake Entity Matching Developer Guide
{: .no_toc }

This section provides detailed implementation information for developers working with or extending the Entity Match AWS Glue ETL job (`etl_consume_entity_match.py`).

For user documentation on entity matching, refer to the [Entity Matching User Documentation](entity_matching.md).

## Contents
{: .no_toc }

* TOC
{:toc}


## Key Functions

### `fill_global_id(df, global_id, args, lineage)`

Assigns unique global IDs to all records that don't have one.

**Parameters**:
- `df`: Spark DataFrame to process
- `global_id`: Name of the global ID field
- `args`: Job arguments dictionary
- `lineage`: DataLineageGenerator instance

**Returns**: Spark DataFrame with all global ID fields populated

**Implementation Details**:
- Uses Spark's `uuid()` function to generate unique identifiers
- Preserves existing global IDs using `coalesce()`
- Ensures global ID field is the first column in the DataFrame
- Updates data lineage tracking

**Example Usage**:
```python
entity_incoming_df = fill_global_id(
    entity_incoming_df, 
    'globalid', 
    args, 
    lineage
)
```

### `split_dataframe(df, global_id)`

Splits a DataFrame into two based on null values in the global ID field.

**Parameters**:
- `df`: Spark DataFrame to split
- `global_id`: Name of the global ID field

**Returns**: Tuple of (matched_df, tomatch_df) where:
- `matched_df`: Records with non-null global IDs
- `tomatch_df`: Records with null global IDs

**Implementation Details**:
- Uses Spark DataFrame `filter()` operations
- No data is modified, only filtered
- Both DataFrames share the same schema

**Example Usage**:
```python
matched_df, tomatch_df = split_dataframe(entity_incoming_df, 'globalid')
```

### `entitymatch_exact(entity_primary_df, entity_incoming_tomatch_df, spec, spark)`

Performs exact matching based on source system identifiers.

**Parameters**:
- `entity_primary_df`: Primary entity table DataFrame
- `entity_incoming_tomatch_df`: Incoming records to match
- `spec`: Configuration dictionary
- `spark`: Spark session

**Returns**: Tuple of (matched_df, unmatched_df)

**Implementation Details**:
- Performs a left outer join on source system key and primary key
- Uses `coalesce()` to prefer incoming global ID over primary table global ID
- Returns empty DataFrames if exact match fields are not configured
- Caches the result DataFrame for subsequent operations

**Join Logic**:
```python
entity_incoming_tomatch_df.join(
    entity_primary_df,
    (entity_incoming_tomatch_df[source_primary_key] == entity_primary_df[source_primary_key]) & \
    (entity_incoming_tomatch_df[source_system_key] == entity_primary_df[source_system_key]),
    'leftouter'
)
```

### `entitymatch_recordlinkage(entity_primary_df, entity_incoming_tomatch_df, spec, spark)`

Performs probabilistic matching using the Python recordlinkage library.

**Parameters**:
- `entity_primary_df`: Primary entity table DataFrame
- `entity_incoming_tomatch_df`: Incoming records to match
- `spec`: Configuration dictionary
- `spark`: Spark session

**Returns**: Tuple of (matched_df, unmatched_df)

**Implementation Details**:

1. **Add Blocking Columns**: Creates temporary columns for recordlinkage blocking
   ```python
   blocking_cols_map = {
       f"recordlinkage_blocking_{level['id']}": 
           reduce(concat, ColumnBlockingIterator(level['blocks']))
       for level in spec['levels']
   }
   ```

1. **Convert to Pandas**: Uses `toPandas()` with Arrow optimization enabled
   ```python
   spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', True)
   entity_incoming_pandas_df = entity_incoming_tomatch_df.toPandas()
   ```

1. **Configure Recordlinkage**: Sets up indexer and comparison for each level
   ```python
   indexer = recordlinkage.Index()
   compare_cl = recordlinkage.Compare()
   
   for field in level['fields']:
       algorithm = getattr(compare_cl, field['type'])
       algorithm(**field)
   ```

1. **Perform Matching**: Computes weighted average of field comparisons
   ```python
   features = compare_cl.compute(candidate_links, entity_primary_pandas_df, entity_incoming_pandas_df)
   wa = np.average(result, weights=arr, axis=1)
   matches = calculated_wa[calculated_wa.loc[:,0] >= level['threshold']]
   ```

1. **Update Global IDs**: Assigns matched global IDs to incoming records
   ```python
   entity_incoming_pandas_df.loc[match_index, global_id] = \
       entity_primary_pandas_df.loc[primary_index, global_id]
   ```

1. **Convert Back to Spark**: Creates Spark DataFrame and removes blocking columns
   ```python
   entity_incoming_matched_df = spark.createDataFrame(entity_incoming_pandas_df) \
       .drop(*list(blocking_cols_map.keys()))
   ```

### `ColumnBlockingIterator`

Helper class to convert Python array-style string slicing to Spark substring expressions.

**Purpose**: Enables blocking configuration like `"firstname[:1]"` to be converted to Spark SQL `substring(firstname, 1, 1)`

**Implementation Details**:
- Implements Python iterator protocol
- Uses regular expressions to parse slicing syntax
- Converts to Spark `substring()` expressions
- Handles empty start/stop values (defaults to 0 and length)
- Adjusts for Spark's 1-based indexing vs Python's 0-based indexing


## Apache Iceberg Integration

The Entity Match job uses Apache Iceberg for the primary entity table to provide:

1. **ACID Transactions**: Ensures data consistency during concurrent reads and writes
1. **Schema Evolution**: Automatically handles schema changes without rewriting data
1. **Time Travel**: Enables querying historical versions of entity data
1. **Efficient Updates**: MERGE INTO operation updates only changed records

### Apache Iceberg Configuration

The job configures Spark to use Iceberg before creating the Spark context:

```python
spark_conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark_conf.set('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
spark_conf.set('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
spark_conf.set('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
spark_conf.set('spark.sql.catalog.glue_catalog.warehouse', storage_location)
spark_conf.set('spark.sql.iceberg.handle-timestamp-without-timezone', True)
```

### Creating the Primary Table

On first run, the primary entity table is created with Iceberg format version 2:

```python
entity_incoming_df.writeTo(
    f"{args['iceberg_catalog']}.{consume_database}.{entity_primary_table_name}") \
    .tableProperty('format-version', '2') \
    .partitionedBy(*partition.keys()) \
    .create()
```

### Merging Updates

On subsequent runs, matched records are merged using Iceberg's MERGE INTO:

```python
spark.sql(f"""MERGE INTO
    `{args['iceberg_catalog']}`.`{consume_database}`.`{entity_primary_table_name}`
    USING entity_incoming
    ON `{entity_primary_table_name}`.`{global_id_field}` = entity_incoming.`{global_id_field}`
    WHEN MATCHED THEN UPDATE SET {update_list}
    WHEN NOT MATCHED THEN INSERT *
    """)
```


## Data Lineage Tracking

The Entity Match job integrates with InsuranceLake's data lineage tracking system to provide partial data lineage:

```python
lineage = DataLineageGenerator(args)

# Track global ID field addition
lineage.update_lineage(entity_incoming_df, args['source_key'], 'add_empty_column',
    transform=[{ 'field': global_id_field }])

# Track global ID filling
lineage.update_lineage(df, args['source_key'], 'fill_global_id',
    transform=[{ 'field': global_id }])
```

Two data operations within the ETL job do not properly track data lineage yet. In the future, these functions will track results of the join at a row level in the lineage data.
1. [Exact match join](#entitymatch_exactentity_primary_df-entity_incoming_tomatch_df-spec-spark)
2. [recordlinkage match data merge](#entitymatch_recordlinkageentity_primary_df-entity_incoming_tomatch_df-spec-spark)


## Known Issues

### Long-running ETL Jobs
{: .no_toc }

**Possible Cause**: The recordlinkage library requires loading data into Pandas DataFrames, which are not distributed; operations run only on the driver. For more information on DataFrame conversions and operations, review the [Developer Guide Additional Code Considerations](developer_guide.md#additional-code-considerations).

**Possible Solutions**: 
- Reduce the number of levels in the match configuration. Each level is a separate recordlinkage `Compare` and Pandas DataFrame operation.
- Reimplement the call to recordlinkage `Compare` as an Apache Spark Pandas User Defined Function. This implementation would require decomposing the the comparison operation to work on one partition of data at a time, possibly through blocking techniques, or by accepting a reduced matching accuracy.


### Iceberg merge fails with new AWS Glue versions
{: .no_toc }

**Error**: `AnalysisException: [INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects a deterministic expression, but the actual expression is "(globalid = globalid)", "exists(globalid)".`

**Cause**: The combination of `uuid()` for assigning global IDs and `MERGE INTO` triggers a non-deterministic expression error. The Spark logic plan believes that this set of expressions is non-deterministic. However, the `uuid()` function is specifically implemented in Apache Spark to be [deterministic enough](https://issues.apache.org/jira/browse/SPARK-23599). This appears to be a bug in the Apache Iceberg library for Apache Spark and is being tracked in [Github Issues](https://github.com/apache/iceberg/issues/14585).

**Workaround**: Use AWS Glue v4 for the Entity Match job until Apache Iceberg issues have been resolved.


### Pandas Conversion Errors
{: .no_toc }

**Error**: `pyarrow.lib.ArrowInvalid: Could not convert <value> with type <type>`

PyArrow is used to convert data in Spark DataFrames to Pandas DataFrames, and vice versa. These operations require converting between Spark data types and Pandas data types; not all data types have a supported mapping.

**Solution**: Ensure all DataFrame columns have compatible types for PyArrow conversion. Check for:
- Complex nested types not supported by Arrow
- Custom Python objects in DataFrame columns
- Inconsistent data types within columns