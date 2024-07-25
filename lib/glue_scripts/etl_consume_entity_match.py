import sys
import json
import os
import re
from functools import reduce
import recordlinkage
import numpy as np
import pyspark.pandas as pd

from pyspark.sql.dataframe import DataFrame
from pyspark.context import SparkContext, SparkConf
from pyspark.sql.functions import col, lit, when, expr, coalesce, concat
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# For running in local Glue container
sys.path.append(os.path.dirname(__file__) + '/lib')

from glue_catalog_helpers import table_exists
from datalineage import DataLineageGenerator

expected_arguments = [
    'JOB_NAME',
    'state_machine_name',
    'execution_id',
    'source_key',
    'environment',
    'TempDir',
    'txn_bucket',
    'txn_spec_prefix_path',
    'target_bucket',
    'iceberg_catalog',
    'database_name_prefix',
    'table_name',
    'p_year',
    'p_month',
    'p_day',
]

# Handle optional arguments
for arg in sys.argv:
    if '--data_lineage_table' in arg:
        expected_arguments.append('data_lineage_table')


def fill_global_id(df: DataFrame, global_id: str, args, lineage) -> DataFrame:
    """ Assign all entities a unique ID that do not have one and ensure Global ID field is the
    first field

    Parameters
    ----------
    df
        Spark DataFrame in which to fill Global ID values
    global_id
        The name of the Global ID field (must exist as a column in the DataFrame)

    Returns
    -------
    DataFrame
        Spark DataFrame with filled Global ID fields
    """
    columns = [
        # Add dataframe alias to avoid ambiguity in select
        column for column in df.columns
            if column != global_id
    ]
    lineage.update_lineage(df, args['source_key'], 'fill_global_id',
        transform=[{ 'field': global_id }])

    return df.select(
        when(col(global_id).isNull(), expr('uuid()')) \
            .otherwise(col(global_id)) \
            .alias(global_id),
        *columns,
    )


def split_dataframe(df: DataFrame, global_id: str) -> tuple:
    """Split DataFrame into 2 based on NULL values in Global ID field

    Parameters
    ----------
    df
        Spark DataFrame to split
    global_id
        The name of the Global ID field

    Returns
    -------
    tuple
        Rows with Global ID field not null, rows with Global ID field null Spark DataFrames
    """
    matched_df = df.filter(df[global_id].isNotNull())
    tomatch_df = df.filter(df[global_id].isNull())
    return matched_df, tomatch_df


def entitymatch_exact(
        entity_primary_df: DataFrame,
        entity_incoming_tomatch_df: DataFrame,
        spec: dict,
        spark
    ) -> tuple:
    """Perform entity matching using exact match on two user configured fields (system ID and customer ID)

    Parameters
    ----------
    entity_primary_df
        Spark DataFrame containing contents of primary entity table
    entity_incoming_tomatch_df
        Spark DataFrame containing incoming entity rows to match to primary
    spec
        Config specification read from JSON config file
    spark
        Spark session to use

    Returns
    -------
    tuple
        Matched rows, unmatched rows in two Spark DataFrames
    """
    global_id = spec['global_id_field']
    empty_df = spark.createDataFrame([], entity_incoming_tomatch_df.schema)

    if 'exact_match_fields' in spec:
        source_primary_key = spec['exact_match_fields']['source_primary_key']
        source_system_key = spec['exact_match_fields']['source_system_key']
    else:
        print('Skipping exact match because exact match fields not present in entitymatch spec')
        return empty_df, entity_incoming_tomatch_df

    # Matched rows will already be filtered, so just check for empty DataFrame
    if entity_incoming_tomatch_df.rdd.isEmpty():
        print(f'Skipping exact match because all incoming rows have a global ID {global_id} field assigned')
        return entity_incoming_tomatch_df, empty_df

    # List of columns in incoming data for optimized select statements
    entity_incoming_columns = [
        # Add dataframe alias to avoid ambiguity in select
        'incoming.' + column for column in entity_incoming_tomatch_df.columns
            # Omit the global ID column because we will add it in the join
            if column != global_id
    ]

    # Incoming data table alias must match the one used in the column list above
    # TODO: Data lineage opportunity here
    entity_incoming_tomatch_df = entity_incoming_tomatch_df.alias('incoming').join(
            entity_primary_df.alias('primary'),
            (entity_incoming_tomatch_df[source_primary_key] == entity_primary_df[source_primary_key]) & \
                (entity_incoming_tomatch_df[source_system_key] == entity_primary_df[source_system_key]
            ),
            'leftouter'
        ).select(
            # Prefer incoming global ID if both are set, ensure there is only one global ID field
            # in the joined dataframe, and it is the first field
            coalesce( 'incoming.' + global_id, 'primary.' + global_id ).alias(global_id),
            *entity_incoming_columns
        )

    entity_incoming_tomatch_df.cache()
    return split_dataframe(entity_incoming_tomatch_df, global_id)


class ColumnBlockingIterator:
    """
    Class to convert Python array-style string slicing (without step) to Spark substring
    Intended to be used with reduce(concat(), ...) to create recordlinkage blocking columns
    """
    def __init__(self, blocking_list: list):
        """
        Parameters
        ----------
        blocking_list
            List of column names optionally with Python array-style string slicing markup to be
            used to create a recordlinkage blocking columns
        """
        self._sequence = blocking_list
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._sequence):
            raise StopIteration

        blocking = self._sequence[self._index]
        self._index += 1

        string_slicing = re.compile(r'(\w+)\[(\d*):(\d*)\]')
        string_slicing_match = string_slicing.match(blocking)
        if string_slicing_match:
            column_name = string_slicing_match.group(1)
            start = string_slicing_match.group(2) if string_slicing_match.group(2) else '0'
            stop = string_slicing_match.group(3) if string_slicing_match.group(3) else f'length({column_name})'
            # Spark substring starts at 1, Python at 0; length = stop - start
            return expr(f'substring({column_name}, {start} + 1, {stop} + 1 - {start})')
        else:
            return col(blocking)


def entitymatch_recordlinkage(
        entity_primary_df: DataFrame,
        entity_incoming_tomatch_df: DataFrame,
        spec: dict,
        spark
    ) -> tuple:
    """Perform entity matching using Python recordlinkage library

    Parameters
    ----------
    entity_primary_df
        Spark DataFrame containing contents of primary entity table
    entity_incoming_tomatch_df
        Spark DataFrame containing incoming entity rows to match to primary
    spec
        Config specification read from JSON config file
    spark
        Spark session to use

    Returns
    -------
    tuple
        Matched rows, unmatched rows in two Spark DataFrames
    """
    global_id = spec['global_id_field']
    empty_df = spark.createDataFrame([], entity_incoming_tomatch_df.schema)

    if 'levels' not in spec:
        print('Skipping recordlinkage match because levels section not present in entitymatch spec')
        return spark.emptyDataFrame, entity_incoming_tomatch_df

    # Matched rows will already be filtered, so just check for empty DataFrame
    if entity_incoming_tomatch_df.rdd.isEmpty():
        print(f'Skipping recordlinkage match because all incoming rows have a global ID {global_id} field assigned')
        return entity_incoming_tomatch_df, empty_df

    # Add blocking columns to be used by recordlinkage library (will later be dropped)
    blocking_cols_map = {
        # Stitch together blocking string in a new Spark DataFrame column
        f"recordlinkage_blocking_{level['id']}": reduce(concat, ColumnBlockingIterator(level['blocks']))
            for level in spec['levels']
    }
    entity_incoming_tomatch_df = entity_incoming_tomatch_df.withColumns(blocking_cols_map)
    entity_primary_df = entity_primary_df.withColumns(blocking_cols_map)

    # Convert to pandas dataframes for recordlinkage
    entity_incoming_pandas_df = entity_incoming_tomatch_df.toPandas()
    entity_primary_pandas_df = entity_primary_df.toPandas()

    indexer = recordlinkage.Index()
    for level in spec['levels']:
        compare_cl = recordlinkage.Compare()
        weights = []
        for field in level['fields']:
            # Remove weight from field dictionary and append to weights list
            weights.append(field.pop('weight'))
            # We have matching schemas, so both sides and the output are the same field name
            field['left_on'] = field['right_on'] = field['label'] = field.pop('fieldname')
            # Get classifier name from configured type value
            classifier = getattr(compare_cl, field.pop('type'))
            # Optional user-specified args: threshold, method, other classifier-specific
            classifier(**field)

        indexer.block(f"recordlinkage_blocking_{level['id']}")
        candidate_links = indexer.index(entity_primary_pandas_df, entity_incoming_pandas_df)
        # Actually do the matching
        features = compare_cl.compute(candidate_links, entity_primary_pandas_df, entity_incoming_pandas_df)

        arr = np.array(weights)
        result = features.to_numpy()
        wa = np.average(result, weights=arr, axis=1)
        index_match = features.index.values
        calculated_wa = pd.DataFrame(wa, index=list(index_match))

        matches = calculated_wa[calculated_wa.loc[:,0] >= level['threshold']]
        # TODO: Data lineage opportunity here
        for i in range(len(matches.index.values)):
            entity_incoming_pandas_df.loc[matches.index.values[i][1], global_id] = \
                entity_primary_pandas_df.loc[matches.index.values[i][0], global_id]

    # Convert incoming data back to Spark DataFrame and drop all blocking columns
    entity_incoming_matched_df = spark.createDataFrame(entity_incoming_pandas_df) \
        .drop( *list(blocking_cols_map.keys()) )
    entity_incoming_matched_df.cache()

    return split_dataframe(entity_incoming_matched_df, global_id)


def main():
    args = getResolvedOptions(sys.argv, expected_arguments)

    # Storage location is used for all tables in the Iceberg warehouse
    storage_location = args['target_bucket'] + '/iceberg'
    consume_database = f"{args['database_name_prefix'].lower()}_consume"
    source_table = args['table_name'].lower()   # lowercase not strictly needed, but more accurate

    spark_conf = SparkConf()
    # Iceberg configuration must be set before creating the Spark Context
    spark_conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    spark_conf.set('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
    spark_conf.set('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
    spark_conf.set('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    spark_conf.set('spark.sql.catalog.glue_catalog.warehouse', storage_location)
    spark_conf.set('spark.sql.iceberg.handle-timestamp-without-timezone', True)

    sc = SparkContext(conf=spark_conf)
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Use the Spark DataFrame to Pandas DataFrame optimized conversion
    spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', True)

    # Job parameter supplied date partition strategy (used for data retrieval)
    partition = {
        # Strongly type job arguments to reduce risk of SQL injection
        'year': f"{int(args['p_year'])}",
        'month': f"{int(args['p_month']):02}",
        'day': f"{int(args['p_day']):02}",
    }


    # Load config file from spec folder
    txn_spec_prefix = args['txn_spec_prefix_path']
    txn_spec_key = txn_spec_prefix[1:] + args['database_name_prefix'] + '-' + 'entitymatch' + '.json'

    print(f'Using entity matching specification from: {txn_spec_key}')
    try:
        spec_data = sc.textFile(f"{args['txn_bucket']}/{txn_spec_key}")
        spec_json_data = json.loads('\n'.join(spec_data.collect()))
    except Exception as e:
        print(f'No entity matching spec file exists or error reading: {e.java_exception.getMessage()}')
        raise RuntimeError('Entity matching spec file required and none found')
    print(f'Using transformation specification: {spec_json_data}')

    entity_primary_table_name = spec_json_data['primary_entity_table']
    global_id_field = spec_json_data['global_id_field']

    # Incoming data can have the global ID set for some rows-- these should be left intact
    # Incoming data can have no global ID, so all rows need it added
    # Incoming data can be matched with optional source system ID and system key and should not be fuzzy matched
    # Any rows with no global ID, and no match from source system ID/system key should be fuzzy matched
    # Any rows that didn't match using any method should be treated as new entities and assigned a global ID


    # Read newly added partition from consume table using Glue Catalog Hive
    # Bandit check suppressed due to parameters being from trusted, known source
    partition_predicate = ' AND '. \
        join([ f"{name} == '{value}'" for name, value in partition.items() ])
    entity_incoming_df = spark.sql(f'SELECT * FROM {consume_database}.{source_table}'   # nosec B608
        f' WHERE {partition_predicate}')

    print(f'Retrieved {entity_incoming_df.count()} records as incoming data from'
        f' {consume_database}.{source_table}')


    lineage = DataLineageGenerator(args)

    if global_id_field not in entity_incoming_df.columns:
        # Incoming dataset does not provide a global ID column, so add an empty one
        entity_incoming_df = entity_incoming_df.withColumn(global_id_field, lit(None))
        lineage.update_lineage(entity_incoming_df, args['source_key'], 'add_column',
            transform=[{ 'field': global_id_field }])
        print(f'Global ID field {global_id_field} not found; field added and populated')
    else:
        print(f'Global ID field {global_id_field} found and will take priority over matching')


    if not table_exists(consume_database, entity_primary_table_name):
        print('Creating primary entity table for the first time...')
        # Any records without a global ID are assumed new records and assigned an ID
        entity_incoming_df = fill_global_id(entity_incoming_df, global_id_field, args, lineage)
        entity_incoming_df.cache()

        print(f'Incoming record schema: {entity_incoming_df.schema}')
        print(f'Writing {entity_incoming_df.count()} incoming records to primary entity table:'
            f' {consume_database}.{entity_primary_table_name}')

        entity_incoming_df.writeTo(
            f"{args['iceberg_catalog']}.{consume_database}.{entity_primary_table_name}") \
            .tableProperty('format-version', '2') \
            .partitionedBy(*partition.keys()) \
            .create()

    else:
        print('Matching incoming data to primary entity table...')

        # Read existing primary entity Hudi table
        entity_primary_df = spark.read \
            .format('iceberg') \
            .load(f"{args['iceberg_catalog']}.{consume_database}.{entity_primary_table_name}")
        entity_incoming_df.cache()
        print(f'{entity_primary_df.count()} records read from primary table {consume_database}.{entity_primary_table_name}')

        # Do not check for schema change
        # Evolution supported by Iceberg: https://iceberg.apache.org/docs/latest/evolution/

        entity_incoming_prematched_df, entity_incoming_tomatch_df = split_dataframe(entity_incoming_df, global_id_field)
        entity_incoming_df.unpersist()

        entity_incoming_exact_matched_df, entity_incoming_tomatch_df = entitymatch_exact(
            entity_primary_df,
            entity_incoming_tomatch_df,
            spec_json_data,
            spark
        )
        print(f'Exact-match matched {entity_incoming_exact_matched_df.count()} records')

        entity_incoming_recordlinkage_matched_df, entity_incoming_tomatch_df = entitymatch_recordlinkage(
            entity_primary_df,
            entity_incoming_tomatch_df,
            spec_json_data,
            spark
        )
        print(f'Recordlinkage-match matched {entity_incoming_recordlinkage_matched_df.count()} records')

        # Anything unmatched after matching is assumed to be new records
        entity_incoming_filled_df = fill_global_id(entity_incoming_tomatch_df, global_id_field, args, lineage)
        entity_incoming_tomatch_df.unpersist()
        print(f'Generated a global ID for {entity_incoming_filled_df.count()} new records')

        # Combine any matched records with newly assigned records
        entity_incoming_df = entity_incoming_filled_df.unionByName(entity_incoming_prematched_df) \
            .unionByName(entity_incoming_exact_matched_df) \
            .unionByName(entity_incoming_recordlinkage_matched_df)

        entity_incoming_df.cache()
        entity_incoming_filled_df.unpersist()
        entity_incoming_prematched_df.unpersist()
        entity_incoming_exact_matched_df.unpersist()
        entity_incoming_recordlinkage_matched_df.unpersist()

        print(f'Writing {entity_incoming_df.count()} incoming records to primary entity table:'
            f' {consume_database}.{entity_primary_table_name}')

        # Creates a temporary view using the DataFrame
        entity_incoming_df.createOrReplaceTempView('entity_incoming')

        update_list = ', '.join([ f"{entity_primary_table_name}.{field.name} = entity_incoming.{field.name}"
            for field in entity_incoming_df.schema ])

        # Bandit check suppressed due to parameters being from trusted, known source
        spark_sql = f"""MERGE INTO
            {args['iceberg_catalog']}.`{consume_database}`.`{entity_primary_table_name}`
            USING entity_incoming
            ON {entity_primary_table_name}.{global_id_field} = entity_incoming.{global_id_field}
            WHEN MATCHED THEN UPDATE SET {update_list}
            WHEN NOT MATCHED THEN INSERT *
            """     # nosec B608
        spark.sql(spark_sql)
        lineage.update_lineage(entity_primary_df, args['source_key'], 'sparksql', transform=[ spark_sql ])

    job.commit()
    print('Data successfully written to Consume Primary Entity table; job complete')


if __name__ == '__main__':
    main()