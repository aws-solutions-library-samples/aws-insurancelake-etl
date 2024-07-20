# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
"""
InsuranceLake implementation of Data Lineage generation class. This class handles different operations
on a dataset and registers them in a DynamoDB. There is an incremental sequence number added 
to the records in the order the operations occur.

"""
import uuid
import json
import boto3
from datetime import datetime
import dateutil.tz
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import sum, input_file_name

class DataLineageGenerator:
    """The InsuranceLake custom data lineage class

    Methods: generateuuid
             insert_table
             update_lineage
    """
    def __init__(self, args: dict, uniid: str = None):
        """
        Parameters
        ----------
        args
            Glue job parameters; required values: JOB_NAME, JOB_RUN_ID, state_machine_name, execution_id, data_lineage_table
        uniid: optional
            Optional UUID to use for lineage events; creates a new UUID if not supplied
        """
        if 'data_lineage_table' in args:
            dynamodb = boto3.resource('dynamodb')

            self.table = dynamodb.Table(args['data_lineage_table'])
            self.job_id = args['JOB_RUN_ID']
            self.job_name  = args['JOB_NAME']
            self.step_function_name = args['state_machine_name']
            self.step_function_execution_id = args['execution_id'] 
            self.uniid = uniid if uniid is not None else self.generateuuid()
            self.count = 1
        else:
            # Allow the calling code to toggle lineage on/off by specifying a table (or not)
            self.table = None

    def generateuuid(self) -> str:
        """Generates a new UUID
        """
        return(str(uuid.uuid4()))

    def generatecounts(self, df) -> dict:
        """Generates a dictionary of dataframe counts/size
        """
        return({
            'row': df.count(),
            'column': len(df.columns),
        })

    def insert_table(self, dataset: str, operation: str, transform_spec: any):
        """Inserts the lineage event to DynamoDB data lineage table

        Parameters
        ----------
        dataset
            Name of the dataset on which the operation has been performed
        operation
            Information about the type of operation; e.g. read, write, numericaudit, or other transform name
        transform_spec
            Detailed information about the operation
        """
        now = datetime.now(tz=dateutil.tz.gettz('UTC'))

        self.table.put_item(
            Item = {
                'step_function_execution_id' : self.step_function_execution_id,
                'job_id_operation_seq': f'{self.job_id}-{self.count}',
                'operation_seq' : self.count,
                'job_id': self.job_id,
                'job_name': self.job_name,
                'dataset' : dataset,
                'uuid' : self.uniid,
                'time_of_operation' : str(now.strftime('%Y-%m-%d %H:%M:%S')),
                'dataset_operation' : operation,
                'step_function_name' : self.step_function_name,
                'info' : json.dumps(transform_spec)
            }
        )
        self.count += 1

    def update_lineage(self, df: DataFrame, dataset: str, operation: str, **kwargs):
        """Prepares the event data to be recorded in DynamoDB data lineage table

        Parameters
        ----------
        df
            Spark DataFrame to use for lineage data
        dataset
            Name of the dataset on which the operation has been performed
        operation
            Information about the type of operation; e.g. read, write, numericaudit, or other transform name
        kwargs: optional
            List of optional keywords used for specific operations; most operations expect 'transform'
        """
        if not self.table:
            # No table is defined so no data lineage is possible
            return

        try:
            dataset_filename = df.select(input_file_name()).limit(1).collect()[0][0]
        except:
            # input_file_name() does not support more than one source, some source types do not capture input_file_name
            dataset_filename = 'N/A'

        # Read / Write
        if operation == 'read':
            lineage_info = {
                'datasetname': dataset_filename,
                'dataset_counts': self.generatecounts(df),
                'schema': str(df.dtypes),
            }
            self.insert_table(dataset, operation, lineage_info)

        elif operation == 'write':
            lineage_info = {
                'datasetname': dataset_filename,
                'format': kwargs['format'],
                'dataset_counts': self.generatecounts(df),
                'schema': str(df.dtypes),
            }
            self.insert_table(dataset, operation, lineage_info)

        # Numeric Audit
        elif operation == 'numericaudit': 
            lineage_info = {
                'dataset_counts': self.generatecounts(df),
                'field_total': {
                    # Sum any double or decimal columns; convert to string explicitly to not rely on the JSON parser
                    field_name: str(df.select(sum(f'`{field_name}`')).collect()[0][0])
                        for field_name, field_type in df.dtypes
                            if field_type == 'double' or field_type.startswith('decimal')
                }
            }
            self.insert_table(dataset, operation, lineage_info)

        # TODO: Add special handling for multilookup: create one row for each return_attribute for column level lineage

        # Mapping
        elif operation == 'mapping':
            for map_record in kwargs['map']:
                if map_record['destname'].lower() == 'null':
                    self.insert_table(dataset, 'dropped', { 'dropped': map_record['sourcename'] })
                elif map_record.get('threshold'):
                    self.insert_table(dataset, 'fuzzymapping', map_record)
                else:
                    self.insert_table(dataset, operation, map_record)

        # All other transforms (that do not require special handling)
        else:
            for spec in kwargs['transform']:
                self.insert_table(dataset, operation, { operation: spec })