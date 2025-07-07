# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import boto3
import time


def athena_execute_query(database: str, query: str, result_bucket: str,
        max_attempts: int = 30) -> str:
    """Function to execute query using Athena boto client, loop until
    result is returned, and return status.

    Parameters
    ----------
    database
        Athena database in which to run the query
    query
        Single or multi-line query to run
    result_bucket
        S3 bucket path to store query results
    max_attempts
        Number of loops (1s apart) to attempt to get query status, default 30

    Returns
    -------
    str
        Status of query result execution
    """
    athena = boto3.client('athena')

    query_response = athena.start_query_execution(
            QueryExecutionContext={ 'Database': database.lower() },
            QueryString=query,
            ResultConfiguration={ 'OutputLocation': result_bucket }
        )
    print(f'Executed query response: {query_response}')

    # Valid "state" Values: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
    attempts = max_attempts
    status = 'QUEUED'
    while status in [ 'QUEUED', 'RUNNING' ]:
        if attempts == 0:
            raise RuntimeError('athena_execute_query() exceeded max_attempts waiting for query')

        query_details = athena.get_query_execution(
            QueryExecutionId=query_response['QueryExecutionId'])
        print(f'Get query execution response: {query_details}')
        status = query_details['QueryExecution']['Status']['State']
        if status in [ 'FAILED', 'CANCELED' ]:
            # Raise errors that are not raised from StartQueryExecution
            raise RuntimeError("athena_execute_query() failed with query engine error: "
                f"{query_details['QueryExecution']['Status'].get('StateChangeReason', '')}")
        if status != 'SUCCEEDED':
            time.sleep(1)   # nosemgrep
        attempts -= 1

    return status


def redshift_execute_query(database: str, query: str, # nosec B107
        cluster_id: str = None, workgroup_name: str = None, secret_arn: str = None,
        max_attempts: int = 60) -> str:
    """Function to execute query using Redshift Data API, loop until
    result is returned, and return status.

    Parameters
    ----------
    database
        Redshift database in which to run the query
    query
        Single SQL statement to run
    cluster_id
        Redshift cluster identifier (required if not using workgroup_name)
    workgroup_name
        Redshift Serverless workgroup name (required if not using cluster_id)
    secret_arn
        Secrets Manager secret to use for authentication (optional, otherwise IAM permissions are used)
    max_attempts
        Number of loops (1s apart) to attempt to get query status, default 60

    Returns
    -------
    str
        Status of query execution
    """
    redshift_data = boto3.client('redshift-data')

    # Let redshift data API handle exceptions if authentication is not provided correctly
    auth_parameters = { 'WorkgroupName': workgroup_name }
    if cluster_id:
        auth_parameters = { 'ClusterIdentifier': cluster_id }
    if secret_arn:
        auth_parameters['SecretArn'] = secret_arn

    query_response = redshift_data.execute_statement(
        Database=database,
        Sql=query,
        **auth_parameters,
    )
    print(f'Executed query response: {query_response}')

    # Valid Status Values: SUBMITTED | PICKED | STARTED | FINISHED | FAILED | ABORTED
    attempts = max_attempts
    status = 'SUBMITTED'
    while status in ['SUBMITTED', 'PICKED', 'STARTED']:
        if attempts == 0:
            raise RuntimeError('redshift_execute_query() exceeded max_attempts waiting for query')

        query_details = redshift_data.describe_statement(Id=query_response['Id'])
        print(f'Get query execution response: {query_details}')
        status = query_details['Status']        
        if status in ['FAILED', 'ABORTED']:
            error_message = query_details.get('Error', '')
            raise RuntimeError(f"redshift_execute_query() failed with error: {error_message}")
        if status != 'FINISHED':
            time.sleep(1)	# nosemgrep
        attempts -= 1

    return status