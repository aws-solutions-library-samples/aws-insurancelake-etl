# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import json
import boto3
import botocore
import os
import logging
import uuid
import re
from dateutil import parser as dateparser
from urllib.parse import unquote_plus

# Logger initiation
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def record_etl_job_run(audit_table_name: str, item: dict):
    """Function to insert entry in dynamodb table for audit trail

    table_name
        DynamoDB table name to use for job audit data
    item
        Dictionary of parameters to record in table
    """
    dynamo_client = boto3.resource('dynamodb')
    try:
        table = dynamo_client.Table(audit_table_name)
        table.put_item(Item=item)
    except botocore.exceptions.ClientError as error:
        raise RuntimeError(f'record_etl_job_run() DynamoDB put_item failed: {error}')

    logger.info('record_etl_job_run() completed successfully')


def lambda_handler(event: dict, _) -> dict:
    """Lambda function's entry point. This function receives a PutObject event
    from S3, prepares Step Function State machine inputs, logs the job start in
    DynamoDB table, and initiates the State Machine

    Parameters
    ----------
    event
        S3 PutObject event dictionary

    Returns
    -------
    dict
        Lambda result dictionary
    """
    s3_client = boto3.client('s3')
    sfn_client = boto3.client('stepfunctions')
    sfn_arn = os.environ['SFN_STATE_MACHINE_ARN']
    audit_table_name = os.environ['DYNAMODB_TABLE_NAME']
    glue_scripts_bucket_name = os.environ['GLUE_SCRIPTS_BUCKET_NAME']

    logger.debug(event)
    lambda_message = event['Records'][0]
    source_bucket_name = lambda_message['s3']['bucket']['name']
    object_full_path = unquote_plus(lambda_message['s3']['object']['key'])
    event_time = dateparser.parse(lambda_message['eventTime'])
    principal_id = lambda_message['userIdentity']['principalId']
    source_ipaddress = lambda_message['requestParameters']['sourceIPAddress']

    if object_full_path.endswith('/'):
        logger.error(f'Ignoring folder creation: {object_full_path}')
        return {
            'statusCode': 400,
            'body': json.dumps('Received PutObject for folder; cannot process')
        }

    try:
        # Bucket/key format: s3://<bucketname>/<source_system_name>/<table_name>
        object_file_dir = os.path.dirname(object_full_path)
        object_base_file_name = os.path.basename(object_full_path)
        # First object/folder name will be used as source system/database name
        # Second object/folder name will be used as table name
        path_components = object_file_dir.split('/')
        object_source_system_name = path_components[0]
        object_table_name = path_components[1]
    except IndexError:
        logger.error(f'File object {object_full_path} cannot be processed without 2 levels of directory structure')
        return {
            'statusCode': 400,
            'body': json.dumps(f'File object {object_full_path} ignored due to unexpected naming convention')
        }

    # Setup partitions from the event time, or user-provided overrides
    p_year = event_time.strftime('%Y')
    p_month = event_time.strftime('%m')
    p_day = event_time.strftime('%d')
    try:
        # Check if uploader has provided folders to override the default partitions
        p_year = path_components[2]
        p_month = path_components[3]
        p_day = path_components[4]
    except IndexError:
        # Keep any defaults
        pass

    entity_match_spec = 'etl/transformation-spec/' + object_source_system_name + '-' + 'entitymatch.json'
    result = s3_client.list_objects_v2(Bucket=glue_scripts_bucket_name, Prefix=entity_match_spec)
    entity_match = False
    if 'Contents' in result:
        entity_match = True
        logger.info(f'Entity Match JSON {entity_match_spec} found; enabling Entity Match job')

    dependent_workflow_spec = \
        f'etl/transformation-spec/{object_source_system_name}-{object_table_name}-dependent.json'
    try:
        dependency_result = s3_client.get_object(Bucket=glue_scripts_bucket_name, Key=dependent_workflow_spec)
        if 'Body' in dependency_result:
            dependencies = json.loads(dependency_result['Body'].read().decode('utf-8')).get('depends_on')
            logger.info(f'Dependent Workflow JSON {dependent_workflow_spec} found; queuing job execution')
    except s3_client.exceptions.NoSuchKey:
        dependencies = {}

    logger.info(f'Bucket: {source_bucket_name}')
    logger.info(f'Key: {object_source_system_name}/{object_table_name}')
    logger.info(f'Source System Name: {object_source_system_name}')
    logger.info(f'Table Name: {object_table_name}')
    logger.info(f'File Path: {object_file_dir}')
    logger.info(f'File Base Name: {object_base_file_name}')
    logger.info(f'State Machine ARN: {sfn_arn}')
    logger.info(f'Date used for partitioning: {p_year}-{p_month}-{p_day}')

    # Ensure state machine execution logging by following CloudWatch log group name constraints
    safe_object_base_file_name = re.sub('[^a-zA-Z0-9_-]', '', object_base_file_name)
    # Max length of execution name is 80 characters, and 21 will be added, so truncate to 59
    execution_name = f"{safe_object_base_file_name[:59]}-{event_time.strftime('%Y%m%d%H%M%S%f')}"
    logger.info(f'Step Function Execution Name: {execution_name}')
    execution_id = str(uuid.uuid4())
    execution_input = json.dumps(
        {
            'target_database_name': object_source_system_name,
            'source_bucketname': source_bucket_name,
            'source_key': object_source_system_name + '/' + object_table_name,
            'source_path': object_file_dir,
            'base_file_name': object_base_file_name,
            'p_year': p_year,
            'p_month': p_month,
            'p_day': p_day,
            'table_name': object_table_name,
            'execution_id': execution_id,
            'entity_match': entity_match,
        }
    )
    logger.info(f'SFN Input: {execution_input}')

    if not dependencies:
        try:
            sfn_response = sfn_client.start_execution(
                stateMachineArn=sfn_arn,
                name=execution_name,
                input=execution_input,
            )
            logger.debug(f'SFN Reponse: {sfn_response}')
        except botocore.exceptions.ClientError as error:
            raise RuntimeError(f'Step Function StartExecution failed: {error}')
        status = 'STARTED'
        return_message = f'Step function triggered successfully'
    else:
        status = 'QUEUED'
        return_message = f'Step function execution queued due to dependent workflow'

    item = {
        'execution_id': execution_id,
        'sfn_execution_name': execution_name,
        'sfn_arn': sfn_arn,
        'sfn_input': execution_input,
        'source_key': f'{object_source_system_name}/{object_table_name}',
        'job_latest_status': status,
        'job_start_date': event_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'job_last_updated_timestamp': event_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        'principal_id': principal_id,
        'source_ipaddress': source_ipaddress,
        # TODO: Add support for multiple dependencies, including how to ensure that all of them are met
        'dependency_key': list(dependencies)[0] if dependencies else None,
    }
    record_etl_job_run(audit_table_name, item)

    return {
        'statusCode': 200,
        'body': json.dumps(return_message)
    }