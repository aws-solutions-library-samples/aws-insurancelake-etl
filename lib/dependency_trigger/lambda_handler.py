# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import os
import json
import logging
from dateutil import parser as dateparser
import boto3
import botocore
from boto3.dynamodb.conditions import Attr

# Logger initiation
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def update_etl_job_run(
        audit_table_name: str,
        execution_id: str,
        status: str,
        update_timestamp: str,
    ):
    """Function to update the job status and timestamp in a DynamoDB job audit table

    Parameters
    ----------
    audit_table_name : str
        Name of DynamoDB table
    execution_id : str
        Execution ID of job to update (UUID, used for the table partition key)
    status : str
        The job status with which to update
    update_timestamp : str
        The job last updated timestamp with which to update

    Raises
    ------
    RuntimeError
        If DynamoDB table item update fails for any reason
    """
    dynamo_client = boto3.resource('dynamodb')
    try:
        # Update audit table
        table = dynamo_client.Table(audit_table_name)
        table.update_item(
            Key={
                'execution_id': execution_id
            },
            UpdateExpression='set job_last_updated_timestamp=:lut,job_latest_status=:sts',
            ExpressionAttributeValues={
                ':sts': status,
                ':lut': update_timestamp,
            },
            ReturnValues='UPDATED_NEW'
        )
    except botocore.exceptions.ClientError as error:
        raise RuntimeError(f'DynamoDB update_item failed: {error}')

    logger.info('record_etl_job_run() completed successfully')


def get_queued_jobs(audit_table_name: str, dependency_key: str):
    """Function to get a list of queued ETL jobs from DynamoDB matching a dependency source key

    Parameters
    ----------
    audit_table_name : str
        Name of DynamoDB table
    source_key : str
        Source key of the dependent workflow to match

    Returns
    -------
    dict : collection of items matching queued status

    Raises
    ------
    RuntimeError
        If DynamoDB table scan fails for any reason
    """
    dynamo_client = boto3.resource('dynamodb')
    try:
        # Update audit table
        table = dynamo_client.Table(audit_table_name)
        result = table.scan(
            FilterExpression=
                Attr('job_latest_status').eq('QUEUED') & \
                Attr('dependency_key').eq(dependency_key),
            ProjectionExpression='execution_id, sfn_execution_name, sfn_input, source_key',
        )
    except botocore.exceptions.ClientError as error:
        raise RuntimeError(f'DynamoDB table scan failed: {error}')

    logger.info(f'Get queued jobs completed successfully with {result['Count']} jobs matched')
    return result['Items']


def lambda_handler(event: dict, _) -> dict:
    """Lambda function's entry point. This function receives a notification
    from SNS, checks DynamoDB for queued jobs, logs the jobs starting,
    and initiates the State Machine for any matching jobs.

    Parameters
    ----------
    event
        S3 PutObject event dictionary

    Returns
    -------
    dict
        Lambda result dictionary
    """
    sfn_client = boto3.client('stepfunctions')
    sfn_arn = os.environ['SFN_STATE_MACHINE_ARN']
    audit_table_name = os.environ['DYNAMODB_TABLE_NAME']

    logger.debug('Event: ' + str(event))

    lambda_message = event['Records'][0]
    message_subject = lambda_message['Sns']['Subject']
    message_data = json.loads(lambda_message['Sns']['Message'])
    notification_timestamp = dateparser.parse(lambda_message['Sns']['Timestamp']). \
        strftime('%Y-%m-%d %H:%M:%S.%f')
    source_key = message_data['source_key']

    logger.info(f'Received ETL workflow notification for {source_key}, subject: {message_subject}')

    if 'Completed' not in lambda_message['Sns']['Subject']:
        return {
            'statusCode': 400,
            'body': f'Ignoring not completed job message'
         }

    jobs = get_queued_jobs(audit_table_name, source_key)
    if not jobs:
        return {
            'statusCode': 400,
            'body': f'No action; no matching queued executions found with dependency on source key'
        }

    for job in jobs:
        logger.info(f"Starting queued job with execution ID: {job['execution_id']}" \
            f" and source_key: {job['source_key']}")
        update_etl_job_run(audit_table_name, job['execution_id'], 'STARTED', notification_timestamp)

        try:
            sfn_response = sfn_client.start_execution(
                stateMachineArn=sfn_arn,
                name=job['sfn_execution_name'],
                input=job['sfn_input'],
            )
            logger.info(f'SFN Reponse: {sfn_response}')

        except botocore.exceptions.ClientError as error:
            raise RuntimeError(f'Step Function StartExecution failed: {error}')

    return {
        'statusCode': 200,
        'body': f'Step function triggered successfully for {len(jobs)} queued jobs matching' \
            f' dependent source key {source_key}'
    }