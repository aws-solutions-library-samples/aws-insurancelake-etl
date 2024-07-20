# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import json
import boto3
import botocore
import os
import logging
import os.path
from datetime import datetime
import dateutil.tz

# Logger initiation
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event: dict, _) -> dict:
    """Lambda function's entry point. This function receives a success/fail
    event from Step Functions State machine, transforms error message, and
    updates DynamoDB table.

    Parameters
    ----------
    event
        State Machine event dictionary

    Returns
    -------
    dict
        Lambda result dictionary
    """
    dynamo_client = boto3.resource('dynamodb')
    audit_table_name = os.environ['DYNAMODB_TABLE_NAME']
    print(event)
    print(event['Input']['execution_id'])
    execution_id = event['Input']['execution_id']

    # Capturing the current time in UTC
    now = datetime.now(tz=dateutil.tz.gettz('UTC'))
    audit_message_timestamp = now.strftime('%Y-%m-%d %H:%M:%S.%f')
    return_message = ''

    if 'JobRunState' in event['Input']['taskresult']:
        print(f"JobRunState exists: {event['Input']['taskresult']['JobRunState']}")
        status = event['Input']['taskresult']['JobRunState']

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
                    ':lut': audit_message_timestamp,
                },
                ReturnValues='UPDATED_NEW'
            )
            return_message = f'status {status}'
        except botocore.exceptions.ClientError as error:
            raise RuntimeError(f'DynamoDB update_item failed: {error}')
    else:
        print('JobRunState does not exist, assuming FAILED')
        status = 'FAILED'
        try:
            error_event = json.loads(event['Input']['taskresult']['Cause'])
            error_message = error_event.get('ErrorMessage', error_event['JobRunState'])
        except json.JSONDecodeError:
            error_message = event['Input']['taskresult']['Cause']
        print(error_message)

        try:
            # Update table
            table = dynamo_client.Table(audit_table_name)
            table.update_item(
                Key={
                    'execution_id': execution_id
                },
                UpdateExpression=
                    'set job_last_updated_timestamp=:lut,job_latest_status=:sts,error_message=:emsg',
                ExpressionAttributeValues={
                    ':sts': status,
                    ':lut': audit_message_timestamp,
                    ':emsg': error_message,
                },
                ReturnValues='UPDATED_NEW'
            )
            return_message = f'status {status} {error_message}'
        except botocore.exceptions.ClientError as error:
            raise RuntimeError(f'DynamoDB UpdateItem failed: {error}')

    return {
        'statusCode': 200,
        'body': f'DynamoDB status updated with {return_message}'
    }