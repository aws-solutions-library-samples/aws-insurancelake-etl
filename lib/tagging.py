# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import aws_cdk as cdk

from .configuration import (
    get_logical_id_prefix, get_resource_name_prefix, get_all_configurations
)


COST_CENTER = 'COST_CENTER'
TAG_ENVIRONMENT = 'TAG_ENVIRONMENT'
TEAM = 'TEAM'
APPLICATION = 'APPLICATION'


def tag(stack: cdk.Stack, target_environment: str):
    """Adds a set of tags to all constructs in the stack

    stack
        CDK stack construct to tag
    target_environment
        The environment the stack is deployed to (for tag values)
    """
    cdk.Tags.of(stack).add(*get_tag(COST_CENTER, target_environment))
    cdk.Tags.of(stack).add(*get_tag(TAG_ENVIRONMENT, target_environment))
    cdk.Tags.of(stack).add(*get_tag(TEAM, target_environment))
    cdk.Tags.of(stack).add(*get_tag(APPLICATION, target_environment))


def get_tag(tag_name: str, target_environment: str) -> dict:
    """Get a tag for a given parameter and target environment.

    tag_name
        The name of the tag (must exist in tag_map)
    target_environment
        The environment the tag is applied to (for tag values)

    Raises
    ------
    AttributeError
        If target environment or tag name is not present in the tag_map below

    Returns
    -------
    dict
        key, value pair for each tag and tag value
    """
    mapping = get_all_configurations()
    if target_environment not in mapping:
        raise AttributeError(f'Target environment {target_environment} not found in environment configurations')

    logical_id_prefix = get_logical_id_prefix()
    resource_name_prefix = get_resource_name_prefix()
    tag_map = {
        COST_CENTER: [
            f'{resource_name_prefix}:cost-center',
            f'{logical_id_prefix}Etl',
        ],
        TAG_ENVIRONMENT: [
            f'{resource_name_prefix}:environment',
            target_environment,
        ],
        TEAM: [
            f'{resource_name_prefix}:team',
            f'{logical_id_prefix}Admin',
        ],
        APPLICATION: [
            f'{resource_name_prefix}:application',
            f'{logical_id_prefix}Etl',
        ],
    }
    if tag_name not in tag_map:
        raise AttributeError(f'Tag map does not contain a key/value for {tag_name}')

    return tag_map[tag_name]