# Copyright Amazon.com and its affiliates; all rights reserved. This file is Amazon Web Services Content and may not be duplicated or distributed without permission.
# SPDX-License-Identifier: MIT-0
import aws_cdk as cdk
from constructs import Construct
import aws_cdk.aws_iam as iam
import aws_cdk.aws_codecommit as CodeCommit

from .configuration import (
    CODECOMMIT_MIRROR_REPOSITORY_NAME, CODECOMMIT_MIRROR_REPOSITORY_NAME,
	get_logical_id_prefix, get_resource_name_prefix, get_all_configurations
)


class CodeCommitStack(cdk.Stack):

    def __init__(
        self, scope: Construct, construct_id: str,
        target_environment: str,
        **kwargs
    ):
        """CloudFormation stack to create CodeCommit mirror repository, if needed.

        Parameters
        ----------
        scope
            Parent of this stack, usually an App or a Stage, but could be any construct
        construct_id
            The construct ID of this stack; if stackName is not explicitly defined,
            this ID (and any parent IDs) will be used to determine the physical ID of the stack
        target_environment
            The target environment for the CodeCommit repository
        kwargs: optional
            Optional keyword arguments to pass up to parent Stack class
        """
        super().__init__(scope, construct_id, **kwargs)

        self.mappings = get_all_configurations()
        self.create_mirror_repository(target_environment)

    def create_mirror_repository(self, target_environment):
        """Creates CodeCommit repository to mirror source code from an unsupported version
        control system (e.g. Bitbucket, Gitlab) and integrate with CodePipeline.

        Parameters
        ----------
        target_environment
            The target environment for the CodeCommit repository
        """
        logical_id_prefix = get_logical_id_prefix()
        resource_name_prefix = get_resource_name_prefix()

        repo = CodeCommit.Repository(
            self, 
            f'{target_environment}{logical_id_prefix}EtlMirrorRepository',
            description='InsuranceLake ETL source code repository mirror for CodePipeline integration',
            repository_name=self.mappings[target_environment][CODECOMMIT_MIRROR_REPOSITORY_NAME],
        )

        git_mirror_user = iam.User(
            self,
            f'{logical_id_prefix}EtlGitExternalMirrorUser',
            user_name=f'{resource_name_prefix}-etl-git-mirror',
        )

        git_mirror_user.attach_inline_policy(
            iam.Policy(
                self,
                f'{logical_id_prefix}EtlCodecommitPushPullPolicy',
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            'codecommit:GitPull',
                            'codecommit:GitPush',
                        ],
                        resources=[
                            repo.repository_arn
                        ],
                    )
                ]
            )
        )

        cdk.CfnOutput(
            self,
            f'{target_environment}{logical_id_prefix}CodeCommitMirrorRepositoryName',
            value=repo.repository_name,
            export_name=f'{target_environment}EtlMirrorRepository'
        )

        cdk.CfnOutput(
            self,
            f'{target_environment}{logical_id_prefix}CodeCommitMirrorUser',
            value=git_mirror_user.user_name,
            export_name=f'{target_environment}EtlMirrorUser'
        )