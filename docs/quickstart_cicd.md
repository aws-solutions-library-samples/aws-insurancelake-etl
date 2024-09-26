---
title: Quickstart with CI/CD
parent: Getting Started
nav_order: 3
last_modified_date: 2024-09-26
---
# InsuranceLake Quickstart with CI/CD Guide

If you've determined that InsuranceLake is a good starting point for your own serverless data lake and would like to rapidly iterate through development cycles with one or more teams, we recommend deploying with a CI/CD pipeline. Follow the steps in this section to create your CodePipeline stack, and to use it to deploy the InsuranceLake resources:

1. If this is your first time using the application, follow the [Python/CDK Basics](#pythoncdk-basics) steps.
1. Use a terminal or command prompt and change the working directory to the location of the infrastruture code.
    ```bash
    cd aws-insurancelake-infrastructure
    ```
1. Open the `lib/configuration.py` file using `vi` or `nano`.
    ```bash
    nano +81 lib/configuration.py
    ```
    ```bash
    vi +81 lib/configuration.py
    ```
1. Review the `local_mapping` structure in the `get_local_configuration()` function.
    - Specifically, the Regions and account IDs should make sense for your environments. These values in the repository (not locally) will be used by AWS CodeCommit and need to be maintained in the repository.
    - The values for the Test and Production environments can be ommitted at this time, because we will only be deploying the Deployment and Development environments.
    - You must explicitly specify the account and Region for each environment so that the infrastructure virtual private clouds (VPCs) get three Availability Zones (if the Region has them available). [Review the reference documentation](https://docs.aws.amazon.com/cdk/api/v2/docs/aws-cdk-lib.aws_ec2.Vpc.html#maxazs).
1. Deploy the CodeCommit repository stack.
    ```bash
    cdk deploy Deploy-InsuranceLakeInfrastructureMirrorRepository
    ```
    - While this stack is designed for a mirror repository, it can also be used as a main repository for your InsuranceLake code. You can follow links to help setup other repository types here:
        - [GitHub](developer_guide.md#aws-codepipeline-and-github-integration)
        - [Bitbucket](https://complereinfosystem.com/2021/02/26/atlassian-bitbucket-to-aws-codecommit-using-bitbucket-pipelines/)
        - [GitLab](https://klika-tech.com/blog/2022/07/12/repository-mirroring-gitlab-to-codecommit/)
1. If you plan to use CodeCommit as the main repository, [install the Git CodeCommit Helper](https://docs.aws.amazon.com/codecommit/latest/userguide/setting-up-git-remote-codecommit.html):
    ```bash
    sudo pip install git-remote-codecommit
    ```
1. Initialize git, create a develop branch, perform initial commit, and push to remote.
    - We are using the develop branch because the Dev environment deployment is triggered by commits to the develop branch.
    - Edit the repository URL to correspond to your version control system if you are not using CodeCommit.
    ```bash
    git init
    git branch -M develop
    git add .
    git commit -m 'Initial commit'
    git remote add origin codecommit::us-east-2://aws-insurancelake-infrastructure
    git push --set-upstream origin develop
    ```
1. Deploy the Infrastructure CodePipeline stack in the development environment (one stack).
    ```bash
    cdk deploy Dev-InsuranceLakeInfrastructurePipeline
    ```
1. Review and accept IAM credential creation for the CodePipeline stack.
    - Wait for deployment to finish (approximately 5 minutes).
1. Open [CodePipeline](https://console.aws.amazon.com/codesuite/codepipeline/pipelines) in the AWS Console and select the `dev-insurancelake-infrastructure-pipeline` Pipeline.
    - The first run of the pipeline starts automatically after the Pipeline stack is deployed.
    ![Select Infrastructure CodePipeline](codepipeline_infrastructure_select_pipeline.png)
1. Monitor the status of the pipeline until complete.
    ![Infrastructure CodePipeline progress](codepipeline_infrastructure_monitor_progress.png)
1. Switch the working directory to the location of the ETL code.
    ```bash/
    cd ../aws-insurancelake-etl
    ```
1. In `lib/configuration.py`, review the `local_mapping` structure in the `get_local_configuration()` function, and ensure this matches the Infrastructure configuration, or differs if specifically needed.
1. Deploy the CodeCommit repository stack.
    ```bash
    cdk deploy Deploy-InsuranceLakeEtlMirrorRepository
    ```
1. Initialize git, create a develop branch, perform initial commit, and push to remote.
    - We are using the develop branch because the Dev environment deployment is triggered by commits to the develop branch.
    - Edit the repository URL to correspond to your version control system if you are not using CodeCommit.
    ```bash
    git init
    git branch -M develop
    git add .
    git commit -m 'Initial commit'
    git remote add origin codecommit::us-east-2://aws-insurancelake-etl
    git push --set-upstream origin develop
    ```
1. Deploy the ETL CodePipeline stack in the development environment (one stack).
    ```bash
    cdk deploy Dev-InsuranceLakeEtlPipeline
    ```
1. Review and accept IAM credential creation for the CodePipeline stack.
    - Wait for deployment to finish (approximately 5 minutes).
1. Open [CodePipeline](https://console.aws.amazon.com/codesuite/codepipeline/pipelines) in the AWS Console and select the `dev-insurancelake-etl-pipeline` Pipeline.
    - The first run of the pipeline starts automatically after the Pipeline stack is deployed.
    ![Select ETL CodePipeline](codepipeline_etl_select_pipeline.png)
1. Monitor the status of the pipeline until completed.
    ![ETL CodePipeline progress](codepipeline_etl_monitor_progress.png)