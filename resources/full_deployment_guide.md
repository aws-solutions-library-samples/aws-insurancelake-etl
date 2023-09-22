# Full Deployment

This section provides complete instructions for three deployment environments, and a separate central deployment account for pipelines.

---

* [Explanation](#explanation)
  * [Logistical Requirements](#logistical-requirements)
  * [Self-mutating Pipelines via CDK](#self-mutating-pipelines-via-cdk)
  * [Centralized Deployment](#centralized-deployment)
  * [Continuous Delivery of Data Lake Infrastructure using CDK Pipelines](#continuous-delivery-of-data-lake-infrastructure-using-cdk-pipelines)
* [Deployment](#deployment)
  * [Software Installation](#software-installation)
  * [AWS Environment Bootstrapping](#aws-environment-bootstrapping)
  * [Application Configuration](#application-configuration)
  * [AWS CodePipeline and GitHub Integration](#aws-codepipeline-and-github-integration)
  * [Deploying CDK Stacks](#deploying-cdk-stacks)

---

## Explanation

### Logistical Requirements

* **Four AWS accounts for full deployment** One of them acts like a central deployment account. The other three are for development, test, and production accounts. To test this solution with single account refer to the [Quickstart section of the README](./README.md#quickstart) for detailed instructions.

* **Cross-account permissions for pipeline** Create a service role for CodePipeline with cross-account permissions. Refer to this [knowledge base article](https://aws.amazon.com/premiumsupport/knowledge-center/codepipeline-deploy-cloudformation/) for details.

* **Number of branches on your GitHub repo** You need to start with at least one branch for e.g. main to start using this solution. test and prod branches can be added at the beginning or after the deployment of data lake infrastructure on dev environment.

* **Administrator privileges** You need to administrator privileges to bootstrap your AWS environments and complete initial deployment. Usually, these steps can be performed by a DevOps administrator of your team. After these steps, you can revoke administrative privileges. Subsequent deployments are based on self-mutating natures of CDK Pipelines.

* **AWS Region selection** We recommend you to use the same AWS region (e.g. us-east-2) for deployment, dev, test, and prod accounts for simplicity. However, this is not a hard requirement.

---

### Self-mutating Pipelines via CDK

The pipeline you have created using the CDK Pipelines module is self-mutating. That means the pipeline itself is infrastructure-as-code and can be changed as part of the deployment. During the build stage the pipeline will determine its own stack is changed, redeploy the pipeline, and repeat the build stage using the new pipeline definition.

---

### Centralized Deployment

Let us see how we deploy data lake ETL workloads from a central deployment account to multiple AWS environments such as dev, test, and prod. As shown in the figure below, we organize **Data Lake ETL source code** into three branches - dev, test, and production. We use a dedicated AWS account to create CDK Pipelines. Each branch is mapped to a CDK pipeline and it turn mapped to a target environment. This way, code changes made to the branches are deployed iteratively to their respective target environment.

To demonstrate this solution, we need 4 AWS accounts as follows:

  1. Central deployment account to create CDK pipelines
  1. Dev account for one or more development data lakes
  1. Test account for testing or staging the data lake
  1. Prod account for the production data lake

Figure below represents the centralized deployment model.

![Aws-cdk-datalake-branch_strategy](./Aws-cdk-pipelines-blog-datalake-branch_strategy_etl.png)

There are few interesting details to point out here:

  1. Each source code repository should be organized into three branches, one for each environment (main branch is often used for production)
  1. Each branch is mapped to a CDK pipeline and a target environment. This way, code changes made to the branches are deployed iteratively to their respective target environment
  1. From CDK perspective, we apply the the following bootstrapping principles
      1. the central deployment account will utilize a standard bootstrap
      1. each target account will require a cross account trust policy to allow access from the centralized deployment account

---

### Continuous Delivery of Data Lake Infrastructure using CDK Pipelines

Figure below illustrates the continuous delivery of ETL resources for the data lake.

![Aws-cdk-datalake-continuous-delivery](./Aws-cdk-pipelines-blog-datalake-continuous_delivery_data_lake_etl.png)

There are few interesting details to point out here:

1. The DevOps administrator checks in the code to the repository.
1. The DevOps administrator (with elevated access) facilitates a one-time manual deployment on a target environment. Elevated access includes administrative privileges on the central deployment account and target AWS environments.
1. CodePipeline periodically listens to commit events on the source code repositories. This is the self-mutating nature of CodePipeline. It’s configured to work with and is able to update itself according to the provided definition.
1. Code changes made to the main branch of the repo are automatically deployed to the dev environment of the data lake.
1. Code changes to the test branch of the repo are automatically deployed to the test environment.
1. Code changes to the prod branch of the repo are automatically deployed to the prod environment.

---

## Deployment

### Software Installation

NOTE: If using AWS Cloud9, you need only fork the repository, as the other software is pre-installed.

1. **AWS CLI** - make sure you have AWS CLI configured on your system. If not, refer to [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) for more details.

1. **Python** - make sure you have Python SDK installed on your system. We recommend Python 3.9 and above.

1. **GitHub Fork** - we recommend you [fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo) so you are in control of deployed resources.

### AWS Environment Bootstrapping

Environment bootstrap is standard CDK process to prepare an AWS environment ready for deployment. Follow the steps:

1. Go to project root directory where [app.py](app.py) file exists

1. Create Python virtual environment. This is a one-time activity.

    ```bash
   python3 -m venv .venv
   ```

1. Expected output: you will see a folder with name **.venv** created in project root folder. You can run the following command to see its contents ```ls -la .venv```

   ```bash
   total 8
   drwxr-xr-x   6 user_id  staff   192 Aug 30 23:21 ./
   drwxr-xr-x  33 user_id  staff  1056 Sep 14 11:42 ../
   drwxr-xr-x  27 user_id  staff   864 Sep  8 13:11 bin/
   drwxr-xr-x   3 user_id  staff    96 Aug 30 23:21 include/
   drwxr-xr-x   3 user_id  staff    96 Aug 30 23:21 lib/
   -rw-r--r--   1 user_id  staff   328 Aug 30 23:21 pyvenv.cfg
   ```

1. Activate Python virtual environment

   ```bash
   source .venv/bin/activate
   ```

1. Install dependencies

   ```bash
   pip install -r requirements.txt
   ```

1. Expected output: run the below command and verify all dependencies are installed

   ```bash
   ls -la .venv/lib/python3.*/site-packages/
   ```

1. Before you bootstrap the **central deployment account** account, set the AWS_PROFILE environment variable

   ```bash
   export AWS_PROFILE=replace_it_with_deployment_account_profile_name_before_running
   ```

   **Important**:
   1. This command is based on the feature [Named Profiles](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html).
   1. If you want to use an alternative option then refer to [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) and [Environment variables to configure the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) for details. Be sure to follow those steps for each configuration step moving forward.

1. Bootstrap central deployment account

   **Important:** Your configured environment *must* target the central deplopyment account

   Your account(s) may have already been bootstrapped for CDK. If so, you do not need to bootstrap them again. Using the AWS Console for CloudFormation, check if a stack called `CDKToolkit` exists. If it does, you can skip this step.

   By default CDK bootstrapping will use the Administrator Access policy attached to the current session's role. If your organization requires a specific policy for CloudFormation deployment, use the `--cloudformation-execution-policies` command line option to specify the policies to attach.

   ```bash
   ./lib/prerequisites/bootstrap_deployment_account.sh
   ```

1. When you see the following text, enter **y**, and press enter/return

   ```bash
   Are you sure you want to bootstrap {
      "UserId": "user_id",
      "Account": "deployment_account_id",
      "Arn": "arn:aws:iam::deployment_account_id:user/user_id"
   }? (y/n)y
   ```

1. Expected outputs:
   1. In your terminal, you see

   ```bash
   ✅  Environment aws://deployment_account_id/us-east-2 bootstrapped.
   ```

   1. You see a stack created in your deployment account as follows

      ![bootstrap_central_deployment_account](./bootstrap_central_deployment_account_exp_output.png)

   1. You see an S3 bucket created in central deployment account. The name is like ```cdk-hnb659fds-<assets-deployment_account_id>-us-east-2```

1. Before you bootstrap the **dev** account, set the AWS_PROFILE environment variable

   ```bash
   export AWS_PROFILE=replace_it_with_dev_account_profile_name_b4_running
   ```

1. Bootstrap **dev** account

   ```bash
   cdk bootstrap
   ```

1. Before you bootstrap the **test** account, set the AWS_PROFILE environment variable

   ```bash
   export AWS_PROFILE=replace_it_with_test_account_profile_name_b4_running
   ```

1. Bootstrap test account

   **Important:** Your configured environment *must* target the Test account

   ```bash
   cdk bootstrap
   ```

1. Before you bootstrap the **prod** account, set the AWS_PROFILE environment variable

   ```bash
   export AWS_PROFILE=replace_it_with_prod_account_profile_name_b4_running
   ```

1. Bootstrap Prod account

   **Important:** Your configured environment *must* target the Prod account

   ```bash
   cdk bootstrap
   ```

---

### Application Configuration

This deployment will depend on both the [Infrastructure](link) and [ETL](link) repositories associated with the project.

Before we deploy our resources we must provide the manual variables and upon deployment the CDK Pipelines will programmatically export outputs for managed resources. Follow the below steps to setup your custom configuration:

1. Go to [configuration.py](./lib/configuration.py) and fill in values under `local_mapping` dictionary within the function `get_local_configuration` as desired.

   **Note:** You can safely commit these values to your repository

   Example:

   ```python
   local_mapping = {
      DEPLOYMENT: {
         ACCOUNT_ID: 'add_your_deployment_account_id_here',
         REGION: 'us-east-2',
         # If you use GitHub / GitHub Enterprise, this will be the organization name
         GITHUB_REPOSITORY_OWNER_NAME: 'aws-projects',
         # Use your forked repo here!
         GITHUB_REPOSITORY_NAME: 'aws-insurancelake-infrastructure',
         # This is used in the Logical Id of CloudFormation resources
         # We recommend capital case for consistency. e.g. DataLakeCdkBlog
         LOGICAL_ID_PREFIX: 'InsuranceLake',
         # This is used in resources that must be globally unique!
         # It may only contain alphanumeric characters, hyphens, and cannot contain trailing hyphens
         # E.g. unique-identifier-data-lake
         RESOURCE_NAME_PREFIX: 'insurancelake',
      },
      DEV: {
         ACCOUNT_ID: 'add_your_dev_account_id_here',
         REGION: 'us-east-2',
         LINEAGE: True,
         # Comment out if you do not need VPC connectivity
         VPC_CIDR: '10.20.0.0/24'
      },
      TEST: {
         ACCOUNT_ID: 'add_your_test_account_id_here',
         REGION: 'us-east-2',
         LINEAGE: True,
         # Comment out if you do not need VPC connectivity
         VPC_CIDR: '10.10.0.0/24'
      },
      PROD: {
         ACCOUNT_ID: 'add_your_prod_account_id_here',
         REGION: 'us-east-2',
         LINEAGE: True,
         # Comment out if you do not need VPC connectivity
         VPC_CIDR: '10.0.0.0/24'
      }
   }
   ```

1. Copy the ```configuration.py``` file to the ETL repository:

   ```bash
   cp lib/configuration.py ../aws-insurancelake-etl/lib/
   ```

### AWS CodePipeline and GitHub Integration

Integration between AWS CodePipeline and GitHub requires a personal access token. This access token is stored in Secrets Manager. This is a one-time setup and is applicable for all target AWS environments and all repositories created under the organization in GitHub.com. Follow the below steps:

**Important Note:** Do **NOT** commit these values to your repository

1. Create a personal access token in your GitHub. Refer to [Creating a personal access token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token) for details

1. Go to [configure_account_secrets.py](./lib/prerequisites/configure_account_secrets.py) and fill in the value for attribute **MY_GITHUB_TOKEN**

1. Run the below command

    ```bash
    python3 ./lib/prerequisites/configure_account_secrets.py
    ```

1. Expected output 1:

    ```bash
    Pushing secret: /DataLake/GitHubToken
    ```

1. Expected output 2: 

   ```bash
   A secret is added to AWS Secrets Manager with name **/DataLake/GitHubToken**
   ```

---

### Deploying CDK Stacks

Configure your AWS profile to target the central deployment account as an Administrator.
Using AWS CLI profiles is one way to do this ([instructions here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)). Perform the following steps:

1. Open command line (terminal)
1. Go to the infrastructure project root directory where ```cdk.json``` and ```app.py``` exist
1. Run the command ```cdk ls```
1. Check the following CloudFormation stack names listed on your terminal

   ```bash
   Deploy-InsuranceLakeInfrastructureMirrorRepository
   Dev-InsuranceLakeInfrastructurePipeline
   Test-InsuranceLakeInfrastructurePipeline
   Prod-InsuranceLakeInfrastructurePipeline
   Dev-InsuranceLakeInfrastructurePipeline/Dev/InsuranceLakeInfrastructureS3BucketZones
   Dev-InsuranceLakeInfrastructurePipeline/Dev/InsuranceLakeInfrastructureVpc
   Test-InsuranceLakeInfrastructurePipeline/Test/InsuranceLakeInfrastructureS3BucketZones
   Test-InsuranceLakeInfrastructurePipeline/Test/InsuranceLakeInfrastructureVpc
   Prod-InsuranceLakeInfrastructurePipeline/Prod/InsuranceLakeInfrastructureS3BucketZones
   Prod-InsuranceLakeInfrastructurePipeline/Prod/InsuranceLakeInfrastructureVpc
   ```

   **Note:**
   1. Here, **InsuranceLake** string literal is the value of ```LOGICAL_ID_PREFIX``` configured in [configuration.py](./lib/configuration.py)
   1. The first three stacks represent the CDK Pipeline stacks which will be created in the deployment account. For each target environment, there will be three stacks.

1. Run the command ```cdk deploy --all```

   Note that this command will only deploy the AWS Codepipelines for each environment, which will in turn, deploy the rest of the data lake stacks in the respective environment accounts. If each CodePipeline is connected to a populated repository, this deployment will start immediately.

1. Check the following expected outputs:

   1. In the deployment account's CloudFormation console, you will see the following CloudFormation stacks created:

      ![Infra_CloudFormation_stacks_in_deployment_account](./cdk_deploy_output_deployment_account_infra.png)

   1. In the deployment account, the following CodePipelines should be created successfully:

      ![Infra_CDK_pipelines_deployment_account](./cdk_pipelines_deployment_account_infra.png)

   1. In the deployment account's CodePipeline console, you will see the Pipelines triggered:

      ![CodePipelines_triggered_in_deployment_account](./image.png)

1. Wait for the infrastructure resource deployment to complete, as these are dependencies for the ETL resources

   * In the deployment account's CodePipeline console, you should see the following:
   
      ![CodePipelines_complete_summary](./dev_codepipeline_in_deployment_account_complete_summary.png)

   * In the Dev environment's CloudFormation console, CodePipeline will create the following stacks:

      ![CDK_deploy_all_dev_output](./cdk_pipelines_deployed_stacks_dev_output_infra.png)

1. Go to the **ETL** project root directory where ```cdk.json``` and ```app.py``` exist
1. Run the command ```cdk ls```
1. Check the following CloudFormation stack names listed on your terminal

   ```bash
   Deploy-InsuranceLakeEtlMirrorRepository
   Dev-InsuranceLakeEtlPipeline
   Test-InsuranceLakeEtlPipeline
   Prod-InsuranceLakeEtlPipeline
   Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlDynamoDb
   Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlGlue
   Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlStepFunctions
   Dev-InsuranceLakeEtlPipeline/Dev/InsuranceLakeEtlAthenaHelper
   Test-InsuranceLakeEtlPipeline/Test/InsuranceLakeEtlDynamoDb
   Test-InsuranceLakeEtlPipeline/Test/InsuranceLakeEtlGlue
   Test-InsuranceLakeEtlPipeline/Test/InsuranceLakeEtlStepFunctions
   Test-InsuranceLakeEtlPipeline/Test/InsuranceLakeEtlAthenaHelper
   Prod-InsuranceLakeEtlPipeline/Prod/InsuranceLakeEtlDynamoDb
   Prod-InsuranceLakeEtlPipeline/Prod/InsuranceLakeEtlGlue
   Prod-InsuranceLakeEtlPipeline/Prod/InsuranceLakeEtlStepFunctions
   Prod-InsuranceLakeEtlPipeline/Prod/InsuranceLakeEtlAthenaHelper
   ```

1. Run the command ```cdk deploy --all```
1. Check the following expected outputs:

   1. In the deployment account's CloudFormation console, you will see the following CloudFormation stacks created:

      ![CloudFormation_stacks_in_deployment_account](./cdk_deploy_output_deployment_account_both.png)

   1. In the deployment account, the following CodePipelines should be created successfully:

      ![ETL_CDK_pipelines_deployment_account](./cdk_pipelines_deployment_account_etl.png)

   1. In the deployment account's CodePipeline console, you will see the Pipelines triggered:

      ![CloudFormation_stacks_in_deployment_account](./dev_codepipeline_in_deployment_account.png)

   1. In the Dev environment's CloudFormation console, CodePipeline will create the following stacks:

      ![CDK_deploy_all_dev_output](./cdk_pipelines_deployed_stacks_dev_output_etl.png)

1. You can now begin using InsuranceLake in three environments, and iteratively deploy updates!