# CDK Instructions

## Prerequisites

1. Install [Python](https://www.python.org/downloads/) on your local computer
1. Install [Node.js](https://nodejs.org/en/download/package-manager/) on your local computer
   - CDK uses Node.js under the hood; the code will be in Python for this application
1. Install [CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) on your local computer
   ```bash
   sudo npm install -g aws-cdk
   ```

## Setup Instructions

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```bash
python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```bash
source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```bash
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```bash
pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```bash
cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

---

## Useful Commands

 1. `cdk ls`          list all stacks in the app
 1. `cdk synth`       emits the synthesized CloudFormation template
 1. `cdk deploy`      deploy this stack to your default AWS account/region
 1. `cdk diff`        compare deployed stack with current state
 1. `cdk docs`        open CDK documentation

 ---

 ## Visual Stodio Code Debugging

 To configure Visual Studio Code for debugging CDK, use the following launch configuration in `launch.json`:

 ```json
 {
	"version": "0.2.0",
	"configurations": [
		{
			"name": "CDK Synth",
			"type": "python",
			"request": "launch",
			"program": "app.py",
			"console": "integratedTerminal",
			"justMyCode": true
		}
	]
}
```

---

## Clean-up Workflow-created Resources

1. Use the `etl_cleanup.py` script to clear the S3 buckets, Glue Data Catalog entries, logs, and DynamoDB tables:
   ```bash
   AWS_DEFAULT_REGION=us-east-2 resources/etl_cleanup.py --mode allbuckets
   ```

### Clean-up ETL Stacks

1. Delete stacks using the command ```cdk destroy --all```. When you see the following text, enter **y**, and press enter/return.

   ```bash
   Are you sure you want to delete: TestInsuranceLakeEtlPipeline, ProdInsuranceLakeEtlPipeline, DevInsuranceLakeEtlPipeline (y/n)?
   ```

   Note: This operation deletes stacks only in central deployment account

1. To delete stacks in **development** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks in the order listed:

   1. Dev-DevInsuranceLakeEtlAthenaHelper
   1. Dev-DevInsuranceLakeEtlStepFunctions
   1. Dev-DevInsuranceLakeEtlGlue
   1. Dev-DevInsuranceLakeEtlDynamoDb

1. To delete stacks in **test** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks in the order listed:

   1. Test-DevInsuranceLakeEtlAthenaHelper
   1. Test-TestInsuranceLakeEtlStepFunctions
   1. Test-TestInsuranceLakeEtlGlue
   1. Test-TestInsuranceLakeEtlDynamoDb

1. To delete stacks in **prod** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks in the order listed:

   1. Prod-DevInsuranceLakeEtlAthenaHelper
   1. Prod-ProdInsuranceLakeEtlStepFunctions
   1. Prod-ProdInsuranceLakeEtlGlue
   1. Prod-ProdInsuranceLakeEtlDynamoDb

## Clean-up Infrastructure Stacks

1. Delete stacks using the command ```cdk destroy --all```. When you see the following text, enter **y**, and press enter/return.

   ```bash
   Are you sure you want to delete: TestInsuranceLakeInfrastructurePipeline, ProdInsuranceLakeInfrastructurePipeline, DevInsuranceLakeInfrastructurePipeline (y/n)?
   ```

   Note: This operation deletes stacks only in central deployment account

1. To delete stacks in **development** account, log onto the Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Dev-DevInsuranceLakeInfrastructureVpc
   1. Dev-DevInsuranceLakeInfrastructureS3BucketZones

   **Note:**
    1. Deletion of **Dev-DevInsuranceLakeInfrastructureS3BucketZones** will delete the S3 buckets (collect, cleanse, and consume). This behavior can be changed by modifying the retention policy in [s3_bucket_zones_stack.py](lib/s3_bucket_zones_stack.py#L38)

1. To delete stacks in **test** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Test-TestInsuranceLakeInfrastructureVpc
   1. Test-TestInsuranceLakeInfrastructureS3BucketZones

   **Note:**
      1. The S3 buckets (raw, conformed, and purpose-built) have retention policies attached and must be removed manually when they are no longer needed.

1. To delete stacks in **prod** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Prod-ProdInsuranceLakeInfrastructureVpc
   1. Prod-ProdInsuranceLakeInfrastructureS3BucketZones

   **Note:**
      1. The S3 buckets (raw, conformed, and purpose-built) have retention policies attached and must be removed manually when they are no longer needed.

1. **Optional:**

   1. If you are not using AWS CDK for other purposes, you can also remove ```CDKToolkit``` stack in each target account. For more details refer to [AWS CDK Toolkit](https://docs.aws.amazon.com/cdk/latest/guide/cli.html)