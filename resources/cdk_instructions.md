# CDK Instructions

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

## Clean-up Instructions - Infrastructure

1. Delete stacks using the command ```cdk destroy --all```. When you see the following text, enter **y**, and press enter/return.

   ```bash
   Are you sure you want to delete: TestDataLakeCDKBlogInfrastructurePipeline, ProdDataLakeCDKBlogInfrastructurePipeline, DevDataLakeCDKBlogInfrastructurePipeline (y/n)?
   ```

   Note: This operation deletes stacks only in central deployment account

1. To delete stacks in **development** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Dev-DevDataLakeCDKBlogInfrastructureVpc
   1. Dev-DevDataLakeCDKBlogInfrastructureS3BucketZones
   1. Dev-DevDataLakeCDKBlogInfrastructureIam

   **Note:**
    1. Deletion of **Dev-DevDataLakeCDKBlogInfrastructureS3BucketZones** will delete the S3 buckets (raw, conformed, and purpose-built). This behavior can be changed by modifying the retention policy in [s3_bucket_zones_stack.py](lib/s3_bucket_zones_stack.py#L38)

1. To delete stacks in **test** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Test-TestDataLakeCDKBlogInfrastructureVpc
   1. Test-TestDataLakeCDKBlogInfrastructureS3BucketZones
   1. Test-TestDataLakeCDKBlogInfrastructureIam

   **Note:**
      1. The S3 buckets (raw, conformed, and purpose-built) have retention policies attached and must be removed manually when they are no longer needed.

1. To delete stacks in **prod** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Prod-ProdDataLakeCDKBlogInfrastructureVpc
   1. Prod-ProdDataLakeCDKBlogInfrastructureS3BucketZones
   1. Prod-ProdDataLakeCDKBlogInfrastructureIam

   **Note:**
      1. The S3 buckets (raw, conformed, and purpose-built) have retention policies attached and must be removed manually when they are no longer needed.

1. **Optional:**

   1. If you are not using AWS CDK for other purposes, you can also remove ```CDKToolkit``` stack in each target account.

   1. Note: The asset S3 bucket has a retention policy and must be removed manually.

1. For more details refer to [AWS CDK Toolkit](https://docs.aws.amazon.com/cdk/latest/guide/cli.html)


### Clean-up - ETL

1. Delete stacks using the command ```cdk destroy --all```. When you see the following text, enter **y**, and press enter/return.

   ```bash
   Are you sure you want to delete: TestDataLakeCDKBlogEtlPipeline, ProdDataLakeCDKBlogEtlPipeline, DevDataLakeCDKBlogEtlPipeline (y/n)?
   ```

   Note: This operation deletes stacks only in central deployment account

1. To delete stacks in **development** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Dev-DevDataLakeCDKBlogEtlDynamoDb
   1. Dev-DevDataLakeCDKBlogEtlGlue
   1. Dev-DevDataLakeCDKBlogEtlStepFunctions

1. To delete stacks in **test** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Test-TestDataLakeCDKBlogEtlDynamoDb
   1. Test-TestDataLakeCDKBlogEtlGlue
   1. Test-TestDataLakeCDKBlogEtlStepFunctions

1. To delete stacks in **prod** account, log onto Dev account, go to AWS CloudFormation console and delete the following stacks:

   1. Prod-ProdDataLakeCDKBlogEtlDynamoDb
   1. Prod-ProdDataLakeCDKBlogEtlGlue
   1. Prod-ProdDataLakeCDKBlogEtlStepFunctions

1. For more details refer to [AWS CDK Toolkit](https://docs.aws.amazon.com/cdk/latest/guide/cli.html)
