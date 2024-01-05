# Developer Guide

## Local CDK Deployment

Reference the [CDK Instructions](./cdk_instructions.md) for standard CDK project setup.

Prerequisites include:
* [Python](https://www.python.org/downloads/) (Python 3.9 or later)
* [Node.js](https://nodejs.org/en/download/package-manager/)

## Code Security

[CDK-Nag](https://github.com/cdklabs/cdk-nag) is integrated into the Python CDK stack and uses supressions only when necessary.
[Bandit](https://pypi.org/project/bandit/) and [Amazon CodeGuru](https://docs.aws.amazon.com/codeguru/) are used for static code analysis.

## Code Quality

We follow [PEP8](https://www.python.org/dev/peps/pep-0008/) enforced through [flake8](https://flake8.pycqa.org/en/latest/) and [pre-commit](https://pre-commit.com/)

Please install and setup pre-commit before making any commits against this project. Example:

```{bash}
pip install pre-commit
pre-commit install
```

The above will create a git hook which will validate code prior to commits. Configuration for standards can be found in:

* [.flake8](../.flake8)
* [.pre-commit-config.yaml](../.pre-commit-config.yaml)

## Testing

### Unit Testing

The Python CDK unit tests use [pytest](https://docs.pytest.org/), which will be installed as part of the solution requirements.

Run tests with the following command (`--cov` will include a code coverage report):
```bash
python -m pytest --cov
```

Glue Job Unit Tests will be automatically skipped if no Glue/Spark environment is detected. For information on setting up a local Glue/Spark environment, refer to the following guides:

* [AWS Blog: Develop and test AWS Glue version 3.0 and 4.0 jobs locally using a Docker container](https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/)
* [AWS Developer Guide: Developing and testing AWS Glue job scripts locally](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html)
* Note that Hudi support and Glue Data Quality are not available in the AWS Glue Docker image.

## Known Issues

   ```bash
   Action execution failed
   Error calling startBuild: Cannot have more than 1 builds in queue for the account (Service: AWSCodeBuild; Status Code: 400; Error Code: AccountLimitExceededException; Request ID: e123456-d617-40d5-abcd-9b92307d238c; Proxy: null)
   ```

   [Quotas for AWS CodeBuild](https://docs.aws.amazon.com/codebuild/latest/userguide/limits.html)