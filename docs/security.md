---
title: Security
nav_order: 4
last_modified_date: 2024-09-26
---
# InsuranceLake Security
{:.no_toc}

## Contents
{:.no_toc}

* TOC
{:toc}

For more information on how AWS services come together in InsuranceLake to align with the [Security Pillar of the AWS Well-Architected Framework](https://docs.aws.amazon.com/wellarchitected/latest/financial-services-industry-lens/security.html), refer to the [InsuranceLake Well-architected Pillar Alignment for Security](well_architected.md#security).

## Infrastructure Code

InsuranceLake uses [CDK-nag](https://github.com/cdklabs/cdk-nag) to confirm AWS resource security recommendations are followed. CDK-nag can generate warnings, which may need to be fixed depending on the context, and errors, which will interrupt the stack synthesis and prevent any deployment.

To force synthesis of all stacks (including the CodePipeline deployed stacks), which will check all code and generate all reports, use the following command:

```bash
cdk synth '**'
```

When this operation is complete, you will also have access to the CDK-nag reports in CSV format in the `cdk.out` directory and assembly directories.

By default, the [AWS Solutions Rules Pack](https://github.com/cdklabs/cdk-nag/blob/main/RULES.md#aws-solutions) is used, but any combination of CDK Nag Rules packs can be selected by adjusting the source code **in four locations** (two for both the Infrastructure and ETL codebases):

[Infrastructure app.py Line 21](https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure/blob/main/app.py#L21), [ETL app.py Line 20](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/app.py#L20):

```python
# Enable CDK Nag for the Mirror repository, Pipeline, and related stacks
# Environment stacks must be enabled on the Stage resource
cdk.Aspects.of(app).add(AwsSolutionsChecks())
```

[Infrastructure pipeline_stack.py Line 148](https://github.com/aws-solutions-library-samples/aws-insurancelake-infrastructure/blob/main/lib/pipeline_stack.py#L148), [ETL pipeline_stack.py Line 147](https://github.com/aws-solutions-library-samples/aws-insurancelake-etl/blob/main/lib/pipeline_stack.py#L147)
```python
        # Enable CDK Nag for environment stacks before adding to
        # pipeline, which are deployed with CodePipeline
        cdk.Aspects.of(pipeline_deploy_stage).add(AwsSolutionsChecks())
```

## Application Code

InsuranceLake uses [Bandit](https://bandit.readthedocs.io/en/latest) and [Amazon CodeGuru](https://docs.aws.amazon.com/codeguru/latest/reviewer-ug/welcome.html) for static code analysis of all helper scripts, Lambda functions, and AWS Glue jobs.

To configure CodeGuru Code Reviews, follow the [AWS Documentation on creating Code Reviews](https://docs.aws.amazon.com/codeguru/latest/reviewer-ug/create-code-reviews.html).

To scan all application code using Bandit, use the following command:

```bash
bandit -r --ini .bandit
```