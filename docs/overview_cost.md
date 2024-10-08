---
title: Cost
parent: Overview
last_modified_date: 2024-09-26
---
## Cost

This solution uses the following services: [Amazon S3](https://aws.amazon.com/s3/pricing/), [AWS Glue](https://aws.amazon.com/glue/pricing/), [AWS Step Functions](https://aws.amazon.com/step-functions/pricing/), [Amazon DynamoDB](https://aws.amazon.com/dynamodb/pricing/), [AWS Lambda](https://aws.amazon.com/lambda/pricing/), [Amazon CloudWatch](https://aws.amazon.com/cloudwatch/pricing/), [Amazon Athena](https://aws.amazon.com/athena/pricing/), and [AWS CodePipeline](https://aws.amazon.com/codepipeline/pricing/) for continuous integration and continuous deployment (CI/CD) installation only.

An estimated cost for following the [Quickstart](#quickstart) and [Quickstart with CI/CD](quickstart_cicd.md) instructions, assuming a total of 8 Glue DPU hours and cleaning all resources when finished, **your cost will not be higher than $2**. This cost could be less as some services are included in the [Free Tier](https://aws.amazon.com/free/).

_We recommend creating a [Budget](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html) through [AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/) to help manage costs. Prices are subject to change. For full details, refer to the pricing webpage for each AWS service used in this solution._

### Sample Cost Table

The following table provides a sample cost breakdown for deploying this Guidance with the default parameters in the US East (Ohio) Region for one month with pricing as of _2 October 2024_.

|AWS service    |Dimensions |Cost [USD]
|---    |---    |---
|AWS Glue   |per DPU-Hour for each Apache Spark or Spark Streaming job, billed per second with a 1-minute minimum   |$0.44
|Amazon S3  |per GB of storage used, Frequent Access Tier, first 50 TB per month<br>PUT, COPY, POST, LIST requests (per 1,000 requests)<br>GET, SELECT, and all other requests (per 1,000 requests)   |$0.023<br>$0.005<br>$0.0004
|Amazon Athena  |per TB of data scanned   |$5.00
|Amazon DynamoDB    |per GB-month of storage, over 25 GB<br>per million Write Request Units (WRU)<br>per million Read Request Units (RRU)  |$0.25<br>$1.25<br>$0.25