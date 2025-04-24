---
title: Well-Architected Pillars
parent: Architecture
last_modified_date: 2024-09-26
---
# InsuranceLake Well-Architected Pillars

The [AWS Well-Architected Framework](https://aws.amazon.com/architecture/well-architected) helps you understand the pros and cons of the decisions you make when building systems in the cloud. The six pillars of the framework allow you to learn architectural best practices for designing and operating reliable, secure, efficient, cost-effective, and sustainable systems. Using the [AWS Well-Architected Tool](https://aws.amazon.com/well-architected-tool), available at no charge in the [AWS Management Console](https://console.aws.amazon.com/wellarchitected), you can review your workloads against these best practices by answering a set of questions for each pillar.

The [Financial Services Industry Lens](https://docs.aws.amazon.com/wellarchitected/latest/financial-services-industry-lens/financial-services-industry-lens.html) identifies best practices for security, data privacy, and resiliency that are intended to address the requirements of financial institutions based on AWS experience working with financial institutions worldwide. It provides guidance on guardrails for technology teams to implement and confidently use AWS to build and deploy applications. This lens describes the process of building transparency and auditability into your AWS environment. It also offers suggestions for controls to help you expedite adoption of new services into your environment while managing the cost of your IT services.

The Financial Services Industry Lens is also available in the [AWS Well-Architected Lens Catalog](https://docs.aws.amazon.com/wellarchitected/latest/userguide/lens-catalog.html) and can be added to your workload review in the Well-Architected Tool:
1. On the `Overview` tab, under Lenses, select `Edit`.
1. In the `Lens Catalog` section, locate the _Financial Services Industry Lens_.
1. Select the checkbox in the top right corner.
1. Select `Save`.

The InsuranceLake solution has been created with Well-Architected best practices in mind. To be fully Well-Architected, you should follow as many Well-Architected best practices as possible with your implementation. The following documentation describes the AWS service implementation alignment to each pillar and can be used to assist your own workload review.


## Pillars

* [Operational Excellence](#operational-excellence)
* [Security](#security)
* [Reliability](#reliability)
* [Performance Efficiency](#performance-efficiency)
* [Cost Optimization](#cost-optimization)
* [Sustainability](#sustainability)


## AWS Service Reference

* [AWS Identity and Access Management (IAM)](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html)
* [Amazon Simple Storage Service (Amazon S3)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)
* [AWS Key Management Service (AWS KMS)](https://docs.aws.amazon.com/kms/latest/developerguide/overview.html)
* [Amazon Virtual Private Cloud (Amazon VPC)](https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html)
* [Amazon CloudWatch](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html)
* [AWS Lambda](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html)
* [AWS Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/welcome.html)
* [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
* [Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html)
* [Amazon Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html)
* [Amazon Simple Notification Service (Amazon SNS)](https://docs.aws.amazon.com/sns/latest/dg/welcome.html)
* [AWS CodePipeline](https://docs.aws.amazon.com/codepipeline/latest/userguide/welcome.html)
* [AWS Cloud Development Kit (CDK)](https://docs.aws.amazon.com/cdk/v2/guide/home.html)
* [AWS Backup](https://docs.aws.amazon.com/aws-backup/latest/devguide/whatisbackup.html)
* [AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/what-is-lake-formation.html)
* [Amazon EventBridge](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-what-is.html)


## Operational Excellence

[Access to all S3 buckets is logged](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html) into a dedicated access log bucket so that administrators can review access and inform permission maintenance.

The Lambda State Machine trigger and ETL job audit functions are designed in the data lake solution to provide curated diagnostic and status information in CloudWatch logs, enabling InsuranceLake administrators to easily troubleshoot.

Step Functions past runtime outcomes can be reviewed in the state machine execution history. The Step Functions workflows reports all pipeline successes and failures to Amazon SNS which makes it easy for pipeline maintainers and downstream services to subscribe, get alerts, and react as needed. [Step Functions task metrics and execution status can be monitored](https://docs.aws.amazon.com/step-functions/latest/dg/procedure-cw-metrics.html) through CloudWatch to ensure the services are scaling as expected.

[AWS Glue Data Quality](https://docs.aws.amazon.com/glue/latest/dg/glue-data-quality.html) enables users to describe business rules for data, and measure data for completeness, accuracy, and uniqueness. [InsuranceLake ETL provides AWS Glue Data Quality integration](data_quality.md) in three locations within the ETL pipeline and user-selected actions: halt pipeline, log warnings, and quarantine individual rows. AWS Glue Data Quality runs emit [Evaluation Result events that EventBridge can receive](https://docs.aws.amazon.com/glue/latest/dg/data-quality-alerts.html), which can be used to trigger actions and other notifications.

Three AWS Glue jobs are designed in the data lake solution to commit data to the Cleanse and Consume buckets, only if all operations succeed (including data quality checks), avoiding the need for manual rollback.

InsuranceLake enables [AWS Glue Continuous Logging](https://docs.aws.amazon.com/glue/latest/dg/monitor-continuous-logging-view.html) by default, providing real-time transparency into data pipeline job progress. CloudWatch Output Logs from the AWS Glue jobs are designed to provide real-time diagnostic information about transformations and validations being performed, while AWS Glue session logs provide low-level insight into Apache Spark operations and issues.

InsuranceLake ETL uses DynamoDB to capture metadata from the ETL process and makes it easy to publish metrics and audit data to an operational dashboard, such as data quality results, ETL pipeline job status, and data lineage logs. [DynamoDB metrics can be monitored through CloudWatch](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/MonitoringAndLoggingInDynamoDB.html) to ensure the services are scaling as expected.

[CodePipeline is used to deploy three replica environments of the data lake](full_deployment_guide.md#centralized-deployment): dev, test, and prod. Having identical testing environments allows users to confirm test datasets accurately represent real-world data, confirm outcomes from the pipeline, and compare test results to previous versions. With CodePipeline and infrastructure-as-code (IaC), deployment of changes to all environments is automated, including changes to the pipeline itself.

[InsuranceLake Infrastructure and ETL support unified tagging](full_deployment_guide.md#application-configuration) of all resources across all stacks which can easily be customized by a user from a central location with AWS CDK. These tags make it easy for administrators to locate resources that belong to the data lake and comply with cloud operations policies.

For scalable, fine-grained data access management and governance, data lake administrators can optionally [configure Lake Formation permissions](https://docs.aws.amazon.com/lake-formation/latest/dg/initial-lf-config.html) on the InsuranceLake S3 buckets.


## Security

All inter-service communications use IAM roles. All roles used by the solution follows least-privilege access: they only contain the minimum permissions required so the service can function properly.

S3 buckets for Collect, Cleanse, Consume, ETL Scripts, Temporary Storage for AWS Glue jobs, CodePipeline Artifacts, and Access Logs deployed in the InsuranceLake solution [block all public access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-control-block-public-access.html), require data in transit to be encrypted, and [encrypt all data at rest server-side using KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-kms-encryption.html). Amazon S3 [access points](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-points-usage-examples.html) and [bucket policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-policies.html) can be implemented by InsuranceLake administrators to control access to each of the three data lake infrastructure buckets. The solution is configured to [log access to all S3 buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html) to a dedicated access log bucket so that administrators can review access and inform permission maintenance. 

If AWS Glue Connections are needed by InsuranceLake users, an administrator can optionally [configure an Amazon VPC to be used for Connections](full_deployment_guide.md#application-configuration). The AWS Glue job resources are deployed within an [Amazon VPC](https://docs.aws.amazon.com/vpc/latest/userguide/how-it-works.html), providing logical networking isolation from the public internet and service endpoints to InsuranceLake resources. Amazon VPC supports security features like [VPC endpoint](https://docs.aws.amazon.com/whitepapers/latest/aws-privatelink/what-are-vpc-endpoints.html) (keeping traffic within the AWS network), [security groups](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-security-groups.html), [network access control lists (ACLs)](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-network-acls.html), and [IAM roles and policies](https://docs.aws.amazon.com/vpc/latest/userguide/security-iam.html) for controlling inbound and outbound traffic authorization.

Pipeline metadata is encrypted in transit with HTTPS between DynamoDB and AWS Glue, and [at rest in DynamoDB by default](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/EncryptionAtRest.html). InsuranceLake administrators can choose the type of encryption keys for data stored in DynamoDB. 

AWS Glue job design in the data lake solution enables InsuranceLake administrators to automate regular execution of data pipelines eliminating the need for direct access or manual processing of data. This reduces the risk of mishandling, untracked modification, and human error when handling sensitive data.

InsuranceLake ETL AWS Glue jobs help users protect sensitive data through [built-in masking and hashing transforms](transforms.md#data-security); they also help users track and identify rows of data loaded through the pipeline end-to-end using a unique execution ID. ETL AWS Glue jobs and Lambdas are designed to log only data schema and diagnostic messages in CloudWatch helping administrators avoid the risk of exposing sensitive data in logs.

InsuranceLake Lambda functions use an [AWS-maintained runtime with the latest version of Python](https://docs.aws.amazon.com/lambda/latest/dg/lambda-python.html). Functions are triggered by AWS service events only. Access to and access for the Lambda functions is provided only through IAM.

[CodePipeline](https://docs.aws.amazon.com/codepipeline/latest/userguide/welcome.html) provided with InsuranceLake enables continuous automated version-controlled deployment of infrastructure and application code changes to multiple environments.

For centrally managed column and row data access through AWS Glue Data Catalog and Athena, a data lake administrator can optionally [configure Lakeformation permissions](https://docs.aws.amazon.com/lake-formation/latest/dg/initial-lf-config.html) on the InsuranceLake S3 buckets.


## Reliability

As the primary data store for the data lake solution, [Amazon S3 by default provides 11 9s of durability and 99.99% availability](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DataDurability.html); Amazon S3 stores data across multiple Availability Zones also by default. For data redundancy across greater geographic distances, InsuranceLake administrators can enable [Amazon S3 Cross-Region Replication](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html#crr-scenario). The InsuranceLake solution enables [Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html) on all S3 buckets, which can be used to preserve, retrieve, and restore every version of every object stored. Data lake administrators can easily recover from both unintended user actions and application failures.

InsuranceLake's [infrastructure as code through AWS CDK](full_deployment_guide.md#continuous-delivery-of-data-lake-infrastructure-using-cdk-pipelines) and [multi-environment deployment through CodePipeline](full_deployment_guide.md#centralized-deployment) enable data lake administrators to easily replicate resources in multiple Regions and accounts. Combined with S3 bucket cross-region replication and [DynamoDB Global Tables](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html), you can deploy a geographically redundant data pipeline and lake.

If AWS Glue Connections are needed by InsuranceLake users, an administrator can optionally [configure a VPC](full_deployment_guide.md#application-configuration); InsuranceLake VPCs are provisioned with three Availability Zones and an AWS Glue Connection is created for every AWS Glue job to every Availability Zone. This helps users maintain high availability to network services from ETL AWS Glue jobs.

DynamoDB automatically stores [three copies of data across three Availability Zoness in a Region](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html#ddb-intro-resilience). InsuranceLake administrators can enable DynamoDB Global Tables for cross-region disaster recovery, and a backup configuration strategy (including [cross-region backup with AWS Backup](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CrossRegionAccountCopyAWS.html)) to meet their recovery point objective (RPO) and recovery time objective (RTO) requirements. [Point in Time Recovery](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Point-in-time-recovery.html) is enabled by default for all DynamoDB tables used by InsuranceLake. The data stored in DynamoDB tables is protected with [DynamoDB's delete protection](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.Basics.html#WorkingWithTables.Basics.DeletionProtection), configured by the solution in testing and production environments to help avoid data loss.

InsuranceLake uses all serverless services to ensure high availability and recovery from service failure.

Lambda functions automatically [launch in multiple availability zones](https://docs.aws.amazon.com/lambda/latest/dg/security-resilience.html) to ensure high availability; Lambda automatically scales: each execution launches a new instance of the function to handle the increased load. Data lake administrators can optionally [reserve Lambda concurrency](https://docs.aws.amazon.com/lambda/latest/dg/configuration-concurrency.html) to ensure that functions scale to meet concurrency needs. InsuranceLake Lambda functions are executed by Amazon S3 and Step Functions, both of which automatically retry on error with delays between retries.

Concurrency controls are used on AWS Glue jobs. The solution uses Step Functions to orchestrate the workflow and provide retries with exponential backoff and jitter for AWS Glue jobs in the workflow.


## Performance Efficiency

Each InsuranceLake ETL AWS Glue job will provision an Apache Spark cluster on demand to transform data and de-provision the resources when done. ETL AWS Glue jobs are designed to both minimize the number of data processing unit (DPU) hours consumed by the jobs themselves, and optimally store data in Amazon S3 for the Cleanse and Consume layers so that [data scanned and time required by queries from Amazon Athena](https://docs.aws.amazon.com/athena/latest/ug/partitions.html) is minimized. The following design practices contribute<sup>1</sup>:

1. AWS Glue jobs write partitioned and [Snappy-compressed Apache Parquet files](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-parquet-home.html) (a columnar storage format) in the Cleanse and Consume buckets so that data is grouped and persisted based on the partition key values. A creation date year, month, day partition strategy is used by default. This partition strategy reduces the data scanned by queries on incremental data loads.

1. InsuranceLake-provided transforms avoid unnecessary [repartitioning and data shuffling](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations). Repartitioning is performed only once in the Cleanse-to-Consume job to optimize the way data is stored in Amazon S3. Other operations which can cause a [shuffle](https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-shuffles.html), such as a join, are used only when they provide a benefit to the user; broadcast joins are used whenever appropriate.

1. AWS Glue jobs use Spark DataFrame actions only when needed to provide desired functionality, and DataFrame [cache](https://medium.com/@ashwin_kumar_/spark-dataframe-cache-and-persist-explained-019ab2abf20f) before groups of transforms. This helps minimize the number of evaluations in the AWS Glue jobs.

1. Native Spark DataFrame functions are used wherever possible to optimally split data and computations across workers. [Spark User-defined functions (UDFs)](https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-user-defined-functions.html) are used only when logic cannot be performed using native Spark DataFrame functions, because they are less efficient than functions that directly use the Spark JVM. Conversions to Pandas DataFrames or AWS Glue DynamicFrames are performed only when necessary to provide functionality.

With Amazon S3 object storage by default, pipeline transformations and data consumers [can achieve thousands of transactions per second](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html) when storing and retrieving data. Built-in parallelized AWS Glue and Athena operations scale Amazon S3 performance reading from and writing to multiple partition prefixes in the Cleanse and Consume bucket. AWS Glue jobs use the [Spark default partition strategy](https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options) on read to avoid writing a large number of small files, which can cause throttling and reduce performance. Use of Athena offers fast serverless querying against Amazon S3 data without moving it, further enhancing analytics workflow efficiency.

InsuranceLake uses DynamoDB, a fully managed NoSQL database with single-digit millisecond latency at any scale, to store data lineage, data quality results, job audit data, lookup transform data, and tokenized source data. These DynamoDB tables are configured using [On-demand Capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html), providing the right amount of read and write throughput to meet the ETL needs. Data in these tables is structured to be retrieved with a primary and sort key for best performance.

InsuranceLake Lambda functions [use Arm architecture](https://docs.aws.amazon.com/lambda/latest/dg/foundation-arch.html) to take advantage of [Graviton processors' better price/performance](https://aws.amazon.com/blogs/aws/aws-lambda-functions-powered-by-aws-graviton2-processor-run-your-functions-on-arm-and-get-up-to-34-better-price-performance). These functions are designed to not load any incoming data, and perform short execution time AWS service calls.

The InsuranceLake ETL data pipeline definitions are designed to make rapid changes through CI/CD deployment using CodePipeline and help ensure quality through provided unit tests. Data pipeline design can be rapidly iterated making use of [the 4 Cs (Collect, Cleanse, Consume, Comply) data lake pattern](https://docs.aws.amazon.com/wellarchitected/latest/financial-services-industry-lens/insurance-lake.html), the included transform and pattern library, and low-code configuration through text files stored in S3. These features help you quickly publish data, get feedback, and iteratively improve.


## Cost Optimization

The InsuranceLake solution uses all serverless fully managed services so you only get charged for what you use, and you minimize the cost of maintaining the data lake. Amazon S3 is used for data storage, and AWS Glue is the primary compute engine for data lake operations; this separation of storage and compute helps data lake administrators independently optimize cost.

InsuranceLake Infrastructure and ETL support unified tagging of all resources across all stacks that can easily be [customized by a user from a central location in AWS CDK](full_deployment_guide.md#application-configuration). Tagging includes a recommended Cost Center tag, helping administrators track costs associated with the data lake<sup>2</sup>.

The InsuranceLake solution automatically deploys with an environment-dependent base Amazon S3 lifecycle policy, which includes transition to Amazon S3 Glacier storage. This helps you manage data storage cost as the amount of data grows.

InsuranceLake DynamoDB tables used in the solution can be configured using [On-demand Capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html), providing read and write throughput only when needed. InsuranceLake structures pipeline metadata in DynamoDB to be retrieved with only a primary and sort key, which helps ensure that only the data needed is retrieved. DynamoDB tables are deployed as a standard storage class by default and can be adjusted to [Infrequent access](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.tableclasses.html) by InsuranceLake administrators based on access patterns. Administrators can configure [DynamoDB Time to Live (TTL)](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/time-to-live-ttl-how-to.html) to automatically delete expired items. [DynamoDB Backup](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Backup.Tutorial.html) or [DynamoDB Backup with Cross-Region replication](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CrossRegionAccountCopyAWS.html) can optionally be used instead of global tables to meet some RPO and RTO requirements and save cost.

InsuranceLake ETL AWS Glue jobs are configured in the solution to use [DPU auto-scaling](https://docs.aws.amazon.com/glue/latest/dg/auto-scaling.html). The jobs are designed to both minimize the number of DPU hours consumed by the jobs themselves and optimally store data in Amazon S3 for the Cleanse and Consume layers so that [data scanned by queries from Amazon Athena is minimized](https://docs.aws.amazon.com/athena/latest/ug/partitions.html). Data lake administrators can enable [Flex execution](https://aws.amazon.com/blogs/big-data/introducing-aws-glue-flex-jobs-cost-savings-on-etl-workloads/) and adjust the Worker Type (compute and memory), both configurable options available from AWS Glue.

The following solution design practices contribute to cost optimization: 

1. AWS Glue jobs write partitioned and [Snappy-compressed Apache Parquet files](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-parquet-home.html) in the Cleanse and Consume buckets so that data is grouped and persisted based on the partition key values. A creation date year, month, day partition strategy is used by default. This partition strategy reduces the data scanned by queries on incremental data loads.

1. InsuranceLake-provided transforms avoid unnecessary [repartitioning and data shuffling](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations). Repartitioning is performed only once in the Cleanse-to-Consume job to optimize the way data is stored in Amazon S3. Other operations which can cause a [shuffle](https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-shuffles.html), such as a join, are used only when they provide a benefit to the user; broadcast joins are used whenever appropriate.

1. AWS Glue jobs use Apache Spark DataFrame actions only when needed to provide desired functionality and DataFrame [cache](https://medium.com/@ashwin_kumar_/spark-dataframe-cache-and-persist-explained-019ab2abf20f) before groups of transforms. This helps minimize the number of evaluations in the AWS Glue jobs.

1. Native Spark DataFrame functions are used wherever possible to optimally split data and computations across workers. [Spark UDFs](https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-user-defined-functions.html) are used only when logic cannot be performed using native Spark DataFrame functions, because they are less efficient than functions that directly use the Spark JVM. Conversions to Pandas DataFrames or AWS Glue DynamicFrames are performed only when necessary to provide functionality.

InsuranceLake Lambda functions use the minimum configurable [memory](https://docs.aws.amazon.com/lambda/latest/dg/configuration-memory.html) and [ephemeral storage](https://docs.aws.amazon.com/lambda/latest/dg/configuration-ephemeral-storage.html), do not load any incoming data, and perform short execution time AWS service calls. This solution design helps you minimize the runtime cost of the Lambda functions used to orchestrate the ETL.

Athena is the recommended data lake access service for this solution; it is a serverless query service that makes it easy to analyze data directly in Amazon S3 using standard SQL. With Athena, you will only pay for the queries that you run and the amount of data that is scanned.

Data Catalog provides a serverless, fully managed metadata repository, eliminating the need to set up and maintain a long-running metadata database and reducing operational overhead and costs.


## Sustainability

The InsuranceLake solution uses all serverless fully managed services so customers only use resources they need, when they need it, reducing energy and environmental impact.

The data lake solution uses compressed partitioned columnar format Parquet files for storing data in the Cleanse and Consume S3 buckets. This efficient storage format helps reduce resource consumption when querying data in the lake, thereby reducing energy impact.

DynamoDB’s serverless design, combined with [On-demand Capacity mode](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html) configured by the solution, reduces carbon footprint compared to continually operating on-premises or provisioned database servers. In addition, InsuranceLake’s efficiently designed DynamoDB queries use partition and sort keys to reduce the amount of capacity consumed.

InsuranceLake Lambda functions are [configured to use Graviton processors](https://docs.aws.amazon.com/lambda/latest/dg/foundation-arch.html), which use less energy than other architectures.


## Footnotes

<sup>1</sup>Further reading on optimizing Apache Spark performance in AWS Glue is available from [AWS Prescriptive Guidance Documentation](https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/performance-tuning-strategies.html).

<sup>2</sup>A unique Cost Center tag is not available on AWS Glue job _executions_ and Data Catalog _tables_. If multiple cost centers share the same job definitions and data lake, this will not provide the needed granularity.