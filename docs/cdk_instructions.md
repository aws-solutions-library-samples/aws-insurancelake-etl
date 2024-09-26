---
title: CDK Instructions
parent: Developer Documentation
nav_order: 3
last_modified_date: 2024-09-26
---
# AWS CDK Instructions
{: .no_toc }

This section provides instructions for using AWS CDK with the solution.

## Contents
{: .no_toc }

* TOC
{:toc}

## Prerequisites

1. Install [Python](https://www.python.org/downloads/) on your local computer.
1. Install [Node.js](https://nodejs.org/en/download/package-manager/) on your local computer.
   - CDK uses Node.js under the hood; the code will be in Python for this application.
1. Install [CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) on your local computer.
   ```bash
   sudo npm install -g aws-cdk
   ```

## Setup Instructions

The `cdk.json` file provides instructions for the AWS CDK toolkit to execute your application.

This project is set up like a standard Python project. The initialization process below will create a Python runtime virtual environment within this project, stored under the `.venv` directory. To create the virtual environment, the instructions assume that there is a `python3` (or `python` for Windows) executable in your path with access to the `venv` package.

To create a Python runtime virtual environment on MacOS and Linux:

```bash
python3 -m venv .venv
```

After the initialization process completes and the virtual environment is created, you can use the following step to activate your virtual environment:

```bash
source .venv/bin/activate
```

If you are using a Windows platform, activate the virtual environment as shown below:

```bash
% .venv\Scripts\activate.bat
```

Once the virtual environment is activated, you can install the required dependencies.

```bash
pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template using AWS CDK for this application.

```bash
cdk synth
```

To add additional dependencies, for example other CDK libraries, add them to the `requirements.txt` or `requirements-dev.txt` files and rerun the `pip install -r requirements.txt` command.

---

## Useful Commands

|<!-- -->  |<!-- -->
|---  |---
|`cdk synth`   |Outputs the synthesized CloudFormation template
|`cdk deploy`  |Deploys this stack to your default AWS account and Region
|`cdk diff`    |Compares deployed stack with current application state
|`cdk docs`    |Opens AWS CDK documentation

For more details refer to the [AWS CDK CLI Reference](https://docs.aws.amazon.com/cdk/latest/guide/cli.html).

 ---

## Visual Studio Code Debugging

 To configure Visual Studio Code for debugging an AWS CDK application in Python, use the following launch configuration in `launch.json`:

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