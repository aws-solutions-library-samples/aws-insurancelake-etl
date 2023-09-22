# AWS CodePipeline and GitHub integration

Integration between AWS CodePipeline and GitHub requires a personal access token. This access token is stored in Secrets Manager. This is a one-time setup and is applicable for all target AWS environments and all repositories created under the organization in GitHub.com. Follow the below steps:

1. **Note:** Do **NOT** commit these values to your repository

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

1. Expected output 2: A secret is added to AWS Secrets Manager with name **/DataLake/GitHubToken**
