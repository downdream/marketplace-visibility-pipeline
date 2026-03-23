# -*- coding: utf-8 -*-

import os
import sys
import requests

###Python scripts###

# Set up GitLab API endpoint and personal access token
gitlab_url = "gitlab url"
project_id = "project id"
access_token = os.environ.get("GITLAB_CICD") # --> when the token is in environment variabales in cloud function

# For local development, input the access token (securely saved on every team member)
if access_token is None:
    access_token = input('GITLAB_CICD access token:')

# Construct the URL to fetch CI/CD variables for the project
url = f"{gitlab_url}/projects/{project_id}/variables"

# Fetch CI/CD variable
def fetch_var(variable_key):
    # Make a GET request to retrieve CI/CD variables
    headers = {"PRIVATE-TOKEN": access_token}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # Parse the response JSON to extract the encryption key
        variables = response.json()
        for variable in variables:
            if variable["key"] == variable_key:
                value_key = variable["value"]
                break
        # Export the encryption key to the local environment
        os.environ["ENCRYPTION_KEY"] = value_key
        print(f'"{variable_key}" exported successfully')
        return value_key
    else:
        print(f'Failed to fetch CI/CD variables "{variable_key}"')

