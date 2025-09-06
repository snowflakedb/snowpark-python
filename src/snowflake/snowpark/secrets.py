#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""
In Stored Procedures, UDFs, and Notebooks you can use these methods to retrieve credentials contained 
in a secret object you created with the CREATE SECRET statement.

In a local development environment (such as VS Code or PyCharm) these methods will read from a secrets.toml file
located in the .snowpark directory in the current working directory. This file is used to store secrets for local
development and testing. You can override the location of this file by setting the SNOWPARK_SECRETS_FILE environment 
variable to the path of your secrets file.

>>> # Example .snowpark/secrets.toml file for development
>>>
>>> # Generic string secret (API key, token, etc.)
>>> [my_api_key]
>>> type = "GENERIC_STRING"
>>> value = "abc123def456ghi789"
>>> 
>>> [github_token]
>>> type = "GENERIC_STRING"
>>> value = "ghp_1234567890abcdef"
>>> 
>>> # OAuth2 access token
>>> [oauth_service]
>>> type = "OAUTH2"
>>> value = "ya29.a0AfH6SMC..."
>>> 
>>> # Username/password credentials
>>> [database_creds]
>>> type = "PASSWORD"
>>> username = "admin"
>>> password = "secret123"
>>> 
>>> [service_account]
>>> type = "PASSWORD"
>>> username = "svc_user"
>>> password = "complex_password_here"
>>> 
>>> # Cloud provider credentials (AWS example)
>>> [aws_credentials]
>>> type = "CLOUD_PROVIDER_TOKEN"
>>> access_key_id = "AKIAIOSFODNN7EXAMPLE"
>>> secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
>>> token = "optional_session_token"
>>> 
>>> [azure_credentials]
>>> type = "CLOUD_PROVIDER_TOKEN"
>>> access_key_id = "azure_client_id"
>>> secret_access_key = "azure_client_secret"
>>> token = "azure_tenant_id"


When you deploy your Python code to Snowflake in the form of UDFs, Procedures, or Notebooks, these methods will instead 
retrieve secrets from the corresponding Snowflake Secrets objects. 
"""
import os
from pathlib import Path
import warnings
import toml
from snowflake.snowpark._internal.utils import publicapi, 


@experimental(version="...")
def _get_development_secrets_file() -> str:
    """
    Gets the path to the development secrets file. This file is used to store secrets in a local 
    development environment.
    """
    env_var = os.environ.get("SNOWPARK_SECRETS_FILE")
    if env_var:
        return env_var

    cwd = Path.cwd()
    secrets_file = cwd / ".snowpark" / "secrets.toml"
    if not secrets_file.exists():
        warnings.warn(f"Secrets file not found at {secrets_file}")
    return str(secrets_file)


@experimental(version="...")
@publicapi
def get_generic_secret_string(generic_string_secret_name: str) -> str:
    """
    Gets the generic token string held by the secret specified by generic_string_secret_name. Returns 
    a valid token string.
    """
    secrets_file = _get_development_secrets_file()

    secrets = toml.load(secrets_file)
    if generic_string_secret_name not in secrets:
        raise KeyError(f"Secret '{generic_string_secret_name}' not found in {secrets_file}")
    return secrets[generic_string_secret_name]

@experimental(version="...")
@publicapi
def get_oauth_access_token(oauth_secret_name: str) -> str:
    """
    Gets the OAuth2 access token held by the secret specified by oauth_secret_name. Returns an OAuth2 
    token string.
    """
    secrets_file = _get_development_secrets_file()

    secrets = toml.load(secrets_file)
    if oauth_secret_name not in secrets:
        raise KeyError(f"Secret '{oauth_secret_name}' not found in {secrets_file}")
    return secrets[oauth_secret_name]


@experimental(version="...")
@publicapi
def get_secret_type(secret_name: str) -> str:
    """
    Gets the type of the secret specified by secret_name. Returns the TYPE parameter value set for this 
    secret when it was created with the CREATE SECRET statement.
    """
    secrets_file = _get_development_secrets_file()

    secrets = toml.load(secrets_file)
    if secret_name not in secrets:
        raise KeyError(f"Secret '{secret_name}' not found in {secrets_file}")
    secret_info = secrets[secret_name]
    if 'type' not in secret_info:
        raise KeyError(f"Secret '{secret_name}' does not have a 'type' defined in {secrets_file}")
    return secret_info['type']


@experimental(version="...")
@publicapi
def get_username_password(username_password_secret_name: str) -> dict[str, str]:
    """
    Gets the username and password from the secret specified by username_password_secret_name. Returns an 
    object with username and password attributes.
    """
    secrets_file = _get_development_secrets_file()

    secrets = toml.load(secrets_file)
    if username_password_secret_name not in secrets:
        raise KeyError(f"Secret '{username_password_secret_name}' not found in {secrets_file}")
    
    secret_info = secrets[username_password_secret_name]
    if 'username' not in secret_info or 'password' not in secret_info:
        raise KeyError(f"Secret '{username_password_secret_name}' does not have 'username' or 'password' defined in {secrets_file}")
    
    return {
        "username": secret_info['username'],
        "password": secret_info['password']
    }


@experimental(version="...")
@publicapi
def get_cloud_provider_token(cloud_provider_secret_name: str) -> dict[str, str]:
    """
    Gets a cloud provider object containing values you can use to create a session with the cloud provider, 
    such as AWS. Returns a type with the following attributes:

    access_key_id
    secret_access_key
    token
    """
    secrets_file = _get_development_secrets_file()

    secrets = toml.load(secrets_file)
    if cloud_provider_secret_name not in secrets:
        raise KeyError(f"Secret '{cloud_provider_secret_name}' not found in {secrets_file}")
    
    secret_info = secrets[cloud_provider_secret_name]
    if 'access_key_id' not in secret_info or 'secret_access_key' not in secret_info or 'token' not in secret_info:
        raise KeyError(f"Secret '{cloud_provider_secret_name}' does not have 'access_key_id', 'secret_access_key', or 'token' defined in {secrets_file}")
    
    return {
        "access_key_id": secret_info['access_key_id'],
        "secret_access_key": secret_info['secret_access_key'],
        "token": secret_info['token']
    }
