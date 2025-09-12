#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark._internal.utils import publicapi
from typing import Dict

# Reference for Python API for Secret Access:
# https://docs.snowflake.com/en/developer-guide/external-network-access/secret-api-reference#python-api-for-secret-access


@publicapi
def get_generic_secret_string(secret_name: str) -> str:
    """Get a generic token string from Snowflake.
    Note:
        Require a Snowflake environment with generic secret strings configured
    Returns:
        The secret value as a string.
    Raises:
        NotImplementedError: If the _snowflake module cannot be imported.
    """
    try:
        import _snowflake

        return _snowflake.get_generic_secret_string(secret_name)
    except ImportError:
        raise NotImplementedError("Cannot import _snowflake module")


@publicapi
def get_oauth_access_token(secret_name: str) -> str:
    """Get an OAuth2 access token from Snowflake.
    Note:
        Require a Snowflake environment with OAuth secrets configured
    Returns:
        The OAuth2 access token as a string.
    Raises:
        NotImplementedError: If the _snowflake module cannot be imported.
    """
    try:
        import _snowflake

        return _snowflake.get_oauth_access_token(secret_name)
    except ImportError:
        raise NotImplementedError("Cannot import _snowflake module")


@publicapi
def get_secret_type(secret_name: str) -> str:
    """Get the type of a secret from Snowflake.
    Note:
        Require a Snowflake environment with secrets configured
    Returns:
        The type of the secret as a string.
    Raises:
        NotImplementedError: If the _snowflake module cannot be imported.
    """
    try:
        import _snowflake

        return str(_snowflake.get_secret_type(secret_name))
    except ImportError:
        raise NotImplementedError("Cannot import _snowflake module")


@publicapi
def get_username_password(secret_name: str) -> Dict[str, str]:
    """Get a username and password secret from Snowflake.
    Note:
        Require a Snowflake environment with username/password secrets configured
    Returns:
        A dictionary containing the username and password with keys:

        - 'username': The username string
        - 'password': The password string
    Raises:
        NotImplementedError: If the _snowflake module cannot be imported.
    """
    try:
        import _snowflake

        secret_object = _snowflake.get_username_password(secret_name)
        return {
            "username": secret_object.username,
            "password": secret_object.password,
        }
    except ImportError:
        raise NotImplementedError("Cannot import _snowflake module")


@publicapi
def get_cloud_provider_token(secret_name: str) -> Dict[str, str]:
    """Get a cloud provider token secret from Snowflake.
    Note:
        Require a Snowflake environment with cloud provider secrets configured
    Returns:
        A dictionary containing the cloud provider credentials with keys:

        - 'access_key_id': The access key ID string
        - 'secret_access_key': The secret access key string
        - 'token': The session token string
    Raises:
        NotImplementedError: If the _snowflake module cannot be imported.
    """
    try:
        import _snowflake

        secret_object = _snowflake.get_cloud_provider_token(secret_name)
        return {
            "access_key_id": secret_object.access_key_id,
            "secret_access_key": secret_object.secret_access_key,
            "token": secret_object.token,
        }
    except ImportError:
        raise NotImplementedError("Cannot import _snowflake module")
