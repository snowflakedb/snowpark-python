#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark._internal.utils import publicapi

# Reference for Python API for Secret Access:
# https://docs.snowflake.com/en/developer-guide/external-network-access/secret-api-reference#python-api-for-secret-access


class UsernamePassword:
    def __init__(self, username, password) -> None:
        self.username = username
        self.password = password


class CloudProviderToken:
    def __init__(self, id, key, token) -> None:
        self.access_key_id = id
        self.secret_access_key = key
        self.token = token


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
        raise NotImplementedError(
            "Cannot import _snowflake module. Secret API is only supported on Snowflake server environment."
        )


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
        raise NotImplementedError(
            "Cannot import _snowflake module. Secret API is only supported on Snowflake server environment."
        )


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
        raise NotImplementedError(
            "Cannot import _snowflake module. Secret API is only supported on Snowflake server environment."
        )


@publicapi
def get_username_password(secret_name: str) -> UsernamePassword:
    """Get a username and password secret from Snowflake.
    Note:
        Require a Snowflake environment with username/password secrets configured
    Returns:
        UsernamePassword: An object with attributes ``username`` and ``password``.
    Raises:
        NotImplementedError: If the _snowflake module cannot be imported.
    """
    try:
        import _snowflake

        secret_object = _snowflake.get_username_password(secret_name)
        return UsernamePassword(secret_object.username, secret_object.password)
    except ImportError:
        raise NotImplementedError(
            "Cannot import _snowflake module. Secret API is only supported on Snowflake server environment."
        )


@publicapi
def get_cloud_provider_token(secret_name: str) -> CloudProviderToken:
    """Get a cloud provider token secret from Snowflake.
    Note:
        Require a Snowflake environment with cloud provider secrets configured
    Returns:
        CloudProviderToken: An object with attributes ``access_key_id``,
        ``secret_access_key``, and ``token``.
    Raises:
        NotImplementedError: If the _snowflake module cannot be imported.
    """
    try:
        import _snowflake

        secret_object = _snowflake.get_cloud_provider_token(secret_name)
        return CloudProviderToken(
            secret_object.access_key_id,
            secret_object.secret_access_key,
            secret_object.token,
        )
    except ImportError:
        raise NotImplementedError(
            "Cannot import _snowflake module. Secret API is only supported on Snowflake server environment."
        )
