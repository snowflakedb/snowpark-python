#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os

# Reference for Python API for Secret Access:
# https://docs.snowflake.com/en/developer-guide/external-network-access/secret-api-reference#python-api-for-secret-access

# As per contract with SCLS SPCS, secret path is stored in an environment variable
SCLS_SPCS_SECRET_ENV_NAME = "SCLS_SPCS_SECRET_PATH"


class UsernamePassword:
    def __init__(self, username, password) -> None:
        self.username = username
        self.password = password


class CloudProviderToken:
    def __init__(self, id, key, token) -> None:
        self.access_key_id = id
        self.secret_access_key = key
        self.token = token


def _get_scls_spcs_base_path():
    return os.getenv(SCLS_SPCS_SECRET_ENV_NAME, None)


def _get_scls_spcs_secret_dir(secret_name: str) -> str:
    base = _get_scls_spcs_base_path()
    if not base:
        raise NotImplementedError(
            "Secret API is only supported on Snowflake server and Spark Classic's SPCS container environments."
        )
    secret_dir = os.path.join(base, secret_name)
    if not os.path.exists(secret_dir):
        raise FileNotFoundError(f"Secret directory not found: {secret_dir}")
    if not os.path.isdir(secret_dir):
        raise NotADirectoryError(f"Secret path is not a directory: {secret_dir}")
    return secret_dir


def _get_scls_spcs_secret_file(secret_name: str, filename: str) -> str:
    base = _get_scls_spcs_base_path()
    if not base:
        raise NotImplementedError(
            "Secret API is only supported on Snowflake server and Spark Classic's SPCS container environments."
        )
    secret_path = os.path.join(base, secret_name, filename)
    if not os.path.exists(secret_path):
        raise FileNotFoundError(f"Secret file not found: {secret_path}")
    if not os.path.isfile(secret_path):
        raise FileNotFoundError(f"Secret path is not a file: {secret_path}")
    with open(secret_path, encoding="utf-8") as f:
        return f.read().rstrip("\r\n")


def get_generic_secret_string(secret_name: str) -> str:
    """Get a generic token string from Snowflake.
    Note:
        Require a Snowflake environment with generic secret strings configured
    Returns:
        The secret value as a string.
    Raises:
        NotImplementedError: If running outside Snowflake server or SPCS environment.
        FileNotFoundError: If the secret files don't exist (SPCS only).
    """
    try:
        import _snowflake

        return _snowflake.get_generic_secret_string(secret_name)
    except ImportError:
        return _get_scls_spcs_secret_file(secret_name, "secret_string")


def get_oauth_access_token(secret_name: str) -> str:
    """Get an OAuth2 access token from Snowflake.
    Note:
        Require a Snowflake environment with OAuth secrets configured
    Returns:
        The OAuth2 access token as a string.
    Raises:
        NotImplementedError: If running outside Snowflake server or SPCS environment.
        FileNotFoundError: If the secret files don't exist (SPCS only).
    """
    try:
        import _snowflake

        return _snowflake.get_oauth_access_token(secret_name)
    except ImportError:
        return _get_scls_spcs_secret_file(secret_name, "access_token")


def get_secret_type(secret_name: str) -> str:
    """Get the type of a secret from Snowflake.
    Note:
        Require a Snowflake environment with secrets configured
    Returns:
        The type of the secret as a string.
    Raises:
        NotImplementedError: If running outside Snowflake server or SPCS environment.
        FileNotFoundError: If the secret directory or files don't exist (SPCS only).
        NotADirectoryError: If the secret path is not a directory (SPCS only).
        ValueError: If the secret directory contains unexpected files (SPCS only).
    """
    try:
        import _snowflake

        return str(_snowflake.get_secret_type(secret_name))
    except ImportError:
        secret_dir = _get_scls_spcs_secret_dir(secret_name)
        entries = os.listdir(secret_dir)
        files = {
            f.upper()
            for f in entries
            if not f.startswith(".") and os.path.isfile(os.path.join(secret_dir, f))
        }

        if len(files) == 0:
            raise FileNotFoundError(f"No secret files found in directory: {secret_dir}")
        if files == {"USERNAME", "PASSWORD"}:
            return "PASSWORD"
        if len(files) == 1:
            file = next(iter(files))
            if file == "SECRET_STRING":
                return "GENERIC_STRING"
            elif file == "ACCESS_TOKEN":
                return "OAUTH2"
            else:
                raise ValueError(
                    f"Unknown secret file type '{file}' in directory: {secret_dir}"
                )
        raise ValueError(
            f"Secret directory contains unexpected files: {sorted(files)} in {secret_dir}"
        )


def get_username_password(secret_name: str) -> UsernamePassword:
    """Get a username and password secret from Snowflake.
    Note:
        Require a Snowflake environment with username/password secrets configured
    Returns:
        UsernamePassword: An object with attributes ``username`` and ``password``.
    Raises:
        NotImplementedError: If running outside Snowflake server or SPCS environment.
        FileNotFoundError: If the secret files don't exist (SPCS only).
    """
    try:
        import _snowflake

        secret_object = _snowflake.get_username_password(secret_name)
        return UsernamePassword(secret_object.username, secret_object.password)
    except ImportError:
        username = _get_scls_spcs_secret_file(secret_name, "username")
        password = _get_scls_spcs_secret_file(secret_name, "password")
        return UsernamePassword(username, password)


def get_cloud_provider_token(secret_name: str) -> CloudProviderToken:
    """Get a cloud provider token secret from Snowflake.
    Note:
        Require a Snowflake environment with cloud provider secrets configured
    Returns:
        CloudProviderToken: An object with attributes ``access_key_id``,
        ``secret_access_key``, and ``token``.
    Raises:
        NotImplementedError: If running outside Snowflake server environment.
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
        # SPCS container currently does not support cloud provider token secrets
        raise NotImplementedError(
            "Cannot import _snowflake module. Secret API is only supported on Snowflake server environment."
        )
