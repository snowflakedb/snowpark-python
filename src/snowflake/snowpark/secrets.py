#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os
from abc import ABC, abstractmethod

# Reference for Python API for Secret Access:
# https://docs.snowflake.com/en/developer-guide/external-network-access/secret-api-reference#python-api-for-secret-access

# As per contract with SCLS SPCS, secret path is stored in an environment variable
SCLS_SPCS_SECRET_ENV_NAME = "SNOWFLAKE_CONTAINER_SERVICES_SECRET_PATH_PREFIX"

__all__ = [
    "get_generic_secret_string",
    "get_oauth_access_token",
    "get_secret_type",
    "get_username_password",
    "get_cloud_provider_token",
    "UsernamePassword",
    "CloudProviderToken",
]


class UsernamePassword:
    def __init__(self, username, password) -> None:
        self.username = username
        self.password = password


class CloudProviderToken:
    def __init__(self, id, key, token) -> None:
        self.access_key_id = id
        self.secret_access_key = key
        self.token = token


class _SnowflakeSecrets(ABC):
    """Abstract class to access Snowflake secrets in different environments."""

    @abstractmethod
    def get_generic_secret_string(self, secret_name: str) -> str:
        pass

    @abstractmethod
    def get_oauth_access_token(self, secret_name: str) -> str:
        pass

    @abstractmethod
    def get_secret_type(self, secret_name: str) -> str:
        pass

    @abstractmethod
    def get_username_password(self, secret_name: str) -> UsernamePassword:
        pass

    @abstractmethod
    def get_cloud_provider_token(self, secret_name: str) -> CloudProviderToken:
        pass


class _SnowflakeSecretsServer(_SnowflakeSecrets):
    """Secret instance for Snowflake server environment (using _snowflake module)."""

    def __init__(self, snowflake_module) -> None:
        self._snowflake = snowflake_module

    def get_generic_secret_string(self, secret_name: str) -> str:
        return self._snowflake.get_generic_secret_string(secret_name)

    def get_oauth_access_token(self, secret_name: str) -> str:
        return self._snowflake.get_oauth_access_token(secret_name)

    def get_secret_type(self, secret_name: str) -> str:
        return str(self._snowflake.get_secret_type(secret_name))

    def get_username_password(self, secret_name: str) -> UsernamePassword:
        secret_object = self._snowflake.get_username_password(secret_name)
        return UsernamePassword(secret_object.username, secret_object.password)

    def get_cloud_provider_token(self, secret_name: str) -> CloudProviderToken:
        secret_object = self._snowflake.get_cloud_provider_token(secret_name)
        return CloudProviderToken(
            secret_object.access_key_id,
            secret_object.secret_access_key,
            secret_object.token,
        )


class _SnowflakeSecretsSPCS(_SnowflakeSecrets):
    """Secret instance for SPCS container environment (file-based secrets)."""

    def _get_scls_spcs_base_path(self):
        base = os.getenv(SCLS_SPCS_SECRET_ENV_NAME, None)
        if not base:
            raise RuntimeError(
                f"Environment variable '{SCLS_SPCS_SECRET_ENV_NAME}' is not set or empty. "
                f"This variable must be set to the SPCS secret base path."
            )
        return base

    def _get_scls_spcs_secret_dir(self, secret_name: str) -> str:
        base = self._get_scls_spcs_base_path()
        secret_dir = os.path.join(base, secret_name)
        if not os.path.exists(secret_dir):
            raise FileNotFoundError(f"Secret directory not found: {secret_dir}")
        if not os.path.isdir(secret_dir):
            raise NotADirectoryError(f"Secret path is not a directory: {secret_dir}")
        return secret_dir

    def _read_scls_spcs_secret_file(self, secret_name: str, filename: str) -> str:
        base = self._get_scls_spcs_base_path()
        secret_path = os.path.join(base, secret_name, filename)
        if not os.path.exists(secret_path):
            raise FileNotFoundError(f"Secret file not found: {secret_path}")
        if not os.path.isfile(secret_path):
            raise FileNotFoundError(f"Secret path is not a file: {secret_path}")
        with open(secret_path, encoding="utf-8") as f:
            return f.read().rstrip("\r\n")

    def _get_scls_spcs_secret_type(self, secret_name: str) -> str:
        secret_dir = self._get_scls_spcs_secret_dir(secret_name)
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

    def get_generic_secret_string(self, secret_name: str) -> str:
        return self._read_scls_spcs_secret_file(secret_name, "secret_string")

    def get_oauth_access_token(self, secret_name: str) -> str:
        return self._read_scls_spcs_secret_file(secret_name, "access_token")

    def get_secret_type(self, secret_name: str) -> str:
        return self._get_scls_spcs_secret_type(secret_name)

    def get_username_password(self, secret_name: str) -> UsernamePassword:
        username = self._read_scls_spcs_secret_file(secret_name, "username")
        password = self._read_scls_spcs_secret_file(secret_name, "password")
        return UsernamePassword(username, password)

    def get_cloud_provider_token(self, secret_name: str) -> CloudProviderToken:
        # SPCS container currently does not support cloud provider token secrets
        raise NotImplementedError(
            "Cannot import _snowflake module. get_cloud_provider_token is only supported on Snowflake server environment."
        )


def _is_spcs_environment() -> bool:
    return os.getenv(SCLS_SPCS_SECRET_ENV_NAME, None) is not None


def _get_secrets_instance() -> _SnowflakeSecrets:
    """Detect environment and return appropriate secrets instance.

    Imports _snowflake once and passes it to avoid redundant imports.
    """
    try:
        import _snowflake

        return _SnowflakeSecretsServer(_snowflake)
    except ImportError:
        if _is_spcs_environment():
            return _SnowflakeSecretsSPCS()
        else:
            raise NotImplementedError(
                "Secret API is only supported on Snowflake server and Spark Classic's SPCS container environments."
            )


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
    return _get_secrets_instance().get_generic_secret_string(secret_name)


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
    return _get_secrets_instance().get_oauth_access_token(secret_name)


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
    return _get_secrets_instance().get_secret_type(secret_name)


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
    return _get_secrets_instance().get_username_password(secret_name)


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
    return _get_secrets_instance().get_cloud_provider_token(secret_name)
