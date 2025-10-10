#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import pytest
from types import SimpleNamespace
from unittest.mock import patch
from snowflake.snowpark.secrets import (
    get_generic_secret_string,
    get_oauth_access_token,
    get_secret_type,
    get_username_password,
    get_cloud_provider_token,
    UsernamePassword,
    CloudProviderToken,
    SCLS_SPCS_SECRET_ENV_NAME,
)


def _build_fake_snowflake_module() -> object:
    fake_username_password = SimpleNamespace(username="user1", password="pass1")
    fake_cloud_token = SimpleNamespace(
        access_key_id="AKIA_TEST",
        secret_access_key="SECRET_TEST",
        token="STS_TOKEN_TEST",
    )
    return SimpleNamespace(
        get_generic_secret_string=lambda secret_name: f"generic:{secret_name}",
        get_oauth_access_token=lambda secret_name: f"oauth:{secret_name}",
        get_secret_type=lambda secret_name: "PASSWORD",
        get_username_password=lambda secret_name: fake_username_password,
        get_cloud_provider_token=lambda secret_name: fake_cloud_token,
    )


def test_secrets_mock_server_paths():
    fake_module = _build_fake_snowflake_module()
    with patch.dict("sys.modules", {"_snowflake": fake_module}):
        assert get_generic_secret_string("s1") == "generic:s1"
        assert get_oauth_access_token("o1") == "oauth:o1"
        assert get_secret_type("t1") == "PASSWORD"

        creds = get_username_password("p1")
        assert isinstance(creds, UsernamePassword)
        assert creds.username == "user1"
        assert creds.password == "pass1"

        cloud = get_cloud_provider_token("c1")
        assert isinstance(cloud, CloudProviderToken)
        assert cloud.access_key_id == "AKIA_TEST"
        assert cloud.secret_access_key == "SECRET_TEST"
        assert cloud.token == "STS_TOKEN_TEST"


@pytest.fixture
def scls_spcs_mock_env(tmp_path):
    secret_base = tmp_path / "test_scls_spcs_creds"
    secret_base.mkdir()

    original_env = os.environ.get(SCLS_SPCS_SECRET_ENV_NAME)
    os.environ[SCLS_SPCS_SECRET_ENV_NAME] = str(secret_base)

    yield secret_base

    if original_env is not None:
        os.environ[SCLS_SPCS_SECRET_ENV_NAME] = original_env
    else:
        os.environ.pop(SCLS_SPCS_SECRET_ENV_NAME, None)


def test_secrets_mock_scls_spcs(scls_spcs_mock_env):
    generic_dir = scls_spcs_mock_env / "my_generic_secret"
    generic_dir.mkdir()
    (generic_dir / "secret_string").write_text("my_secret_value")

    oauth_dir = scls_spcs_mock_env / "my_oauth_secret"
    oauth_dir.mkdir()
    (oauth_dir / "access_token").write_text("oauth_token_12345")

    password_dir = scls_spcs_mock_env / "my_password_secret"
    password_dir.mkdir()
    (password_dir / "username").write_text("test_user")
    (password_dir / "password").write_text("test_pass")

    with patch.dict("sys.modules", {"_snowflake": None}):
        assert get_generic_secret_string("my_generic_secret") == "my_secret_value"
        assert get_oauth_access_token("my_oauth_secret") == "oauth_token_12345"

        creds = get_username_password("my_password_secret")
        assert isinstance(creds, UsernamePassword)
        assert creds.username == "test_user"
        assert creds.password == "test_pass"

        assert get_secret_type("my_generic_secret") == "GENERIC_STRING"
        assert get_secret_type("my_oauth_secret") == "OAUTH2"
        assert get_secret_type("my_password_secret") == "PASSWORD"


def test_secrets_mock_scls_spcs_error_cases(scls_spcs_mock_env):
    edge_dir = scls_spcs_mock_env / "edge"
    edge_dir.mkdir()
    (edge_dir / "secret_string").write_text("value\n")
    (edge_dir / ".DS_Store").write_text("garbage")
    (edge_dir / "unknown_file").write_text("extra")

    empty_dir = scls_spcs_mock_env / "empty"
    empty_dir.mkdir()

    file_as_dir = scls_spcs_mock_env / "notdir"
    file_as_dir.write_text("fake")

    with patch.dict("sys.modules", {"_snowflake": None}):
        assert get_generic_secret_string("edge") == "value"

        with pytest.raises(ValueError, match="unexpected files"):
            get_secret_type("edge")

        with pytest.raises(FileNotFoundError, match="Secret directory not found"):
            get_secret_type("nonexistent")

        with pytest.raises(FileNotFoundError, match="Secret file not found"):
            get_generic_secret_string("empty")

        with pytest.raises(FileNotFoundError, match="No secret files found"):
            get_secret_type("empty")

        with pytest.raises(NotADirectoryError, match="not a directory"):
            get_secret_type("notdir")


def test_secrets_import_error_paths():
    original_env = os.environ.get(SCLS_SPCS_SECRET_ENV_NAME)
    if SCLS_SPCS_SECRET_ENV_NAME in os.environ:
        del os.environ[SCLS_SPCS_SECRET_ENV_NAME]

    try:
        with patch.dict("sys.modules", {"_snowflake": None}):
            if "_snowflake" in __import__("sys").modules:
                del __import__("sys").modules["_snowflake"]

        with pytest.raises(NotImplementedError):
            get_generic_secret_string("s1")
        with pytest.raises(NotImplementedError):
            get_oauth_access_token("o1")
        with pytest.raises(NotImplementedError):
            get_secret_type("t1")
        with pytest.raises(NotImplementedError):
            get_username_password("p1")
        with pytest.raises(NotImplementedError):
            get_cloud_provider_token("c1")
    finally:
        if original_env is not None:
            os.environ[SCLS_SPCS_SECRET_ENV_NAME] = original_env
