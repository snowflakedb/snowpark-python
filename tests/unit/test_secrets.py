#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from types import SimpleNamespace
from unittest.mock import patch
from snowflake.snowpark.secrets import (
    get_generic_secret_string,
    get_oauth_access_token,
    get_secret_type,
    get_username_password,
    get_cloud_provider_token,
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
        assert creds == {"username": "user1", "password": "pass1"}

        cloud = get_cloud_provider_token("c1")
        assert cloud == {
            "access_key_id": "AKIA_TEST",
            "secret_access_key": "SECRET_TEST",
            "token": "STS_TOKEN_TEST",
        }


def test_secrets_import_error_paths():
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
