#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import snowflake.snowpark.mock._constants
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.mock._constants import (
    CURRENT_ACCOUNT,
    CURRENT_DATABASE,
    CURRENT_ROLE,
    CURRENT_SCHEMA,
    CURRENT_USER,
    CURRENT_WAREHOUSE,
)
from snowflake.snowpark.session import Session

# @mock.patch(
#     "snowflake.snowpark.session._is_execution_environment_sandboxed"
# )
# @pytest.mark.parametrize("is_sandboxed", [False, True])
# def test_get_current_session(sandbox_patch, is_sandboxed):
#     sandbox_patch.return_value = is_sandboxed
#     test_parameter = {
#         "account": "test_account",
#         "user": "test_user",
#         "schema": "test_schema",
#         "database": "test_database",
#         "warehouse": "test_warehouse",
#         "role": "test_role",
#         "local_testing": True,
#     }
#     session = Session.builder.configs(options=test_parameter).create()
#     session = snowflake.snowpark.session._get_sandbox_conditional_active_session(session)
#     if is_sandboxed:
#         assert session is None
#     else:
#         assert session is not None


def test_connection_get_current_parameter():
    # test no option
    conn = MockServerConnection()
    assert conn._get_current_parameter("random_option") is None
    assert conn._get_current_parameter("account") == f'"{CURRENT_ACCOUNT.upper()}"'
    assert (
        conn._get_current_parameter("account", quoted=False) == CURRENT_ACCOUNT.upper()
    )

    # test given option
    conn = MockServerConnection(options={"account": "test_account"})
    assert conn._get_current_parameter("account") == '"TEST_ACCOUNT"'
    assert conn._get_current_parameter("account", quoted=False) == "TEST_ACCOUNT"
    assert conn._get_current_parameter("non_existing_option") is None


def test_session_get_current_info(monkeypatch):
    # test given db information
    test_parameter = {
        "account": "test_account",
        "user": "test_user",
        "schema": "test_schema",
        "database": "test_database",
        "warehouse": "test_warehouse",
        "role": "test_role",
        "local_testing": True,
    }
    session = Session.builder.configs(options=test_parameter).create()
    assert session.get_current_account() == f'"{test_parameter["account"].upper()}"'
    assert session.get_current_user() == f'"{test_parameter["user"].upper()}"'
    assert session.get_current_warehouse() == f'"{test_parameter["warehouse"].upper()}"'
    assert session.get_current_schema() == f'"{test_parameter["schema"].upper()}"'
    assert session.get_current_database() == f'"{test_parameter["database"].upper()}"'
    assert session.get_current_role() == f'"{test_parameter["role"].upper()}"'

    # test no given db information
    session = Session.builder.configs(options={"local_testing": True}).create()
    assert session.get_current_account() == f'"{CURRENT_ACCOUNT.upper()}"'
    assert session.get_current_user() == f'"{CURRENT_USER.upper()}"'
    assert session.get_current_warehouse() == f'"{CURRENT_WAREHOUSE.upper()}"'
    assert session.get_current_schema() == f'"{CURRENT_SCHEMA.upper()}"'
    assert session.get_current_database() == f'"{CURRENT_DATABASE.upper()}"'
    assert session.get_current_role() == f'"{CURRENT_ROLE.upper()}"'

    # test update module variable
    monkeypatch.setattr(
        snowflake.snowpark.mock._constants, "CURRENT_ACCOUNT", test_parameter["account"]
    )
    monkeypatch.setattr(
        snowflake.snowpark.mock._constants, "CURRENT_USER", test_parameter["user"]
    )
    monkeypatch.setattr(
        snowflake.snowpark.mock._constants,
        "CURRENT_WAREHOUSE",
        test_parameter["warehouse"],
    )
    monkeypatch.setattr(
        snowflake.snowpark.mock._constants, "CURRENT_SCHEMA", test_parameter["schema"]
    )
    monkeypatch.setattr(
        snowflake.snowpark.mock._constants,
        "CURRENT_DATABASE",
        test_parameter["database"],
    )
    monkeypatch.setattr(
        snowflake.snowpark.mock._constants, "CURRENT_ROLE", test_parameter["role"]
    )

    session = Session.builder.configs(options={"local_testing": True}).create()
    assert session.get_current_account() == f'"{test_parameter["account"].upper()}"'
    assert session.get_current_user() == f'"{test_parameter["user"].upper()}"'
    assert session.get_current_warehouse() == f'"{test_parameter["warehouse"].upper()}"'
    assert session.get_current_schema() == f'"{test_parameter["schema"].upper()}"'
    assert session.get_current_database() == f'"{test_parameter["database"].upper()}"'
    assert session.get_current_role() == f'"{test_parameter["role"].upper()}"'

    session.close()


def test_session_use_object():
    session = Session.builder.configs(options={"local_testing": True}).create()

    session.use_schema("test_schema")
    session.use_role("test_role")
    session.use_database("test_database")
    session.use_warehouse("test_warehouse")

    assert session.get_current_account() == f'"{CURRENT_ACCOUNT.upper()}"'
    assert session.get_current_user() == f'"{CURRENT_USER.upper()}"'
    assert session.get_current_warehouse() == '"TEST_WAREHOUSE"'
    assert session.get_current_schema() == '"TEST_SCHEMA"'
    assert session.get_current_database() == '"TEST_DATABASE"'
    assert session.get_current_role() == '"TEST_ROLE"'

    session.close()
