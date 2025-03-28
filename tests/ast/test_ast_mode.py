#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark.session import (
    AstMode,
    Session,
    _PYTHON_SNOWPARK_CLIENT_MIN_VERSION_FOR_AST,
    _PYTHON_SNOWPARK_CLIENT_AST_MODE,
    _PYTHON_SNOWPARK_USE_AST,
)
from snowflake.snowpark.version import VERSION
from snowflake.connector import SnowflakeConnection
from tests.parameters import CONNECTION_PARAMETERS


@pytest.fixture(autouse=True, scope="module")
def snowflake_connection() -> SnowflakeConnection:
    conn = SnowflakeConnection(None, None, **CONNECTION_PARAMETERS)
    yield conn
    conn.close()


@pytest.mark.parametrize(
    "use_ast,ast_mode,min_ver,expected_ast_mode,expect_downgrade_log,expect_min_ver_log",
    [
        # Test: None of the new client ast / version parameters are set, but PYTHON_SNOWPARK_USE_AST=false
        (False, None, None, AstMode.SQL_ONLY, False, False),
        # Test: None of the new client ast / version parameters are set, but PYTHON_SNOWPARK_USE_AST=true
        (True, None, None, AstMode.SQL_AND_AST, False, False),
        # Test: The new client ast mode parameters takes precedence over PYTHON_SNOWPARK_USE_AST value
        (True, AstMode.SQL_ONLY, VERSION, AstMode.SQL_ONLY, False, False),
        # Test: The new client ast mode parameters takes precedence over PYTHON_SNOWPARK_USE_AST value
        (False, AstMode.SQL_ONLY, VERSION, AstMode.SQL_ONLY, False, False),
        # Test: The new client ast mode SQL_ONLY is applied
        (None, AstMode.SQL_ONLY, VERSION, AstMode.SQL_ONLY, False, False),
        # Test: The new client ast mode SQL_AND_AST is applied
        (None, AstMode.SQL_AND_AST, VERSION, AstMode.SQL_AND_AST, False, False),
        # Test: AST_ONLY is downgraded to SQL_AND_AST since no support yet.
        (None, AstMode.AST_ONLY, VERSION, AstMode.SQL_AND_AST, True, False),
        # Test: The client ast mode SQL_ONLY is applied and ignores the min version check.
        (None, AstMode.SQL_ONLY, (99, 0, 0), AstMode.SQL_ONLY, False, False),
        # Test: AST_ONLY is downgraded to SQL_ONLY because not supported by client.
        (None, AstMode.AST_ONLY, (99, 0, 0), AstMode.SQL_ONLY, True, True),
        # Test: SQL_AND_AST is downgraded to SQL_ONLY because client version is too low.
        (None, AstMode.SQL_AND_AST, (99, 0, 0), AstMode.SQL_ONLY, False, True),
    ],
)
def test_snowpark_python_ast_mode_and_version(
    snowflake_connection,
    caplog,
    use_ast,
    ast_mode,
    min_ver,
    expected_ast_mode,
    expect_downgrade_log,
    expect_min_ver_log,
) -> None:
    snowflake_connection._session_parameters[_PYTHON_SNOWPARK_USE_AST] = (
        use_ast if use_ast is not None else None
    )
    snowflake_connection._session_parameters[_PYTHON_SNOWPARK_CLIENT_AST_MODE] = (
        int(ast_mode) if ast_mode is not None else None
    )
    snowflake_connection._session_parameters[
        _PYTHON_SNOWPARK_CLIENT_MIN_VERSION_FOR_AST
    ] = (".".join(str(v) for v in min_ver) if min_ver is not None else None)

    sess = (
        Session.SessionBuilder().configs({"connection": snowflake_connection}).create()
    )

    assert sess._ast_mode == expected_ast_mode
    assert (
        any(
            "Snowpark python client does not support dataframe requests, downgrading to SQL_AND_AST."
            in m
            for m in caplog.messages
        )
        == expect_downgrade_log
    )
    assert (
        any(
            "Server side dataframe support requires minimum snowpark-python client version."
            in m
            for m in caplog.messages
        )
        == expect_min_ver_log
    )
