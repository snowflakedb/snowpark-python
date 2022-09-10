#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from unittest import mock

import pytest

import snowflake.snowpark.session
from snowflake.connector import SnowflakeConnection
from snowflake.snowpark import Session
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.session import _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING


def test_aliases():
    assert Session.createDataFrame == Session.create_dataframe


@pytest.mark.parametrize(
    "account, role,database,schema,warehouse",
    [("ACCOUNT", "ADMIN", "DB", "SCHEMA", "WH"), (None, None, None, None, None)],
)
def test_str(account, role, database, schema, warehouse):
    mock_sf_connection = mock.create_autospec(
        SnowflakeConnection,
        account=account,
        role=role,
        database=database,
        schema=schema,
        warehouse=warehouse,
        _telemetry=None,
        _session_parameters={},
    )
    mock_sf_connection.is_closed.return_value = False
    mock_server_connection = ServerConnection({}, mock_sf_connection)

    def quoted(s):
        return f'"{s}"' if s else s

    assert (
        str(Session(mock_server_connection))
        == f"<snowflake.snowpark.session.Session: account={quoted(account)}, role={quoted(role)}, database={quoted(database)}, schema={quoted(schema)}, warehouse={quoted(warehouse)}>"
    )


def test_used_scoped_temp_object():
    fake_connection = mock.create_autospec(ServerConnection)
    fake_connection._conn = mock.Mock()

    # by default module level config is on
    fake_connection._conn._session_parameters = None
    assert Session(fake_connection)._use_scoped_temp_objects is True

    fake_connection._conn._session_parameters = {}
    assert Session(fake_connection)._use_scoped_temp_objects is True

    fake_connection._conn._session_parameters = {
        _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING: True
    }
    assert Session(fake_connection)._use_scoped_temp_objects is True

    fake_connection._conn._session_parameters = {
        _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING: False
    }
    assert Session(fake_connection)._use_scoped_temp_objects is False

    # turn off module level config
    snowflake.snowpark.session._use_scoped_temp_objects = False

    fake_connection._conn._session_parameters = {}
    assert Session(fake_connection)._use_scoped_temp_objects is False

    fake_connection._conn._session_parameters = {
        _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING: True
    }
    assert Session(fake_connection)._use_scoped_temp_objects is False

    fake_connection._conn._session_parameters = {
        _PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS_STRING: False
    }
    assert Session(fake_connection)._use_scoped_temp_objects is False
