#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.connector import SnowflakeConnection
from snowflake.snowpark import Session
from snowflake.snowpark._internal.server_connection import ServerConnection


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
    )
    mock_sf_connection.is_closed.return_value = False
    mock_server_connection = ServerConnection({}, mock_sf_connection)

    def quoted(s):
        return f'"{s}"' if s else s

    assert (
        str(Session(mock_server_connection))
        == f"<snowflake.snowpark.session.Session: account={quoted(account)}, role={quoted(role)}, database={quoted(database)}, schema={quoted(schema)}, warehouse={quoted(warehouse)}>"
    )
