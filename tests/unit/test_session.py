#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

from snowflake.connector import SnowflakeConnection
from snowflake.snowpark import Session
from snowflake.snowpark._internal.server_connection import ServerConnection


def test_aliases():
    assert Session.createDataFrame == Session.create_dataframe


def test_repr():
    mock_sf_connection = mock.create_autospec(
        SnowflakeConnection, role="ROLE", database="DB", schema="SCHEMA", warehouse="WH"
    )
    mock_server_connection = mock.create_autospec(
        ServerConnection, _conn=mock_sf_connection
    )
    assert (
        repr(Session(mock_server_connection))
        == "<snowflake.snowpark.session.Session: role='ROLE', database='DB', schema='SCHEMA', warehouse='WH'>"
    )
