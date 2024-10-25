#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.mock._connection import MockServerConnection


def test_session(session):
    assert session.ast_enabled
    assert isinstance(session._conn, MockServerConnection)
