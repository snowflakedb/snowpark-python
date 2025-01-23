#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.mock._connection import MockServerConnection


def test_ensure_valid_test_setup(session):
    assert session.ast_enabled
    assert isinstance(session._conn, MockServerConnection)
