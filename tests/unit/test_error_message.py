#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from snowflake.connector import OperationalError, ProgrammingError
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.exceptions import SnowparkSQLException


@pytest.mark.parametrize("debug_context", [None, " debug_context"])
def test_sql_exception_from_programming_error(debug_context):
    pe = ProgrammingError(
        msg="test message",
        errno=123,
        sfqid="0000-1111",
        query="SELECT CURRENT_USER()",
    )
    ex = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
        pe, debug_context
    )
    assert type(ex) == SnowparkSQLException
    assert ex.error_code == "1304"
    assert ex.conn_error == pe
    assert ex.sfqid == "0000-1111"
    assert ex.query == "SELECT CURRENT_USER()"
    assert ex.message == "000123: test message"
    assert ex.sql_error_code == 123
    assert ex.raw_message == "test message"
    assert ex.debug_context == debug_context

    assert str(ex) == f"(1304): 0000-1111: 000123: test message{debug_context or ''}"


def test_sql_exception_from_operational_error():
    oe = OperationalError(
        msg="test message",
        errno=123,
        sfqid="0000-1111",
        query="SELECT CURRENT_USER()",
    )
    ex = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_OPERATIONAL_ERROR(oe)
    assert type(ex) == SnowparkSQLException
    assert ex.error_code == "1305"
    assert ex.conn_error == oe
    assert ex.sfqid == "0000-1111"
    assert ex.query == "SELECT CURRENT_USER()"
    assert ex.message == "000123: test message"
    assert ex.sql_error_code == 123
    assert ex.raw_message == "test message"
