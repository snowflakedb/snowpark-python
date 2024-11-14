#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.connector import OperationalError, ProgrammingError
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.exceptions import SnowparkSQLException


def test_sql_exception_from_programming_error():
    pe = ProgrammingError(
        msg="test message",
        errno=123,
        sfqid="0000-1111",
        query="SELECT CURRENT_USER()",
    )
    ex = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(pe)
    assert type(ex) == SnowparkSQLException
    assert ex.error_code == "1304"
    assert ex.conn_error == pe
    assert ex.sfqid == "0000-1111"
    assert ex.query == "SELECT CURRENT_USER()"
    assert ex.message == "000123: test message"
    assert ex.sql_error_code == 123
    assert ex.raw_message == "test message"


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
