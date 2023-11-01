#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from snowflake.connector import ProgrammingError
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages


def test_programming_error_sql_exception_attributes():
    pe = ProgrammingError(
        msg="errmsg",
        errno=123456,
        sqlstate="P0000",
        sfqid="the_query_id",
        query="select * from foo",
    )
    sql_exception = (
        SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(pe)
    )
    assert sql_exception.sql_error_code == 123456
    assert sql_exception.raw_message == "errmsg"
