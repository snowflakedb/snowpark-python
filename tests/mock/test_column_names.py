#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.functions import avg, col
from snowflake.snowpark.mock._connection import MockServerConnection

session = Session(MockServerConnection())


@pytest.mark.localtest
def test_str_column_name_no_quotes():
    df = session.create_dataframe([1, 2], schema=["a"])
    assert str(df.select(col("a")).collect()) == "[Row(A=1), Row(A=2)]"
    assert str(df.select(avg(col("a"))).collect()) == '[Row(AVG("A")=1.5)]'

    # column name with quotes
    df = session.create_dataframe([1, 2], schema=['"a"'])
    assert str(df.select(col('"a"')).collect()) == "[Row(a=1), Row(a=2)]"
    assert str(df.select(avg(col('"a"'))).collect()) == '[Row(AVG("A")=1.5)]'


@pytest.mark.localtest
def test_show_column_name_with_quotes():
    df = session.create_dataframe([1, 2], schema=["a"])
    assert (
        df.select(col("a"))._show_string()
        == """\
-------
|"A"  |
-------
|1    |
|2    |
-------
"""
    )
    assert (
        df.select(avg(col("a")))._show_string()
        == """\
----------------
|"AVG(""A"")"  |
----------------
|1.5           |
----------------
"""
    )

    # column name with quotes
    df = session.create_dataframe([1, 2], schema=['"a"'])
    assert (
        df.select(col('"a"'))._show_string()
        == """\
-------
|"a"  |
-------
|1    |
|2    |
-------
"""
    )
    assert (
        df.select(avg(col('"a"')))._show_string()
        == """\
----------------
|"AVG(""A"")"  |
----------------
|1.5           |
----------------
"""
    )
