#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import decimal

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import (
    call_table_function,
    col,
    lit,
    parse_json,
    table_function,
)
from tests.utils import Utils


def test_query_args(session):
    split_to_table = table_function("split_to_table")
    df = session.table_function(split_to_table(lit("Hello Table Function"), lit(" ")))
    assert df.columns == ["SEQ", "INDEX", "VALUE"]
    Utils.check_answer(
        df, [Row(1, 1, "Hello"), Row(1, 2, "Table"), Row(1, 3, "Function")]
    )


def test_query_kwargs(session):
    flatten = table_function("flatten")
    df = session.table_function(flatten(input=parse_json(lit("[1, 2]"))))
    assert df.columns == ["SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS"]
    Utils.check_answer(df.select("VALUE"), [Row("1"), Row("2")])


def test_query_over_clause(session):
    split_to_table = table_function("split_to_table")
    df = session.create_dataframe(
        [["Hello World", "p1", 1], ["Hello Python", "p2", 2]],
        schema=["text", "partition", "seq"],
    )
    df1 = df.join_table_function(split_to_table("text", lit(" ")).over())
    assert "OVER ( )" in df1.queries["queries"][0]
    assert df1.columns == ["TEXT", "PARTITION", "SEQ", "SEQ", "INDEX", "VALUE"]

    df2 = df.join_table_function(
        split_to_table("text", lit(" ")).over(partition_by="partition", order_by="seq")
    )
    assert (
        'OVER (PARTITION BY "PARTITION"  ORDER BY "SEQ" ASC NULLS FIRST))'
        in df2.queries["queries"][0]
    )
    assert df2.columns == ["TEXT", "PARTITION", "SEQ", "SEQ", "INDEX", "VALUE"]

    df3 = df.join_table_function(
        split_to_table("text", lit(" ")).over(
            partition_by=["partition"], order_by=["seq"]
        )
    )
    assert (
        'OVER (PARTITION BY "PARTITION"  ORDER BY "SEQ" ASC NULLS FIRST))'
        in df2.queries["queries"][0]
    )
    assert df3.columns == ["TEXT", "PARTITION", "SEQ", "SEQ", "INDEX", "VALUE"]

    df4 = df.join_table_function(
        split_to_table("text", lit(" ")).over(
            partition_by=[col("partition")], order_by=[col("seq")]
        )
    )
    assert (
        'OVER (PARTITION BY "PARTITION"  ORDER BY "SEQ" ASC NULLS FIRST))'
        in df2.queries["queries"][0]
    )
    assert df4.columns == ["TEXT", "PARTITION", "SEQ", "SEQ", "INDEX", "VALUE"]


def test_call_table_function(session):
    df = session.create_dataframe(
        [["Hello World", "p1", 1], ["Hello Python", "p2", 2]],
        schema=["text", "partition", "seq"],
    )
    df1 = df.join_table_function(
        call_table_function("split_to_table", "text", lit(" ")).over()
    )
    assert "OVER ( )" in df1.queries["queries"][0]
    assert df1.columns == ["TEXT", "PARTITION", "SEQ", "SEQ", "INDEX", "VALUE"]

    df2 = df.join_table_function(
        call_table_function("split_to_table", "text", lit(" ")).over(
            partition_by="partition", order_by="seq"
        )
    )
    assert (
        'OVER (PARTITION BY "PARTITION"  ORDER BY "SEQ" ASC NULLS FIRST))'
        in df2.queries["queries"][0]
    )
    assert df2.columns == ["TEXT", "PARTITION", "SEQ", "SEQ", "INDEX", "VALUE"]

    df3 = df.join_table_function(
        call_table_function("split_to_table", "text", lit(" ")).over(
            partition_by=["partition"], order_by=["seq"]
        )
    )
    assert (
        'OVER (PARTITION BY "PARTITION"  ORDER BY "SEQ" ASC NULLS FIRST))'
        in df2.queries["queries"][0]
    )
    assert df3.columns == ["TEXT", "PARTITION", "SEQ", "SEQ", "INDEX", "VALUE"]

    df4 = df.join_table_function(
        call_table_function("split_to_table", "text", lit(" ")).over(
            partition_by=[col("partition")], order_by=[col("seq")]
        )
    )
    assert (
        'OVER (PARTITION BY "PARTITION"  ORDER BY "SEQ" ASC NULLS FIRST))'
        in df2.queries["queries"][0]
    )
    assert df4.columns == ["TEXT", "PARTITION", "SEQ", "SEQ", "INDEX", "VALUE"]
