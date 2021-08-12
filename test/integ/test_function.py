#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from test.utils import TestData

import pytest

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark.functions import (
    builtin,
    call_builtin,
    col,
    count_distinct,
    parse_json,
)
from snowflake.snowpark.row import Row


def test_count_distinct(session_cnx):
    with session_cnx() as session:
        df = session.createDataFrame(
            [["a", 1, 1], ["b", 2, 2], ["c", 1, None], ["d", 5, None]]
        ).toDF(["id", "value", "other"])

        res = df.select(
            count_distinct(df["id"]),
            count_distinct(df["value"]),
            count_distinct(df["other"]),
        ).collect()
        assert res == [Row([4, 3, 2])]

        res = df.select(count_distinct(df["id"], df["value"])).collect()
        assert res == [Row([4])]

        # Pass invalid type - str
        with pytest.raises(TypeError) as ex_info:
            df.select(count_distinct("abc"))
        assert "Invalid input to count_distinct()." in str(ex_info)

        # Pass invalid type - list of str
        with pytest.raises(TypeError) as ex_info:
            df.select(count_distinct("abc", "abc"))
        assert "Invalid input to count_distinct()." in str(ex_info)

        assert df.select(count_distinct(df["*"])).collect() == [Row(2)]


def test_builtin_avg_from_range(session_cnx):
    """Tests the builtin functionality, using avg()."""
    with session_cnx() as session:
        avg = builtin("avg")

        df = session.range(1, 10, 2).select(avg(col("id")))
        res = df.collect()
        expected = [Row([5.000])]
        assert res == expected

        df = session.range(1, 10, 2).filter(col("id") > 2).select(avg(col("id")))
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected

        # Add extra select on existing column
        df = (
            session.range(1, 10, 2)
            .select("id")
            .filter(col("id") > 2)
            .select(avg(col("id")))
        )
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected

        # Add extra selects on existing column
        df = (
            session.range(1, 10, 2)
            .select("id")
            .select("id")
            .select("id")
            .select("id")
            .filter(col("id") > 2)
            .select(avg(col("id")))
        )
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected


def test_call_builtin_avg_from_range(session_cnx):
    """Tests the builtin functionality, using avg()."""
    with session_cnx() as session:
        df = session.range(1, 10, 2).select(call_builtin("avg", col("id")))
        res = df.collect()
        expected = [Row([5.000])]
        assert res == expected

        df = (
            session.range(1, 10, 2)
            .filter(col("id") > 2)
            .select(call_builtin("avg", col("id")))
        )
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected

        # Add extra select on existing column
        df = (
            session.range(1, 10, 2)
            .select("id")
            .filter(col("id") > 2)
            .select(call_builtin("avg", col("id")))
        )
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected

        # Add extra selects on existing column
        df = (
            session.range(1, 10, 2)
            .select("id")
            .select("id")
            .select("id")
            .select("id")
            .filter(col("id") > 2)
            .select(call_builtin("avg", col("id")))
        )
        res = df.collect()
        expected = [Row([6.000])]
        assert res == expected


def test_parse_json(session_cnx):
    with session_cnx() as session:
        assert TestData.null_json1(session).select(parse_json(col("v"))).collect() == [
            Row('{\n  "a": null\n}'),
            Row('{\n  "a": "foo"\n}'),
            Row(None),
        ]
