#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from test.utils import TestData

import pytest

from snowflake.snowpark.functions import (
    builtin,
    call_builtin,
    col,
    count_distinct,
    parse_json,
    to_array,
    to_binary,
    to_date,
    to_object,
    to_variant,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types.sf_types import (
    ArrayType,
    DateType,
    MapType,
    StringType,
    VariantType,
)


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

        # Pass invalid type - list of numbers
        with pytest.raises(TypeError) as ex_info:
            df.select(count_distinct(123, 456))
        assert "COUNT_DISTINCT expected Column or str, got: <class 'int'>" in str(
            ex_info
        )

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

        # same as above, but pass str instead of Column
        assert TestData.null_json1(session).select(parse_json("v")).collect() == [
            Row('{\n  "a": null\n}'),
            Row('{\n  "a": "foo"\n}'),
            Row(None),
        ]


def test_to_date_to_array_to_variant_to_object(session_cnx):
    with session_cnx() as session:
        df = (
            session.createDataFrame([["2013-05-17", 1, 3.14, '{"a":1}']])
            .toDF("date", "array", "var", "obj")
            .withColumn("json", parse_json("obj"))
        )

        df1 = df.select(
            to_date("date"), to_array("array"), to_variant("var"), to_object("json")
        )
        df2 = df.select(
            to_date(col("date")),
            to_array(col("array")),
            to_variant(col("var")),
            to_object(col("json")),
        )

        res1, res2 = df1.collect(), df2.collect()
        assert res1 == res2
        assert df1.schema.fields[0].datatype == DateType()
        assert df1.schema.fields[1].datatype == ArrayType(StringType())
        assert df1.schema.fields[2].datatype == VariantType()
        assert df1.schema.fields[3].datatype == MapType(StringType(), StringType())


def test_to_binary(session_cnx):
    with session_cnx() as session:
        res = (
            TestData.test_data1(session)
            .toDF("a", "b", "c")
            .select(to_binary(col("c"), "utf-8"))
            .collect()
        )
        assert res == [Row(bytearray(b"a")), Row(bytearray(b"b"))]

        res = (
            TestData.test_data1(session)
            .toDF("a", "b", "c")
            .select(to_binary("c", "utf-8"))
            .collect()
        )
        assert res == [Row(bytearray(b"a")), Row(bytearray(b"b"))]

        # For NULL input, the output is NULL
        res = (
            TestData.all_nulls(session).toDF("a").select(to_binary(col("a"))).collect()
        )
        assert res == [Row(None), Row(None), Row(None), Row(None)]
