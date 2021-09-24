#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from test.utils import TestData, Utils

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark.functions import (
    abs,
    array_agg,
    builtin,
    call_builtin,
    ceil,
    char,
    coalesce,
    col,
    contains,
    count_distinct,
    exp,
    floor,
    lit,
    log,
    negate,
    not_,
    parse_json,
    parse_xml,
    pow,
    random,
    split,
    sqrt,
    startswith,
    substring,
    to_array,
    to_binary,
    to_date,
    to_json,
    to_object,
    to_variant,
    to_xml,
    translate,
    trim,
    upper,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import (
    ArrayType,
    DateType,
    MapType,
    StringType,
    VariantType,
)


def test_basic_numerical_operations_negative(session):
    # sqrt
    df = session.sql("select 4").toDF("a")
    with pytest.raises(TypeError) as ex_info:
        df.select(sqrt([1])).collect()
    assert "SQRT expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(sqrt(lit(-1))).collect()
    assert "Invalid floating point operation: sqrt(-1)" in str(ex_info)

    # abs
    with pytest.raises(TypeError) as ex_info:
        df.select(abs([None])).collect()
    assert "ABS expected Column or str, got: <class 'list'>" in str(ex_info)

    # exp
    with pytest.raises(TypeError) as ex_info:
        df.select(exp([None])).collect()
    assert "EXP expected Column or str, got: <class 'list'>" in str(ex_info)

    # log
    with pytest.raises(TypeError) as ex_info:
        df.select(log([None], "a")).collect()
    assert "LOG expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(log("a", [123])).collect()
    assert "LOG expected Column or str, got: <class 'list'>" in str(ex_info)

    # pow
    with pytest.raises(TypeError) as ex_info:
        df.select(pow([None], "a")).collect()
    assert "POW expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(pow("a", [123])).collect()
    assert "POW expected Column or str, got: <class 'list'>" in str(ex_info)

    # floor
    with pytest.raises(TypeError) as ex_info:
        df.select(floor([None])).collect()
    assert "FLOOR expected Column or str, got: <class 'list'>" in str(ex_info)

    # ceil
    with pytest.raises(TypeError) as ex_info:
        df.select(ceil([None])).collect()
    assert "CEIL expected Column or str, got: <class 'list'>" in str(ex_info)


def test_basic_string_operations(session):
    # Substring
    df = session.sql("select 'a not that long string'").toDF("a")
    with pytest.raises(ProgrammingError) as ex_info:
        df.select(substring("a", "b", 1)).collect()
    assert "Numeric value 'b' is not recognized" in str(ex_info)

    # substring - negative lenght yields empty string
    res = df.select(substring("a", 6, -1)).collect()
    assert len(res) == 1
    assert len(res[0]) == 1
    assert res[0][0] == ""

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(substring("a", 1, "c")).collect()
    assert "Numeric value 'c' is not recognized" in str(ex_info)

    # split
    res = df.select(split("a", lit("not"))).collect()
    assert res == [Row("""[\n  "a ",\n  " that long string"\n]""")]

    with pytest.raises(TypeError) as ex_info:
        df.select(split([1, 2, 3], "b")).collect()
    assert "SPLIT expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(split("a", [1, 2, 3])).collect()
    assert "SPLIT expected Column or str, got: <class 'list'>" in str(ex_info)

    # upper
    with pytest.raises(TypeError) as ex_info:
        df.select(upper([1])).collect()
    assert "UPPER expected Column or str, got: <class 'list'>" in str(ex_info)

    # contains
    with pytest.raises(TypeError) as ex_info:
        df.select(contains("a", [1])).collect()
    assert "CONTAINS expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(contains([1], "b")).collect()
    assert "CONTAINS expected Column or str, got: <class 'list'>" in str(ex_info)

    # startswith
    with pytest.raises(TypeError) as ex_info:
        df.select(startswith("a", [1])).collect()
    assert "STARTSWITH expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(startswith([1], "b")).collect()
    assert "STARTSWITH expected Column or str, got: <class 'list'>" in str(ex_info)

    # char
    with pytest.raises(TypeError) as ex_info:
        df.select(char([1])).collect()
    assert "CHAR expected Column or str, got: <class 'list'>" in str(ex_info)

    # translate
    with pytest.raises(TypeError) as ex_info:
        df.select(translate("a", "b", [1])).collect()
    assert "TRANSLATE expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(translate("a", [1], "c")).collect()
    assert "TRANSLATE expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(translate([1], "a", "c")).collect()
    assert "TRANSLATE expected Column or str, got: <class 'list'>" in str(ex_info)

    # trim
    with pytest.raises(TypeError) as ex_info:
        df.select(trim("a", [1])).collect()
    assert "TRIM expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(trim([1], "b")).collect()
    assert "TRIM expected Column or str, got: <class 'list'>" in str(ex_info)


def test_count_distinct(session):
    df = session.createDataFrame(
        [["a", 1, 1], ["b", 2, 2], ["c", 1, None], ["d", 5, None]]
    ).toDF(["id", "value", "other"])

    res = df.select(
        count_distinct(df["id"]),
        count_distinct(df["value"]),
        count_distinct(df["other"]),
    ).collect()
    assert res == [Row(4, 3, 2)]

    res = df.select(count_distinct(df["id"], df["value"])).collect()
    assert res == [Row(4)]

    # Pass invalid type - list of numbers
    with pytest.raises(TypeError) as ex_info:
        df.select(count_distinct(123, 456))
    assert "COUNT_DISTINCT expected Column or str, got: <class 'int'>" in str(ex_info)

    assert df.select(count_distinct(df["*"])).collect() == [Row(2)]


def test_builtin_avg_from_range(session):
    """Tests the builtin functionality, using avg()."""
    avg = builtin("avg")

    df = session.range(1, 10, 2).select(avg(col("id")))
    res = df.collect()
    expected = [Row(5.000)]
    assert res == expected

    df = session.range(1, 10, 2).filter(col("id") > 2).select(avg(col("id")))
    res = df.collect()
    expected = [Row(6.000)]
    assert res == expected

    # Add extra select on existing column
    df = (
        session.range(1, 10, 2)
        .select("id")
        .filter(col("id") > 2)
        .select(avg(col("id")))
    )
    res = df.collect()
    expected = [Row(6.000)]
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
    expected = [Row(6.000)]
    assert res == expected


def test_call_builtin_avg_from_range(session):
    """Tests the builtin functionality, using avg()."""
    df = session.range(1, 10, 2).select(call_builtin("avg", col("id")))
    res = df.collect()
    expected = [Row(5.000)]
    assert res == expected

    df = (
        session.range(1, 10, 2)
        .filter(col("id") > 2)
        .select(call_builtin("avg", col("id")))
    )
    res = df.collect()
    expected = [Row(6.000)]
    assert res == expected

    # Add extra select on existing column
    df = (
        session.range(1, 10, 2)
        .select("id")
        .filter(col("id") > 2)
        .select(call_builtin("avg", col("id")))
    )
    res = df.collect()
    expected = [Row(6.000)]
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
    expected = [Row(6.000)]
    assert res == expected


def test_parse_json(session):
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


def test_to_date_to_array_to_variant_to_object(session):
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


def test_to_binary(session):
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
    res = TestData.all_nulls(session).toDF("a").select(to_binary(col("a"))).collect()
    assert res == [Row(None), Row(None), Row(None), Row(None)]


def test_coalesce(session):
    # Taken from FunctionSuite.scala
    Utils.check_answer(
        TestData.null_data2(session).select(coalesce("A", "B", "C")),
        [Row(1), Row(2), Row(3), Row(None), Row(1), Row(1), Row(1)],
        sort=False,
    )

    # single input column
    with pytest.raises(ProgrammingError) as ex_info:
        TestData.null_data2(session).select(coalesce(col("A"))).collect()
    assert "not enough arguments for function [COALESCE" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.null_data2(session).select(coalesce(["A", "B", "C"]))
    assert "COALESCE expected Column or str, got: <class 'list'>" in str(ex_info)


def test_negate_and_not_negative(session):
    with pytest.raises(TypeError) as ex_info:
        TestData.null_data2(session).select(negate(["A", "B", "C"]))
    assert "NEGATE expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.null_data2(session).select(not_(["A", "B", "C"]))
    assert "NOT_ expected Column or str, got: <class 'list'>" in str(ex_info)


def test_random_negative(session):
    df = session.sql("select 1")
    with pytest.raises(ProgrammingError) as ex_info:
        res = df.select(random("abc")).collect()
    assert "invalid identifier 'ABC'" in str(ex_info)


def test_parse_functions_negative(session):
    df = session.sql("select 1").toDF("a")

    # parse_json
    with pytest.raises(TypeError) as ex_info:
        df.select(parse_json([1])).collect()
    assert "PARSE_JSON expected Column or str, got: <class 'list'>" in str(ex_info)

    # parse_xml
    with pytest.raises(TypeError) as ex_info:
        df.select(parse_xml([1])).collect()
    assert "PARSE_XML expected Column or str, got: <class 'list'>" in str(ex_info)


def test_to_filetype_negative(session):
    df = session.sql("select 1").toDF("a")
    # to_json
    with pytest.raises(TypeError) as ex_info:
        df.select(to_json([1])).collect()
    assert "TO_JSON expected Column or str, got: <class 'list'>" in str(ex_info)

    # to_xml
    with pytest.raises(TypeError) as ex_info:
        df.select(to_xml([1])).collect()
    assert "TO_XML expected Column or str, got: <class 'list'>" in str(ex_info)


def test_array_agg_negative(session):
    df = session.sql("select 1").toDF("a")
    with pytest.raises(TypeError) as ex_info:
        df.select(array_agg([1])).collect()
    assert "ARRAY_AGG expected Column or str, got: <class 'list'>" in str(ex_info)
