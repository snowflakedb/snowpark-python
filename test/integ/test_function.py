#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from test.utils import TestData, Utils

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark.functions import (
    abs,
    array_agg,
    array_append,
    array_cat,
    array_compact,
    array_construct,
    array_construct_compact,
    array_contains,
    array_insert,
    array_intersection,
    array_position,
    array_prepend,
    array_size,
    array_slice,
    array_to_string,
    arrays_overlap,
    as_array,
    as_binary,
    as_char,
    as_date,
    as_decimal,
    as_double,
    as_integer,
    as_number,
    as_object,
    as_real,
    as_time,
    as_timestamp_ltz,
    as_timestamp_ntz,
    as_timestamp_tz,
    as_varchar,
    builtin,
    call_builtin,
    ceil,
    char,
    check_json,
    check_xml,
    coalesce,
    col,
    contains,
    count_distinct,
    exp,
    floor,
    is_array,
    is_binary,
    is_char,
    is_date,
    is_decimal,
    is_double,
    is_integer,
    is_null_value,
    is_object,
    is_real,
    is_time,
    is_timestamp_ltz,
    is_timestamp_ntz,
    is_timestamp_tz,
    is_varchar,
    json_extract_path_text,
    lit,
    log,
    negate,
    not_,
    object_construct,
    object_construct_keep_null,
    object_delete,
    object_insert,
    object_pick,
    objectagg,
    parse_json,
    parse_xml,
    pow,
    random,
    split,
    sqrt,
    startswith,
    strip_null_value,
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
    assert "'SQRT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        df.select(sqrt(lit(-1))).collect()
    assert "Invalid floating point operation: sqrt(-1)" in str(ex_info)

    # abs
    with pytest.raises(TypeError) as ex_info:
        df.select(abs([None])).collect()
    assert "'ABS' expected Column or str, got: <class 'list'>" in str(ex_info)

    # exp
    with pytest.raises(TypeError) as ex_info:
        df.select(exp([None])).collect()
    assert "'EXP' expected Column or str, got: <class 'list'>" in str(ex_info)

    # log
    with pytest.raises(TypeError) as ex_info:
        df.select(log([None], "a")).collect()
    assert "'LOG' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(log("a", [123])).collect()
    assert "'LOG' expected Column or str, got: <class 'list'>" in str(ex_info)

    # pow
    with pytest.raises(TypeError) as ex_info:
        df.select(pow([None], "a")).collect()
    assert "'POW' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(pow("a", [123])).collect()
    assert "'POW' expected Column or str, got: <class 'list'>" in str(ex_info)

    # floor
    with pytest.raises(TypeError) as ex_info:
        df.select(floor([None])).collect()
    assert "'FLOOR' expected Column or str, got: <class 'list'>" in str(ex_info)

    # ceil
    with pytest.raises(TypeError) as ex_info:
        df.select(ceil([None])).collect()
    assert "'CEIL' expected Column or str, got: <class 'list'>" in str(ex_info)


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
    assert "'SPLIT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(split("a", [1, 2, 3])).collect()
    assert "'SPLIT' expected Column or str, got: <class 'list'>" in str(ex_info)

    # upper
    with pytest.raises(TypeError) as ex_info:
        df.select(upper([1])).collect()
    assert "'UPPER' expected Column or str, got: <class 'list'>" in str(ex_info)

    # contains
    with pytest.raises(TypeError) as ex_info:
        df.select(contains("a", [1])).collect()
    assert "'CONTAINS' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(contains([1], "b")).collect()
    assert "'CONTAINS' expected Column or str, got: <class 'list'>" in str(ex_info)

    # startswith
    with pytest.raises(TypeError) as ex_info:
        df.select(startswith("a", [1])).collect()
    assert "'STARTSWITH' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(startswith([1], "b")).collect()
    assert "'STARTSWITH' expected Column or str, got: <class 'list'>" in str(ex_info)

    # char
    with pytest.raises(TypeError) as ex_info:
        df.select(char([1])).collect()
    assert "'CHAR' expected Column or str, got: <class 'list'>" in str(ex_info)

    # translate
    with pytest.raises(TypeError) as ex_info:
        df.select(translate("a", "b", [1])).collect()
    assert "'TRANSLATE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(translate("a", [1], "c")).collect()
    assert "'TRANSLATE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(translate([1], "a", "c")).collect()
    assert "'TRANSLATE' expected Column or str, got: <class 'list'>" in str(ex_info)

    # trim
    with pytest.raises(TypeError) as ex_info:
        df.select(trim("a", [1])).collect()
    assert "'TRIM' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(trim([1], "b")).collect()
    assert "'TRIM' expected Column or str, got: <class 'list'>" in str(ex_info)


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
    assert "'COUNT_DISTINCT' expected Column or str, got: <class 'int'>" in str(ex_info)

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


def test_is_negative(session):
    td = TestData.string1(session)

    # Test negative input types for __to_col_if_str
    with pytest.raises(TypeError) as ex_info:
        td.select(is_array(["a"])).collect()
    assert "'IS_ARRAY' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_binary(["a"])).collect()
    assert "'IS_BINARY' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_char(["a"])).collect()
    assert "'IS_CHAR' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_varchar(["a"])).collect()
    assert "'IS_VARCHAR' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_date(["a"])).collect()
    assert "'IS_DATE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_decimal(["a"])).collect()
    assert "'IS_DECIMAL' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_double(["a"])).collect()
    assert "'IS_DOUBLE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_real(["a"])).collect()
    assert "'IS_REAL' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_integer(["a"])).collect()
    assert "'IS_INTEGER' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_null_value(["a"])).collect()
    assert "'IS_NULL_VALUE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_object(["a"])).collect()
    assert "'IS_OBJECT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_time(["a"])).collect()
    assert "'IS_TIME' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(is_timestamp_ltz(["a"])).collect()
    assert "'IS_TIMESTAMP_LTZ' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_timestamp_ntz(["a"])).collect()
    assert "'IS_TIMESTAMP_NTZ' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(is_timestamp_tz(["a"])).collect()
    assert "'IS_TIMESTAMP_TZ' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    # Test that we can only use these with variants
    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_array("a")).collect()
    assert "Invalid argument types for function 'IS_ARRAY'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_binary("a")).collect()
    assert "Invalid argument types for function 'IS_BINARY'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_char("a")).collect()
    assert "Invalid argument types for function 'IS_CHAR'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_varchar("a")).collect()
    assert "Invalid argument types for function 'IS_VARCHAR'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_date("a")).collect()
    assert "Invalid argument types for function 'IS_DATE'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_decimal("a")).collect()
    assert "Invalid argument types for function 'IS_DECIMAL'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_double("a")).collect()
    assert "Invalid argument types for function 'IS_DOUBLE'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_real("a")).collect()
    assert "Invalid argument types for function 'IS_REAL'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_integer("a")).collect()
    assert "Invalid argument types for function 'IS_INTEGER'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_null_value("a")).collect()
    assert "Invalid argument types for function 'IS_NULL_VALUE'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_object("a")).collect()
    assert "Invalid argument types for function 'IS_OBJECT'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_time("a")).collect()
    assert "Invalid argument types for function 'IS_TIME'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_timestamp_ltz("a")).collect()
    assert "Invalid argument types for function 'IS_TIMESTAMP_LTZ'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_timestamp_ntz("a")).collect()
    assert "Invalid argument types for function 'IS_TIMESTAMP_NTZ'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(is_timestamp_tz("a")).collect()
    assert "Invalid argument types for function 'IS_TIMESTAMP_TZ'" in str(ex_info)


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


def test_as_negative(session):
    td = TestData.string1(session)

    # Test negative input types for __to_col_if_str
    with pytest.raises(TypeError) as ex_info:
        td.select(as_array(["a"])).collect()
    assert "'AS_ARRAY' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_binary(["a"])).collect()
    assert "'AS_BINARY' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_char(["a"])).collect()
    assert "'AS_CHAR' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_varchar(["a"])).collect()
    assert "'AS_VARCHAR' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_date(["a"])).collect()
    assert "'AS_DATE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_decimal(["a"])).collect()
    assert "'AS_DECIMAL' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_number(["a"])).collect()
    assert "'AS_NUMBER' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_double(["a"])).collect()
    assert "'AS_DOUBLE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_real(["a"])).collect()
    assert "'AS_REAL' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_integer(["a"])).collect()
    assert "'AS_INTEGER' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_object(["a"])).collect()
    assert "'AS_OBJECT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_time(["a"])).collect()
    assert "'AS_TIME' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        td.select(as_timestamp_ltz(["a"])).collect()
    assert "'AS_TIMESTAMP_LTZ' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_timestamp_ntz(["a"])).collect()
    assert "'AS_TIMESTAMP_NTZ' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        td.select(as_timestamp_tz(["a"])).collect()
    assert "'AS_TIMESTAMP_TZ' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    # Test that we can only use these with variants
    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_array("a")).collect()
    assert "Invalid argument types for function 'AS_ARRAY'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_binary("a")).collect()
    assert "Invalid argument types for function 'AS_BINARY'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_char("a")).collect()
    assert "Invalid argument types for function 'AS_CHAR'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_varchar("a")).collect()
    assert "Invalid argument types for function 'AS_VARCHAR'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_date("a")).collect()
    assert "Invalid argument types for function 'AS_DATE'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_decimal("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_DECIMAL(variantValue...)'"
        in str(ex_info)
    )

    with pytest.raises(ValueError) as ex_info:
        td.select(as_decimal("a", None, 3)).collect()
    assert "Cannot define scale without precision" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.variant1(session).select(as_decimal(col("decimal1"), -1)).collect()
    assert "invalid value [-1] for parameter 'AS_DECIMAL(?, precision...)'" in str(
        ex_info
    )

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.variant1(session).select(as_decimal(col("decimal1"), 6, -1)).collect()
    assert "invalid value [-1] for parameter 'AS_DECIMAL(?, ?, scale)'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_number("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_NUMBER(variantValue...)'"
        in str(ex_info)
    )

    with pytest.raises(ValueError) as ex_info:
        td.select(as_number("a", None, 3)).collect()
    assert "Cannot define scale without precision" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.variant1(session).select(as_number(col("decimal1"), -1)).collect()
    assert "invalid value [-1] for parameter 'AS_NUMBER(?, precision...)'" in str(
        ex_info
    )

    with pytest.raises(ProgrammingError) as ex_info:
        TestData.variant1(session).select(as_number(col("decimal1"), 6, -1)).collect()
    assert "invalid value [-1] for parameter 'AS_NUMBER(?, ?, scale)'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_double("a")).collect()
    assert "Invalid argument types for function 'AS_DOUBLE'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_real("a")).collect()
    assert "Invalid argument types for function 'AS_REAL'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_integer("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_INTEGER(variantValue...)'"
        in str(ex_info)
    )

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_object("a")).collect()
    assert "Invalid argument types for function 'AS_OBJECT'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_time("a")).collect()
    assert "Invalid argument types for function 'AS_TIME'" in str(ex_info)

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_timestamp_ltz("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_TIMESTAMP_LTZ(variantValue...)'"
        in str(ex_info)
    )

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_timestamp_ntz("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_TIMESTAMP_NTZ(variantValue...)'"
        in str(ex_info)
    )

    with pytest.raises(ProgrammingError) as ex_info:
        td.select(as_timestamp_tz("a")).collect()
    assert (
        "invalid type [VARCHAR(5)] for parameter 'AS_TIMESTAMP_TZ(variantValue...)'"
        in str(ex_info)
    )


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
    assert "'COALESCE' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_negate_and_not_negative(session):
    with pytest.raises(TypeError) as ex_info:
        TestData.null_data2(session).select(negate(["A", "B", "C"]))
    assert "'NEGATE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.null_data2(session).select(not_(["A", "B", "C"]))
    assert "'NOT_' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_random_negative(session):
    df = session.sql("select 1")
    with pytest.raises(ProgrammingError) as ex_info:
        res = df.select(random("abc")).collect()
    assert "invalid identifier 'ABC'" in str(ex_info)


def test_check_functions_negative(session):
    df = session.sql("select 1").toDF("a")

    # check_json
    with pytest.raises(TypeError) as ex_info:
        df.select(check_json([1])).collect()
    assert "'CHECK_JSON' expected Column or str, got: <class 'list'>" in str(ex_info)

    # check_xml
    with pytest.raises(TypeError) as ex_info:
        df.select(check_xml([1])).collect()
    assert "'CHECK_XML' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_parse_functions_negative(session):
    df = session.sql("select 1").toDF("a")

    # parse_json
    with pytest.raises(TypeError) as ex_info:
        df.select(parse_json([1])).collect()
    assert "'PARSE_JSON' expected Column or str, got: <class 'list'>" in str(ex_info)

    # parse_xml
    with pytest.raises(TypeError) as ex_info:
        df.select(parse_xml([1])).collect()
    assert "'PARSE_XML' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_json_functions_negative(session):
    df = session.sql("select 1").toDF("a")

    # json_extract_path_text
    with pytest.raises(TypeError) as ex_info:
        df.select(json_extract_path_text([1], "a")).collect()
    assert (
        "'JSON_EXTRACT_PATH_TEXT' expected Column or str, got: <class 'list'>"
        in str(ex_info)
    )

    # strip_null_value
    with pytest.raises(TypeError) as ex_info:
        df.select(strip_null_value([1])).collect()
    assert "'STRIP_NULL_VALUE' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )


def test_to_filetype_negative(session):
    df = session.sql("select 1").toDF("a")
    # to_json
    with pytest.raises(TypeError) as ex_info:
        df.select(to_json([1])).collect()
    assert "'TO_JSON' expected Column or str, got: <class 'list'>" in str(ex_info)

    # to_xml
    with pytest.raises(TypeError) as ex_info:
        df.select(to_xml([1])).collect()
    assert "'TO_XML' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_array_negative(session):
    df = session.sql("select 1").toDF("a")

    with pytest.raises(TypeError) as ex_info:
        df.select(array_agg([1])).collect()
    assert "'ARRAY_AGG' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_append([1], "column")).collect()
    assert "'ARRAY_APPEND' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_cat([1], "column")).collect()
    assert "'ARRAY_CAT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_compact([1])).collect()
    assert "'ARRAY_COMPACT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_construct([1])).collect()
    assert "'ARRAY_CONSTRUCT' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_construct_compact([1])).collect()
    assert (
        "'ARRAY_CONSTRUCT_COMPACT' expected Column or str, got: <class 'list'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_contains([1], "column")).collect()
    assert "'ARRAY_CONTAINS' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_insert([1], lit(3), "column")).collect()
    assert "'ARRAY_INSERT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_position([1], "column")).collect()
    assert "'ARRAY_POSITION' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_prepend([1], "column")).collect()
    assert "'ARRAY_PREPEND' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_size([1])).collect()
    assert "'ARRAY_SIZE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_slice([1], "col1", "col2")).collect()
    assert "'ARRAY_SLICE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(array_to_string([1], "column")).collect()
    assert "'ARRAY_TO_STRING' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(arrays_overlap([1], "column")).collect()
    assert "'ARRAYS_OVERLAP' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(array_intersection([1], "column")).collect()
    assert "'ARRAY_INTERSECTION' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )


def test_object_negative(session):
    df = session.sql("select 1").toDF("a")

    with pytest.raises(TypeError) as ex_info:
        df.select(objectagg([1], "column")).collect()
    assert "'OBJECTAGG' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(object_construct([1])).collect()
    assert "'OBJECT_CONSTRUCT' expected Column or str, got: <class 'list'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(object_construct_keep_null([1], "column")).collect()
    assert (
        "'OBJECT_CONSTRUCT_KEEP_NULL' expected Column or str, got: <class 'list'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(object_delete([1], "column", "col1", "col2")).collect()
    assert "'OBJECT_DELETE' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(object_insert([1], "key", "key1")).collect()
    assert "'OBJECT_INSERT' expected Column or str, got: <class 'list'>" in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        df.select(object_pick([1], "key", "key1")).collect()
    assert "'OBJECT_PICK' expected Column or str, got: <class 'list'>" in str(ex_info)
