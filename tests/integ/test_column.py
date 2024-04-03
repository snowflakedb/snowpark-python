#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import json

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkColumnException, SnowparkSQLException
from snowflake.snowpark.functions import col, lit, parse_json, when
from tests.utils import TestData, Utils


@pytest.mark.localtest
def test_column_constructors_subscriptable(session):
    df = session.create_dataframe([[1, 2, 3]]).to_df("col", '"col"', "col .")
    assert df.select(df["col"]).collect() == [Row(1)]
    assert df.select(df['"col"']).collect() == [Row(2)]
    assert df.select(df["col ."]).collect() == [Row(3)]
    assert df.select(df["COL"]).collect() == [Row(1)]
    assert df.select(df["CoL"]).collect() == [Row(1)]
    assert df.select(df['"COL"']).collect() == [Row(1)]

    with pytest.raises(SnowparkColumnException) as ex_info:
        df.select(df['"Col"']).collect()
    assert "The DataFrame does not contain the column" in str(ex_info)
    with pytest.raises(SnowparkColumnException) as ex_info:
        df.select(df["COL ."]).collect()
    assert "The DataFrame does not contain the column" in str(ex_info)


@pytest.mark.localtest
def test_between(session):
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]).to_df(
        ["a", "b"]
    )

    # between in where
    Utils.check_answer(df.where(col("a").between(lit(2), 6)), [Row(3, 4), Row(5, 6)])

    # between in select
    Utils.check_answer(
        df.select(col("a").between(lit(2), 6)),
        [Row(False), Row(True), Row(True), Row(False), Row(False)],
    )


@pytest.mark.localtest
def test_try_cast(session):
    df = session.create_dataframe([["2018-01-01"]], schema=["a"])
    cast_res = df.select(df["a"].cast("date")).collect()
    try_cast_res = df.select(df["a"].try_cast("date")).collect()
    assert cast_res[0][0] == try_cast_res[0][0] == datetime.date(2018, 1, 1)


@pytest.mark.localtest
def test_try_cast_work_cast_not_work(session, local_testing_mode):
    df = session.create_dataframe([["aaa"]], schema=["a"])
    with pytest.raises(
        ValueError if local_testing_mode else SnowparkSQLException
    ) as execinfo:
        df.select(df["a"].cast("date")).collect()
    if not local_testing_mode:
        assert "Date 'aaa' is not recognized" in str(execinfo)

    Utils.check_answer(
        df.select(df["a"].try_cast("date")), [Row(None)]
    )  # try_cast doesn't throw exception


@pytest.mark.localtest
def test_cast_try_cast_negative(session):
    df = session.create_dataframe([["aaa"]], schema=["a"])
    with pytest.raises(ValueError) as execinfo:
        df.select(df["a"].cast("wrong_type"))
    assert "'wrong_type' is not a supported type" in str(execinfo)
    with pytest.raises(ValueError) as execinfo:
        df.select(df["a"].try_cast("wrong_type"))
    assert "'wrong_type' is not a supported type" in str(execinfo)


@pytest.mark.parametrize("number_word", ["decimal", "number", "numeric"])
def test_cast_decimal(session, number_word):
    df = session.create_dataframe([[5.2354]], schema=["a"])
    Utils.check_answer(
        df.select(df["a"].cast(f" {number_word} ( 3, 2 ) ")), [Row(5.24)]
    )


@pytest.mark.localtest
def test_cast_map_type(session):
    df = session.create_dataframe([['{"key": "1"}']], schema=["a"])
    result = df.select(parse_json(df["a"]).cast("object")).collect()
    assert json.loads(result[0][0]) == {"key": "1"}


@pytest.mark.localtest
def test_cast_array_type(session):
    df = session.create_dataframe([["[1,2,3]"]], schema=["a"])
    result = df.select(parse_json(df["a"]).cast("array")).collect()
    assert json.loads(result[0][0]) == [1, 2, 3]


@pytest.mark.localtest
def test_startswith(session):
    Utils.check_answer(
        TestData.string4(session).select(col("a").startswith(lit("a"))),
        [Row(True), Row(False), Row(False)],
        sort=False,
    )


@pytest.mark.localtest
def test_endswith(session):
    Utils.check_answer(
        TestData.string4(session).select(col("a").endswith(lit("ana"))),
        [Row(False), Row(True), Row(False)],
        sort=False,
    )


@pytest.mark.localtest
def test_substring(session):
    Utils.check_answer(
        TestData.string4(session).select(
            col("a").substring(1, 3), col("a").substring(2, 100)
        ),
        [Row("app", "pple"), Row("ban", "anana"), Row("pea", "each")],
        sort=False,
    )


@pytest.mark.localtest
def test_contains(session):
    Utils.check_answer(
        TestData.string4(session).filter(col("a").contains(lit("e"))),
        [Row("apple"), Row("peach")],
        sort=False,
    )


@pytest.mark.localtest
def test_when_accept_literal_value(session):
    assert TestData.null_data1(session).select(
        when(col("a").is_null(), 5).when(col("a") == 1, 6).otherwise(7).as_("a")
    ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]
    assert TestData.null_data1(session).select(
        when(col("a").is_null(), 5).when(col("a") == 1, 6).else_(7).as_("a")
    ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]

    # empty otherwise
    assert TestData.null_data1(session).select(
        when(col("a").is_null(), 5).when(col("a") == 1, 6).as_("a")
    ).collect() == [Row(5), Row(None), Row(6), Row(None), Row(5)]


@pytest.mark.localtest
def test_logical_operator_raise_error(session):
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    with pytest.raises(TypeError) as execinfo:
        df.filter(df.a > 1 and df.b > 1)
    assert "Cannot convert a Column object into bool" in str(execinfo)


def test_when_accept_sql_expr(session):
    assert TestData.null_data1(session).select(
        when("a is NULL", 5).when("a = 1", 6).otherwise(7).as_("a")
    ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]
    assert TestData.null_data1(session).select(
        when("a is NULL", 5).when("a = 1", 6).else_(7).as_("a")
    ).collect() == [Row(5), Row(7), Row(6), Row(7), Row(5)]

    # empty otherwise
    assert TestData.null_data1(session).select(
        when("a is NULL", 5).when("a = 1", 6).as_("a")
    ).collect() == [Row(5), Row(None), Row(6), Row(None), Row(5)]


def test_column_with_builtins_that_shadow_functions(session):
    conversion_error_msg_text = "Cannot convert a Column object into bool"
    iter_error_msg_text = "Column is not iterable. This error can occur when you use the Python built-ins for sum, min and max. Please make sure you use the corresponding function from snowflake.snowpark.functions."
    round_error_msg_text = "Column cannot be rounded. This error can occur when you use the Python built-in round. Please make sure you use the snowflake.snowpark.functions.round function instead."

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(max(col("a"), 25)).collect()
    assert conversion_error_msg_text in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(max(col("a"))).collect()
    assert iter_error_msg_text in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(min(col("a"), 25)).collect()
    assert conversion_error_msg_text in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(min(col("a"))).collect()
    assert iter_error_msg_text in str(ex_info)

    # This works because we explicitly implement the __pow__ and __rpow__ methods
    assert TestData.double1(session).select(pow(col("a"), 2)).collect() == [
        Row(1.234321),
        Row(4.937284),
        Row(11.108889000000001),
    ]

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(round(col("a"))).collect()
    assert round_error_msg_text in str(ex_info)

    with pytest.raises(TypeError) as ex_info:
        TestData.double1(session).select(sum(col("a"))).collect()
    assert iter_error_msg_text in str(ex_info)
