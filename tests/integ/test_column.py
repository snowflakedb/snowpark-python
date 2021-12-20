#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import datetime

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkColumnException
from snowflake.snowpark.functions import call_builtin, col, lit, when
from snowflake.snowpark.types import MapType
from tests.utils import TestData, Utils


def test_column_constructors_subscriptable(session):
    df = session.createDataFrame([[1, 2, 3]]).toDF("col", '"col"', "col .")
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


def test_between(session):
    df = session.createDataFrame([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]).toDF(
        ["a", "b"]
    )

    # between in where
    Utils.check_answer(df.where(col("a").between(lit(2), 6)), [Row(3, 4), Row(5, 6)])

    # between in select
    Utils.check_answer(
        df.select(col("a").between(lit(2), 6)),
        [Row(False), Row(True), Row(True), Row(False), Row(False)],
    )


def test_try_cast(session):
    df = session.createDataFrame([["2018-01-01"]], schema=["a"])
    cast_res = df.select(df["a"].cast("date")).collect()
    try_cast_res = df.select(df["a"].try_cast("date")).collect()
    assert cast_res[0][0] == try_cast_res[0][0] == datetime.date(2018, 1, 1)


def test_try_cast_work_cast_not_work(session):
    df = session.createDataFrame([["aaa"]], schema=["a"])
    with pytest.raises(ProgrammingError) as execinfo:
        df.select(df["a"].cast("date")).collect()
    assert "Date 'aaa' is not recognized" in str(execinfo)
    Utils.check_answer(
        df.select(df["a"].try_cast("date")), [Row(None)]
    )  # try_cast doesn't throw exception


def test_cast_try_cast_negative(session):
    df = session.createDataFrame([["aaa"]], schema=["a"])
    with pytest.raises(ValueError) as execinfo:
        df.select(df["a"].cast("wrong_type"))
    assert "'wrong_type' is not a supported type" in str(execinfo)
    with pytest.raises(ValueError) as execinfo:
        df.select(df["a"].try_cast("wrong_type"))
    assert "'wrong_type' is not a supported type" in str(execinfo)


def test_cast_decimal(session):
    df = session.createDataFrame([[5.2354]], schema=["a"])
    Utils.check_answer(df.select(df["a"].cast(" decimal ( 3, 2 ) ")), [Row(5.24)])


def test_startswith(session):
    Utils.check_answer(
        TestData.string4(session).select(col("a").startswith(lit("a"))),
        [Row(True), Row(False), Row(False)],
        sort=False,
    )


def test_substring(session):
    Utils.check_answer(
        TestData.string4(session).select(col("a").substring(1, 3)),
        [Row("app"), Row("ban"), Row("pea")],
        sort=False,
    )


@pytest.mark.skip("The returned result is a string instead of a dict")
def test_cast_map_type(session):
    df = session.createDataFrame([['{"key": "1"}']], schema=["a"])
    result = df.select(
        call_builtin("parse_json", df["a"]).cast(MapType("varchar", "varchar"))
    ).collect()
    # TODO: The generated sql CAST (parse_json("A") AS OBJECT). Does it make sense to cast to an object/MapType?
    assert result[0][0] == {"key": "1"}


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
