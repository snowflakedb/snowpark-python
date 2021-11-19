#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkColumnException
from snowflake.snowpark.functions import col, lit, when
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
