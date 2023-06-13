#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import asc, asc_nulls_first, asc_nulls_last, sequence
from tests.utils import TestData, Utils


def test_order(session):
    null_data1 = TestData.null_data1(session)
    assert null_data1.sort(asc(null_data1["A"])).collect() == [
        Row(None),
        Row(None),
        Row(1),
        Row(2),
        Row(3),
    ]
    assert null_data1.sort(asc_nulls_first(null_data1["A"])).collect() == [
        Row(None),
        Row(None),
        Row(1),
        Row(2),
        Row(3),
    ]
    assert null_data1.sort(asc_nulls_last(null_data1["A"])).collect() == [
        Row(1),
        Row(2),
        Row(3),
        Row(None),
        Row(None),
    ]


def test_sequence_negative(session):
    df = session.sql("select 1").to_df("a")

    with pytest.raises(TypeError) as ex_info:
        df.select(sequence([1], 1)).collect()
    assert "'SEQUENCE' expected Column or str, got: <class 'list'>" in str(ex_info)


def test_sequence(session):
    df = session.createDataFrame([(-2, 2)], ["C1", "C2"])
    Utils.check_answer(
        df.select(sequence("C1", "C2").alias("r")),
        [Row(R="[\n  -2,\n  -1,\n  0,\n  1,\n  2\n]")],
        sort=False,
    )

    df = session.createDataFrame([(4, -4, -2)], ["C1", "C2", "C3"])
    Utils.check_answer(
        df.select(sequence("C1", "C2", "C3").alias("r")),
        [Row(R="[\n  4,\n  2,\n  0,\n  -2,\n  -4\n]")],
        sort=False,
    )
