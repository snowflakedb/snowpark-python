#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import Row
from snowflake.snowpark.functions import avg, stddev
from snowflake.snowpark.ml.transformers.standard_scaler import StandardScaler

TABLENAME = "table_temp"


def test_fit(session):
    input_df = session.createDataFrame([-1.0, 2.0, 3.5, 4.0]).toDF("input_value")
    expected_mean = input_df.select(avg(input_df["input_value"])).collect()[0][0]
    expected_stddev = input_df.select(stddev(input_df["input_value"])).collect()[0][0]

    scaler = StandardScaler(session=session, input_col="input_value")
    scaler.fit(input_df)

    actual_mean = float(session.table(TABLENAME).collect()[0][0])
    actual_stddev = float(session.table(TABLENAME).collect()[0][1])

    assert "{:.6f}".format(actual_mean) == "{:.6f}".format(expected_mean)
    assert "{:.6f}".format(actual_stddev) == "{:.6f}".format(expected_stddev)


def test_transform(session):
    input_df = session.createDataFrame([-1.0, 2.0, 3.5, 4.0]).toDF("input_value")
    expected_mean = input_df.select(avg(input_df["input_value"])).collect()[0][0]
    expected_stddev = input_df.select(stddev(input_df["input_value"])).collect()[0][0]

    scaler = StandardScaler(session=session, input_col="input_value")
    scaler.fit(input_df)

    values = [-0.6, 5.0, 10.2]
    df = session.createDataFrame(values).toDF("value")

    expected_df = session.createDataFrame(
        [Row((v - expected_mean) / expected_stddev) for v in values]
    ).toDF("expected")
    actual_df = df.select(scaler.transform(df["value"]))

    for value1, value2 in zip(actual_df.collect(), expected_df.collect()):
        assert "{:.6f}".format(value1[0]) == "{:.6f}".format(value2[0])
