#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.ml.transformers.Imputer import Imputer

TABLENAME = "hayu.imputer.table_temp"


def test_transform_numerical(session):
    mean = float(session.table(TABLENAME).collect()[0][0])

    df = session.createDataFrame([1.0, None]).toDF("value")
    expected_df = session.createDataFrame([1.0, mean]).toDF("expected").collect()
    imputer = Imputer()
    actual_df = df.select(imputer.transform(df.col("value"))).collect()

    for value1, value2 in zip(expected_df, actual_df):
        assert "{:.6f}".format(value1[0]) == "{:.6f}".format(value2[0])


def test_transform_categorical(session):
    mode = session.table(TABLENAME).collect()[0][1]

    df = session.createDataFrame(["a", None]).toDF("value")
    expected_df = session.createDataFrame(["a", mode]).toDF("expected").collect()
    imputer = Imputer()
    actual_df = df.select(imputer.transform(df.col("value"))).collect()

    assert expected_df == actual_df
