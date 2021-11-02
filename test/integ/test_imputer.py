#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from test.utils import Utils

from snowflake.snowpark import Row
from snowflake.snowpark.functions import avg, builtin
from snowflake.snowpark.ml.transformers.Imputer import Imputer

TABLENAME = "table_temp"


def test_fit_numerical(session):
    input_df = session.createDataFrame([2.0, 0.0, -3.2, 1.0, 2.0, 4.5]).toDF(
        "input_value"
    )
    expected_mean = input_df.select(avg(input_df["input_value"])).collect()[0][0]

    imputer = Imputer(session=session, input_col="input_value")
    imputer.fit(input_df, True)

    expected_df = session.createDataFrame(
        [
            Row(expected_mean, "null"),
        ]
    ).toDF("mean", "mode")
    actual_df = session.table(TABLENAME)

    Utils.check_answer(expected_df, actual_df)


def test_fit_categorical(session):
    input_df = session.createDataFrame(["a", "b", "a", "c", "d", "b", "a"]).toDF(
        "input_value"
    )
    expected_mode = input_df.select(builtin("mode")(input_df["input_value"])).collect()[
        0
    ][0]

    imputer = Imputer(session=session, input_col="input_value")
    imputer.fit(input_df, False)

    expected_df = session.createDataFrame(
        [
            Row(-1.0, expected_mode),
        ]
    ).toDF("mean", "mode")
    actual_df = session.table(TABLENAME)

    Utils.check_answer(expected_df, actual_df)


def test_transform_numerical(session):
    input_df = session.createDataFrame([2.0, 0.0, -3.2, 1.0, 2.0, 4.5]).toDF(
        "input_value"
    )
    expected_mean = input_df.select(avg(input_df["input_value"])).collect()[0][0]

    imputer = Imputer(session=session, input_col="input_value")
    imputer.fit(input_df, True)

    values = [-0.6, 5.0, None, 10.2, None]
    df = session.createDataFrame(values).toDF("value")

    expected_df = session.createDataFrame(
        [Row(expected_mean if v is None else v) for v in values]
    ).toDF("expected")
    actual_df = df.select(imputer.transform(df["value"]))

    for value1, value2 in zip(actual_df.collect(), expected_df.collect()):
        assert "{:.6f}".format(value1[0]) == "{:.6f}".format(value2[0])


def test_transform_categorical(session):
    input_df = session.createDataFrame(["a", "b", "a", "c", "d", "b", "a"]).toDF(
        "input_value"
    )
    expected_mode = input_df.select(builtin("mode")(input_df["input_value"])).collect()[
        0
    ][0]

    imputer = Imputer(session=session, input_col="input_value")
    imputer.fit(input_df, False)

    values = ["c", "a", None, "b", "a", None]
    df = session.createDataFrame(values).toDF("value")

    expected_df = session.createDataFrame(
        [Row(expected_mode if v is None else v) for v in values]
    ).toDF("expected")
    actual_df = df.select(imputer.transform(df["value"]))

    Utils.check_answer(expected_df, actual_df)
