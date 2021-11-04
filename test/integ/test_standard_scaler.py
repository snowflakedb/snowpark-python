#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import pytest

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

    assert "{:.4f}".format(actual_mean) == "{:.4f}".format(expected_mean)
    assert "{:.4f}".format(actual_stddev) == "{:.4f}".format(expected_stddev)

    # compares with incorrect mean and stddev
    incorrect_mean = expected_mean + 1
    incorrect_stddev = expected_stddev + 1

    with pytest.raises(AssertionError) as ex_info:
        assert "{:.4f}".format(actual_mean) == "{:.4f}".format(incorrect_mean)
    assert (
        f"assert '{'{:.4f}'.format(actual_mean)}' == '{'{:.4f}'.format(incorrect_mean)}'"
        in str(ex_info)
    )

    with pytest.raises(AssertionError) as ex_info:
        assert "{:.4f}".format(actual_stddev) == "{:.4f}".format(incorrect_stddev)
    assert (
        f"assert '{'{:.4f}'.format(actual_stddev)}' == '{'{:.4f}'.format(incorrect_stddev)}'"
        in str(ex_info)
    )


def test_transform(session):
    input_df = session.createDataFrame([-1.0, 2.0, 3.5, 4.0]).toDF("input_value")
    expected_mean = input_df.select(avg(input_df["input_value"])).collect()[0][0]
    expected_stddev = input_df.select(stddev(input_df["input_value"])).collect()[0][0]

    scaler = StandardScaler(session=session, input_col="input_value")
    scaler.fit(input_df)

    values = [-0.6, 5.0, 10.2, 8.3, 0.1]
    df = session.createDataFrame(values).toDF("value")

    expected_df = session.createDataFrame(
        [Row((v - expected_mean) / expected_stddev) for v in values]
    ).toDF("expected")
    actual_df = df.select(scaler.transform(df["value"]))
    expected_rows = expected_df.collect()
    actual_rows = actual_df.collect()

    assert len(actual_rows) == len(expected_rows)
    for value1, value2 in zip(actual_rows, expected_rows):
        assert "{:.4f}".format(value1[0]) == "{:.4f}".format(value2[0])

    # compares with df with a missing row
    missing_value_df = session.createDataFrame(
        [Row((v - expected_mean) / expected_stddev) for v in values[:-1]]
    ).toDF("missing")
    missing_value_rows = missing_value_df.collect()

    with pytest.raises(AssertionError) as ex_info:
        assert len(actual_rows) == len(missing_value_rows)
    assert f"assert {len(actual_rows)} == {len(missing_value_rows)}" in str(ex_info)

    # compares with df with incorrect values
    incorrect_value_df = session.createDataFrame(
        [Row((v - expected_mean) / expected_stddev + 1) for v in values]
    ).toDF("incorrect")
    incorrect_value_rows = incorrect_value_df.collect()

    assert len(actual_rows) == len(incorrect_value_rows)
    with pytest.raises(AssertionError) as ex_info:
        assert "{:.4f}".format(actual_rows[0][0]) == "{:.4f}".format(
            incorrect_value_rows[0][0]
        )
    assert (
        f"assert '{'{:.4f}'.format(actual_rows[0][0])}' == '{'{:.4f}'.format(incorrect_value_rows[0][0])}'"
        in str(ex_info)
    )
