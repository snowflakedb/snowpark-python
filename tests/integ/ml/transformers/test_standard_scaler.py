#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark.functions import avg, stddev
from snowflake.snowpark.ml.transformers.feature import StandardScaler

TABLENAME = "table_temp"


def test_fit(session):
    input_df = session.createDataFrame(
        [
            Row(1, "a", -1.0),
            Row(2, "a", 8.3),
            Row(3, "b", 2.0),
            Row(4, "c", 3.5),
            Row(5, "d", 2.5),
            Row(6, "b", 4.0),
        ]
    ).toDF("id", "str", "input_value")
    expected_mean = input_df.select(avg(input_df["input_value"])).collect()[0][0]
    expected_stddev = input_df.select(stddev(input_df["input_value"])).collect()[0][0]

    scaler = StandardScaler(session=session, input_col="input_value")
    scaler.fit(input_df)

    actual_mean = float(session.table(TABLENAME).collect()[0][0])
    actual_stddev = float(session.table(TABLENAME).collect()[0][1])

    assert f"{actual_mean:.4f}" == f"{expected_mean:.4f}"
    assert f"{actual_stddev:.4f}" == f"{expected_stddev:.4f}"

    # input_df does not contain input_col
    missing_input_col_input_df = session.createDataFrame(
        [
            Row(1, "a", -1.0),
            Row(2, "a", 8.3),
            Row(3, "b", 2.0),
            Row(4, "c", 3.5),
            Row(5, "d", 2.5),
            Row(6, "b", 4.0),
        ]
    ).toDF("id", "str", "float")

    with pytest.raises(ProgrammingError) as ex_info:
        scaler.fit(missing_input_col_input_df)
    assert "invalid identifier 'INPUT_VALUE'" in str(ex_info)

    # standard scaler with session as None
    none_session_scaler = StandardScaler(session=None, input_col="input_value")

    with pytest.raises(AttributeError) as ex_info:
        none_session_scaler.fit(input_df)
    assert "'NoneType' object has no attribute 'sql'" in str(ex_info)

    # standard scaler with input_col as None
    none_input_col_scaler = StandardScaler(session=session, input_col=None)

    with pytest.raises(TypeError) as ex_info:
        none_input_col_scaler.fit(input_df)
    assert "The select() input must be Column, str, or list" in str(ex_info)

    # input_df type is not DataFrame
    with pytest.raises(TypeError) as ex_info:
        scaler.fit(None)
    assert (
        "StandardScaler.fit() input type must be DataFrame. Got: <class 'NoneType'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        scaler.fit("df")
    assert (
        "StandardScaler.fit() input type must be DataFrame. Got: <class 'str'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        scaler.fit(0)
    assert (
        "StandardScaler.fit() input type must be DataFrame. Got: <class 'int'>"
        in str(ex_info)
    )


def test_transform(session):
    input_df = session.createDataFrame(
        [
            Row(1, "a", -1.0),
            Row(2, "b", 2.0),
            Row(3, "c", 3.5),
            Row(4, "b", 4.0),
            Row(5, "d", 2.5),
            Row(6, "a", 8.3),
        ]
    ).toDF("id", "str", "input_value")
    expected_mean = input_df.select(avg(input_df["input_value"])).collect()[0][0]
    expected_stddev = input_df.select(stddev(input_df["input_value"])).collect()[0][0]

    scaler = StandardScaler(session=session, input_col="input_value")
    scaler.fit(input_df)

    values = [5.0, 10.2, 0.1, -0.6]
    df = session.createDataFrame(values).toDF("value")

    expected_df = session.createDataFrame(
        [Row((v - expected_mean) / expected_stddev) for v in values]
    ).toDF("expected")
    actual_df = df.select(scaler.transform(df["value"]))
    expected_rows = expected_df.collect()
    actual_rows = actual_df.collect()

    for value1, value2 in zip(actual_rows, expected_rows):
        assert f"{value1[0]:.4f}" == f"{value2[0]:.4f}"

    # col type is not Column
    with pytest.raises(TypeError) as ex_info:
        df.select(scaler.transform(None))
    assert (
        "StandardScaler.transform() input type must be Column. Got: <class 'NoneType'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(scaler.transform("value"))
    assert (
        "StandardScaler.transform() input type must be Column. Got: <class 'str'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(scaler.transform(0))
    assert (
        "StandardScaler.transform() input type must be Column. Got: <class 'int'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(scaler.transform(df))
    assert (
        "StandardScaler.transform() input type must be Column. Got: <class 'snowflake.snowpark.dataframe.DataFrame'>"
        in str(ex_info)
    )
