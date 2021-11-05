#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from test.utils import Utils

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkColumnException
from snowflake.snowpark.functions import avg, builtin
from snowflake.snowpark.ml.transformers.Imputer import Imputer

TABLENAME = "table_temp"


def test_fit_numerical(session):
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

    imputer = Imputer(session=session, input_col="input_value")
    imputer.fit(input_df, True)

    expected_df = session.createDataFrame(
        [
            Row(expected_mean, "null"),
        ]
    ).toDF("mean", "mode")
    actual_df = session.table(TABLENAME)

    Utils.check_answer(expected_df, actual_df)

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
        imputer.fit(missing_input_col_input_df, True)
    assert "invalid identifier 'INPUT_VALUE'" in str(ex_info)

    # imputer with session as None
    none_session_imputer = Imputer(session=None, input_col="input_value")

    with pytest.raises(AttributeError) as ex_info:
        none_session_imputer.fit(input_df, True)
    assert "'NoneType' object has no attribute 'sql'" in str(ex_info)

    # imputer with input_col as None
    none_input_col_imputer = Imputer(session=session, input_col=None)

    with pytest.raises(TypeError) as ex_info:
        none_input_col_imputer.fit(input_df, True)
    assert "The select() input must be Column, str, or list" in str(ex_info)

    # input_df type is not DataFrame
    with pytest.raises(TypeError) as ex_info:
        imputer.fit(None, True)
    assert (
        "Imputer.fit() input input_df type must be DataFrame. Got: <class 'NoneType'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        imputer.fit("df", True)
    assert (
        "Imputer.fit() input input_df type must be DataFrame. Got: <class 'str'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        imputer.fit(0, True)
    assert (
        "Imputer.fit() input input_df type must be DataFrame. Got: <class 'int'>"
        in str(ex_info)
    )

    # is_numerical type is not bool
    with pytest.raises(TypeError) as ex_info:
        imputer.fit(input_df, None)
    assert (
        "Imputer.fit() input is_numerical type must be bool. Got: <class 'NoneType'>"
        in str(ex_info)
    )

    # input_col is not numerical
    non_numerical_input_col_df = session.createDataFrame(
        [
            Row(1, "a", -1.0),
            Row(2, "a", 8.3),
            Row(3, "b", 2.0),
            Row(4, "c", 3.5),
            Row(5, "d", 2.5),
            Row(6, "b", 4.0),
        ]
    ).toDF("id", "input_value", "float")

    with pytest.raises(ProgrammingError) as ex_info:
        imputer.fit(non_numerical_input_col_df, True)
    assert "Numeric value 'a' is not recognized" in str(ex_info)


def test_fit_categorical(session):
    input_df = session.createDataFrame(
        [
            Row(1, "a", -1.0),
            Row(2, "a", 8.3),
            Row(3, "b", 2.0),
            Row(4, "c", 3.5),
            Row(5, "d", 2.5),
            Row(6, "b", 4.0),
        ]
    ).toDF("id", "input_value", "float")
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

    # col type is not Column
    with pytest.raises(TypeError) as ex_info:
        df.select(imputer.transform(None))
    assert (
        "Imputer.transform() input type must be Column. Got: <class 'NoneType'>"
        in str(ex_info)
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(imputer.transform("value"))
    assert "Imputer.transform() input type must be Column. Got: <class 'str'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(imputer.transform(0))
    assert "Imputer.transform() input type must be Column. Got: <class 'int'>" in str(
        ex_info
    )

    with pytest.raises(TypeError) as ex_info:
        df.select(imputer.transform(df))
    assert (
        "Imputer.transform() input type must be Column. Got: <class 'snowflake.snowpark.dataframe.DataFrame'>"
        in str(ex_info)
    )

    # df does not contain col
    with pytest.raises(SnowparkColumnException) as ex_info:
        df.select(imputer.transform(df["missing"]))
    assert "'The DataFrame does not contain the column named missing." in str(ex_info)


def test_transform_categorical(session):
    input_df = session.createDataFrame(
        [
            Row(1, "a", -1.0),
            Row(2, "a", 8.3),
            Row(3, "b", 2.0),
            Row(4, "c", 3.5),
            Row(5, "d", 2.5),
            Row(6, "b", 4.0),
        ]
    ).toDF("id", "input_value", "float")
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
