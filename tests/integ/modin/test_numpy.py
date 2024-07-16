#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from numpy.testing import assert_array_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    ("cond", "query_count"),
    [
        (lambda df: df["A"] >= 1, 1),
        (lambda df: True, 1),
        (lambda df: False, 1),
    ],
    ids=[
        "Conditional Column Operator",
        "True",
        "False",
    ],
)
def test_np_where(cond, query_count):
    data = {
        "A": [0, 1, 2, 0, 1, 2, 0, 1, 2],
        "B": [True, False, True, True, False, True, False, False, False],
        "C": ["a", "b", "c", "d", "a", "b", "c", "d", "e"],
    }
    snow_df = pd.DataFrame(data)
    pandas_df = native_pd.DataFrame(data)

    with SqlCounter(query_count=query_count if not isinstance(cond, bool) else 0):
        snow_result = np.where(cond(snow_df), snow_df["A"], snow_df["A"] - 1)
        pandas_result = np.where(cond(pandas_df), pandas_df["A"], pandas_df["A"] - 1)
        assert_array_equal(np.array(snow_result), np.array(pandas_result))


def test_logical_operators():
    data = {
        "A": [0, 1, 2, 0, 1, 2, 0, 1, 2],
        "B": [True, False, True, True, False, True, False, False, False],
        "C": ["a", "b", "c", "d", "a", "b", "c", "d", "e"],
    }
    snow_df = pd.DataFrame(data)
    pandas_df = native_pd.DataFrame(data)

    with SqlCounter(query_count=1):
        # Test simple logical not
        snow_result = np.logical_not(snow_df["B"])
        pandas_result = np.logical_not(pandas_df["B"])
        assert_array_equal(np.array(snow_result), np.array(pandas_result))

    with SqlCounter(query_count=1):
        # Test for chaining pandas and numpy calls
        snow_result = np.logical_not(snow_df["C"].isin(["a", "d"]))
        pandas_result = np.logical_not(pandas_df["C"].isin(["a", "d"]))
        assert_array_equal(np.array(snow_result), np.array(pandas_result))

    with SqlCounter(query_count=1):
        # Test binary logical operator
        snow_result = np.logical_and(snow_df["B"], snow_df["A"])
        pandas_result = np.logical_and(pandas_df["B"], pandas_df["A"])
        assert_array_equal(np.array(snow_result), np.array(pandas_result))


def test_np_ufunc_operators():
    data = {
        "A": [0, 1, 2, 0, 1, 2, 0, 1, 2],
        "B": [True, False, True, True, False, True, False, False, False],
        "C": ["a", "b", "c", "d", "a", "b", "c", "d", "e"],
    }
    snow_df = pd.DataFrame(data)
    pandas_df = native_pd.DataFrame(data)

    with SqlCounter(query_count=1):
        # Test numpy ufunc with scalar
        snow_result = np.add(snow_df["A"], 1)
        pandas_result = np.add(pandas_df["A"], 1)
        assert_array_equal(np.array(snow_result), np.array(pandas_result))

    with SqlCounter(query_count=1):
        # Test binary numpy ufunc
        snow_result = np.add(snow_df["A"], snow_df["A"])
        pandas_result = np.add(pandas_df["A"], pandas_df["A"])
        assert_array_equal(np.array(snow_result), np.array(pandas_result))

    with SqlCounter(query_count=1):
        # Test chained numpy ufuncs
        snow_result = np.add(snow_df["A"], np.add(snow_df["A"], 1))
        pandas_result = np.add(pandas_df["A"], np.add(pandas_df["A"], 1))
        assert_array_equal(np.array(snow_result), np.array(pandas_result))


def test_np_where_notimplemented():
    data = {
        "A": [0, 1, 2, 0, 1, 2, 0, 1, 2],
        "B": [True, False, True, True, False, True, False, False, False],
        "C": ["a", "b", "c", "d", "a", "b", "c", "d", "e"],
    }
    snow_df = pd.DataFrame(data)

    with SqlCounter(query_count=0):
        with pytest.raises(TypeError):
            np.where(
                np.array([True, False, True, True, False, True, False, False, False]),
                snow_df["A"],
                -1,
            )


@sql_count_checker(query_count=5, join_count=4)
def test_scalar():
    pdf_scalar = native_pd.DataFrame([[99, 99], [99, 99]])
    sdf_scalar = pd.DataFrame([[99, 99], [99, 99]])
    pdf_cond = native_pd.DataFrame([[True, False], [False, True]])
    sdf_cond = pd.DataFrame(pdf_cond)

    # pandas
    pdf_result = pdf_scalar.where(pdf_cond, -99)
    sdf_result = sdf_scalar.where(sdf_cond, -99)

    assert_array_equal(sdf_result, pdf_result)

    # numpy
    np_orig_result = np.where(pdf_cond, 99, -99)
    sp_result = np.where(sdf_cond, 99, -99)
    assert_array_equal(sp_result, np_orig_result)

    # numpy w/ zeros - SNOW-1372268
    np_orig_result = np.where(pdf_cond, 1, 0)
    sp_result = np.where(sdf_cond, 1, 0)
    assert_array_equal(sp_result, np_orig_result)

    # numpy w/ False - SNOW-1372268
    np_orig_result = np.where(pdf_cond, 1, False)
    sp_result = np.where(sdf_cond, 1, False)
    assert_array_equal(sp_result, np_orig_result)


@pytest.mark.parametrize(
    "cond",
    [0, 1, 2],
    ids=["cDF", "cDF2", "cScalar"],
)
@pytest.mark.parametrize(
    "x",
    [0, 1, 2],
    ids=["xDF", "xDF2", "xScalar"],
)
@pytest.mark.parametrize(
    "y",
    [0, 1, 2],
    ids=["yDF", "yDF2", "yScalar"],
)
def test_different_inputs(cond, x, y):
    input_df = native_pd.DataFrame([[1, 0], [0, 1]])
    input_df2 = native_pd.DataFrame([[99, 99], [99, 99]])
    native_inputs = [input_df, input_df2, -99]

    snow_inputs = [pd.DataFrame(input_df), pd.DataFrame(input_df2), -99]
    np_orig_result = np.where(
        native_inputs[cond] == 0, native_inputs[x], native_inputs[y]
    )

    with SqlCounter(no_check=True):
        sp_result = np.where(snow_inputs[cond] == 0, snow_inputs[x], snow_inputs[y])
        assert_array_equal(sp_result, np_orig_result)


@sql_count_checker(query_count=2, join_count=2)
def test_broadcast_scalar_x_df():
    input_df = native_pd.DataFrame([[False, True], [False, True]])
    input_df2 = native_pd.DataFrame([[1, 0], [0, 1]])
    snow_df = pd.DataFrame(input_df)
    snow_df2 = pd.DataFrame(input_df2)
    snow_result = np.where(snow_df, -99, snow_df2)
    np_result = np.where(input_df, -99, input_df2)
    assert_array_equal(snow_result, np_result)


@sql_count_checker(query_count=2, join_count=2)
def test_broadcast_scalar_x_ser():
    input_ser = native_pd.Series([False, True])
    input_ser2 = native_pd.Series([1, 0])
    snow_ser = pd.Series(input_ser)
    snow_ser2 = pd.Series(input_ser2)
    snow_result = np.where(snow_ser, -99, snow_ser2)
    np_result = np.where(input_ser, -99, input_ser2)
    assert_array_equal(snow_result, np_result)


@sql_count_checker(query_count=1, join_count=1)
def test_scalar_y_df():
    input_df = native_pd.DataFrame([[False, True], [False, True]])
    input_df2 = native_pd.DataFrame([[1, 0], [0, 1]])
    snow_df = pd.DataFrame(input_df)
    snow_df2 = pd.DataFrame(input_df2)
    snow_result = np.where(snow_df, snow_df2, -99)
    np_result = np.where(input_df, input_df2, -99)
    assert_array_equal(snow_result, np_result)


@sql_count_checker(query_count=1, join_count=2)
def test_where_with_same_indexes():
    pdf1 = native_pd.DataFrame([0, 1], index=["a", "b"])
    pdf2 = native_pd.DataFrame([1, 0], index=["a", "b"])
    sdf1 = pd.DataFrame(pdf1)
    sdf2 = pd.DataFrame(pdf2)
    numpy_result = np.where(pdf1 == 0, pdf2, pdf1)
    snow_result = np.where(sdf1 == 0, sdf2, sdf1)
    assert_array_equal(snow_result, numpy_result)


@sql_count_checker(query_count=1, join_count=2)
def test_where_with_different_indexes():
    pdf1 = native_pd.DataFrame([0, 1], index=["a", "b"])
    pdf2 = native_pd.DataFrame([1, 0], index=["b", "a"])
    sdf1 = pd.DataFrame(pdf1)
    sdf2 = pd.DataFrame(pdf2)
    numpy_result = np.where(pdf1 == 0, pdf2, pdf1)
    snow_result = np.where(sdf1 == 0, sdf2, sdf1)
    with pytest.raises(AssertionError):
        assert_array_equal(snow_result, numpy_result)


@sql_count_checker(query_count=1, join_count=2)
def test_where_with_same_columns():
    pdf1 = native_pd.DataFrame([[0, 1, 2], [3, 4, 5]], columns=["k", "w", "v"])
    pdf2 = native_pd.DataFrame([[6, 7, 8], [9, 10, 11]], columns=["k", "w", "v"])
    sdf1 = pd.DataFrame(pdf1)
    sdf2 = pd.DataFrame(pdf2)
    numpy_result = np.where(pdf1 == 0, pdf2, pdf1)
    snow_result = np.where(sdf1 == 0, sdf2, sdf1)
    assert_array_equal(snow_result, numpy_result)


@sql_count_checker(query_count=0, join_count=0)
def test_where_with_different_columns_negative():
    pdf1 = native_pd.DataFrame([[0, 1, 0], [3, 0, 5]], columns=["w", "v", "k"])
    pdf2 = native_pd.DataFrame([[6, 7, 8], [9, 10, 11]], columns=["k", "v", "w"])
    sdf1 = pd.DataFrame(pdf1)
    sdf2 = pd.DataFrame(pdf2)
    with pytest.raises(TypeError):
        np.where(sdf1 == 0, sdf2, sdf1)
