#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    assert_values_equal,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def nonempty_boolagg_sql_counter(axis):
    # All operations incur 1 query to perform the initial aggregation, regardless of axis.
    # axis=None calls the query compiler function with axis=0 twice.
    expected_query_count = 2 if axis is None else 1
    return SqlCounter(query_count=expected_query_count)


def boolagg_comparator(axis):
    return (
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck
        if axis is not None
        else assert_values_equal
    )


@pytest.mark.parametrize("axis", [0, 1])
@pytest.mark.parametrize(
    "data",
    [
        [],
        [[]],
        [[], [], []],
        pytest.param(
            {"a": [], "b": []},
            marks=pytest.mark.skip(
                "empty rows are treated as float64; TO_BOOLEAN cast does not accept float arguments"
            ),
        ),
    ],
)
def test_empty(data, axis):
    with SqlCounter(query_count=1):
        # Treat index like any other column in a DataFrame when it comes to types,
        # therefore Snowpark pandas returns an Index(dtype="object") for an empty index
        # whereas pandas returns RangeIndex()
        # This is compatible with behavior for empty dataframe in other tests.
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
            comparator=assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
            check_index_type=False,
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
            comparator=assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
            check_index_type=False,
        )


@pytest.mark.parametrize(
    "data",
    [
        [],
        [[]],
        [[], [], []],
        {"a": [], "b": []},
    ],
)
@pytest.mark.parametrize("method", ["any", "all"])
def test_empty_axis_none(data, method):
    """This test function is separate from the other empty dataframe test
    function because we expected a scalar result, so we can't pass the
    check_index_type kwarg."""
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(data, dtype=bool),
            native_pd.DataFrame(data),
            lambda df: getattr(df, method)(axis=None),
            comparator=assert_values_equal,
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": [1, 2, 3], "b": [4, 5, 6]},
        [[0, 0, 1], [0, 1, 1], [1, 1, 1]],
        [[1, 1, 1], [1, 1, 1], [1, 1, 1]],
    ],
)
def test_all_int(data, axis):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": [1, 2, 3], "b": [4, 5, 6]},
        [[0, 0, 1], [0, 1, 1], [0, 1, 1]],
        [[0, 0, 0], [0, 0, 0], [0, 0, 0]],
    ],
)
def test_any_int(data, axis):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", ["columns", "index"])
def test_all_axis_str_arg(axis):
    with nonempty_boolagg_sql_counter(axis):
        data = [[0, 1], [0, 1]]
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
        )


@pytest.mark.parametrize("axis", ["columns", "index"])
def test_any_axis_str_arg(axis):
    with nonempty_boolagg_sql_counter(axis):
        data = [[0, 1], [0, 1]]
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
        )


@pytest.mark.parametrize("axis", [None, 0, 1])
def test_all_named_index(axis):
    with SqlCounter(query_count=1):
        data = {"a": [1, 0, 3], "b": [4, 5, 6]}
        index_name = ["c", "d", "e"]
        eval_snowpark_pandas_result(
            pd.DataFrame(data, index_name),
            native_pd.DataFrame(data, index_name),
            lambda df: df.all(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [None, 0, 1])
def test_any_named_index(axis):
    with SqlCounter(query_count=1):
        data = {"a": [1, 0, 3], "b": [4, 5, 6]}
        index_name = ["c", "d", "e"]
        eval_snowpark_pandas_result(
            pd.DataFrame(data, index_name),
            native_pd.DataFrame(data, index_name),
            lambda df: df.any(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "c": [True, False, False],
            "d": [True, True, True],
        }
    ],
)
def test_all_bool_only(data, axis):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.all(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "c": [True, False, False],
            "d": [False, False, False],
        }
    ],
)
def test_any_bool_only(data, axis):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            pd.DataFrame(data),
            native_pd.DataFrame(data),
            lambda df: df.any(axis=axis),
            comparator=boolagg_comparator(axis),
        )


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": [0.1, 0.0, 0.3], "b": [0.4, 0.5, 0.6]},
        {"a": [np.nan, 0.0, np.nan], "b": [0.4, 0.5, np.nan]},
    ],
)
@pytest.mark.parametrize("skipna", [True, False])
@sql_count_checker(query_count=0)
def test_all_float_not_implemented(data, axis, skipna):
    df = pd.DataFrame(data)
    msg = "Snowpark pandas all API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        df.all(axis=axis, skipna=skipna)


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": [0.1, 0.0, 0.3], "b": [0.4, 0.5, 0.6]},
        {"a": [np.nan, 0.0, np.nan], "b": [0.4, 0.5, np.nan]},
    ],
)
@pytest.mark.parametrize("skipna", [True, False])
@sql_count_checker(query_count=0)
def test_any_float_not_implemented(data, axis, skipna):
    df = pd.DataFrame(data)
    msg = "Snowpark pandas any API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        df.any(axis=axis, skipna=skipna)


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": ["", "b", "c"], "b": ["d", "e", "f"]},
    ],
)
@sql_count_checker(query_count=0)
def test_all_str_not_implemented(data, axis):
    df = pd.DataFrame(data)
    msg = "Snowpark pandas all API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        df.all(axis=axis)


@pytest.mark.parametrize("axis", [0, 1, None])
@pytest.mark.parametrize(
    "data",
    [
        {"a": ["", "b", "c"], "b": ["", "e", "f"]},
    ],
)
@sql_count_checker(query_count=0)
def test_any_str_not_implemented(data, axis):
    df = pd.DataFrame(data)
    msg = "Snowpark pandas any API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        df.any(axis=axis)
