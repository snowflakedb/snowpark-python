#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import random

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "key",
    [
        [True, True, False, False, False, True, True],
        [True] * 7,
        [False] * 7,
        np.array([True, True, False, False, False, True, True], dtype=bool),
        native_pd.Index([True, True, False, False, False, True, True]),
        [True],
        [True, True, False, False, False, True, True, True],
        native_pd.Index([], dtype=bool),
        np.array([], dtype=bool),
    ],
)
def test_series_getitem_with_boolean_list_like(
    key, default_index_snowpark_pandas_series, default_index_native_series
):
    if isinstance(key, native_pd.Index):
        snow_key = pd.Index(key)
    else:
        snow_key = key

    def getitem_helper(ser):
        # Native pandas can only handle boolean list-likes objects of length = num(rows).
        if isinstance(ser, native_pd.Series):
            # If native pandas Series, truncate the series and key.
            _ser = ser[: len(key)]
            _key = key[: len(_ser)]
        else:
            _key, _ser = snow_key, ser
        return _ser[_key]

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_series,
            default_index_native_series,
            getitem_helper,
        )


@pytest.mark.parametrize(
    "key",
    [
        [],
        [0],
        [-1],
        # unsorted with duplicates
        [2, 3, -1, 3, 2, 1],
        [random.choice(range(-20, 20)) for _ in range(random.randint(1, 20))],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_getitem_with_int_list_like(
    key, default_index_snowpark_pandas_series, default_index_native_series
):
    def getitem_helper(series):
        if isinstance(series, native_pd.Series):
            # Native pandas only supports non-negative integer values in range. Snowpark ignores them.
            # If native pandas DataFrame, remove negative and out-of-bound values to avoid errors and compare.
            _key = [k for k in key if 0 <= k <= 6]
        else:
            _key = key
        return series[_key]

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        getitem_helper,
    )


@pytest.mark.parametrize(
    "key",
    [
        [-5],
        [-1, -2],
        [-2, -3, -1, -3, -2, -1],
        [random.choice(range(-20, 0)) for _ in range(random.randint(1, 20))],
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_getitem_with_int_list_like_returns_empty_series(
    key, default_index_snowpark_pandas_series, default_index_native_series
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        lambda ser: ser[[]] if isinstance(ser, native_pd.Series) else ser[key],
    )


@sql_count_checker(query_count=2, join_count=2)
@pytest.mark.parametrize("key", [0, 6, 100, 3, 5, -6, -7.2, 6.5, -120.3, 23.9])
def test_series_getitem_with_scalars(
    key, default_index_snowpark_pandas_series, default_index_native_series
):
    # Native pandas only supports non-negative integer values in range. Snowpark ignores them.
    def getitem_helper(series):
        if isinstance(series, native_pd.Series):
            _key = key if 0 <= key <= 6 else []  # returns a scalar value
        else:
            _key = key
        return series[_key]

    if 0 <= key <= 6:
        snowpark_res = getitem_helper(default_index_snowpark_pandas_series)
        native_res = getitem_helper(default_index_native_series)
        assert snowpark_res == (
            native_res if not isinstance(native_res, tuple) else list(native_res)
        )
    else:
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_series,
            default_index_native_series,
            getitem_helper,
        )


@pytest.mark.parametrize(
    "key",
    [
        [],
        native_pd.Index([]),
        np.array([]),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_series_getitem_with_empty_keys(
    key, default_index_snowpark_pandas_series, default_index_native_series
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        lambda ser: ser[try_convert_index_to_native(key)],
    )


@pytest.mark.parametrize(
    "key",
    [
        slice(1, 4, 2),  # start < stop, step > 0
        slice(1, 4, -2),  # start < stop, step < 0
        slice(-1, -4, 2),  # start > stop, step > 0
        slice(-1, -4, -2),  # start > stop, step < 0
        slice(3, -1, 4),
        slice(5, 1, -36897),
        # start = step
        slice(3, -1, 4),  # step > 0
        slice(100, 100, 1245),  # step > 0
        slice(-100, -100, -3),  # step < 0
        slice(-100, -100, -36897),  # step < 0
        slice(2, 1, -2),
        # with None
        slice(None, 2, 1),
        slice(-100, None, -2),
    ],
)
@sql_count_checker(query_count=1)
def test_series_getitem_with_slice(
    key, default_index_snowpark_pandas_series, default_index_native_series
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_series,
        default_index_native_series,
        lambda ser: ser[key],
    )


@pytest.mark.parametrize(
    "key",
    [
        np.array([True, True, False, False, False, True, True], dtype=bool),
        [],
        slice(2, 6, 4),
        "baz",
        ["foo", "bar"],
        [("foo", "one"), ("bar", "two")],
    ],
)
def test_series_getitem_with_multiindex(
    key, default_index_native_series, multiindex_native
):
    expected_join_count = 0 if isinstance(key, slice) or isinstance(key, str) else 1
    with SqlCounter(query_count=1, join_count=expected_join_count):
        # Test __getitem__ with series with MultiIndex index.
        native_ser = default_index_native_series.reindex(multiindex_native)
        snowpark_ser = pd.Series(native_ser)
        eval_snowpark_pandas_result(
            snowpark_ser,
            native_ser,
            lambda ser: ser[key],
            check_index_type=False,
        )


@sql_count_checker(query_count=1)
def test_series_getitem_lambda_series():
    data = {"a": 1, "b": 2, "c": 3, "d": -1, "e": 0, "f": 10}
    snow_ser = pd.Series(data)
    native_ser = native_pd.Series(data)

    def helper(ser):
        return ser[lambda x: x < 2]

    eval_snowpark_pandas_result(snow_ser, native_ser, helper)
