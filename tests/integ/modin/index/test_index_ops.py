#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

from tests.integ.modin.utils import assert_index_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "func",
    [
        # addition
        lambda x: x + 1,
        lambda x: -1.1 + x,
        # subtraction
        lambda x: -x,
        lambda x: x - 2,
        lambda x: -2.2 - x,
        # multiplication
        lambda x: x * 3,
        lambda x: 0 * x,
        # division
        lambda x: 0 / x,
        lambda x: x / 2,
        # floor division
        lambda x: x // 5,
        lambda x: 0 // x,
        # pow
        lambda x: x**1,
        lambda x: 1.0**x,
        # mod
        lambda x: x % 2,
        lambda x: 10 % x,
    ],
)
@sql_count_checker(query_count=1)
def test_index_ops(func):
    data = [-10, 5, 2.1]
    eval_snowpark_pandas_result(pd.Index(data), native_pd.Index(data), func)


@pytest.mark.parametrize(
    "func",
    [
        lambda x: x == 0,
        lambda x: x != 0,
        lambda x: x > 0,
        lambda x: x >= 0,
        lambda x: x < 0,
        lambda x: x <= 0,
    ],
)
@sql_count_checker(query_count=1)
def test_index_compare_ops(func):
    native_idx = native_pd.Index([0, -1, 1.1])
    snow_idx = pd.Index(native_idx)
    native_array_res = func(native_idx)
    snow_idx_res = func(snow_idx)
    assert_index_equal(snow_idx_res, native_pd.Index(native_array_res))


@pytest.mark.parametrize(
    "func",
    [
        lambda x, y: x == y,
        lambda x, y: x != y,
        lambda x, y: x > y,
        lambda x, y: x >= y,
        lambda x, y: x < y,
        lambda x, y: x <= y,
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_index_compare_ops_bw_indices(func):
    left = [-10, 5, 4]
    right = [3, 6, -9]
    native_array_res = func(native_pd.Index(left), native_pd.Index(right))
    snow_idx_res = func(pd.Index(left), pd.Index(right))
    assert_index_equal(snow_idx_res, native_pd.Index(native_array_res))


@pytest.mark.parametrize(
    "func",
    [
        lambda x, y: x + y,
        lambda x, y: x - y,
        lambda x, y: x * y,
        lambda x, y: x / y,
        lambda x, y: x // y,
        lambda x, y: x**y,
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_index_ops_bw_indices(func):
    left = [-10, 5, 2.1]
    right = [2, 6, 7]
    assert_index_equal(
        func(pd.Index(left), pd.Index(right)),
        func(native_pd.Index(left), native_pd.Index(right)),
        check_exact=False,
    )


@pytest.mark.parametrize(
    "func",
    [
        lambda x: x & 1,
        lambda x: x | 1,
        lambda x: x ^ 1,
        lambda x: x << 1,
        lambda x: x >> 1,
        lambda x: 1 & x,
        lambda x: 1 | x,
        lambda x: 1 ^ x,
        lambda x: 1 << x,
        lambda x: 1 >> x,
    ],
)
@sql_count_checker(query_count=0)
def test_index_not_implemented_ops(func):
    with pytest.raises(NotImplementedError):
        func(pd.Index([-10, 5, 2.1]))
