#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import contextlib
import re

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas._libs.lib import no_default

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.base_overrides import (
    _TIMEDELTA_PCT_CHANGE_AXIS_1_MIXED_TYPE_ERROR_MESSAGE,
)
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

# From pandas doc example
PCT_CHANGE_NATIVE_FRAME = native_pd.DataFrame(
    {
        "FR": [4.0405, 4.0963, 4.3149],
        "GR": [1.7246, 1.7482, 1.8519],
        "IT": [804.74, 810.01, 860.13],
    },
    index=["1980-01-01", "1980-02-01", "1980-03-01"],
)

# Fake, doctored data with nulls -- do not use this if you actually want to know the prices of the
# French franc, Deutsche Mark, or Italian lira between January/April 1980
PCT_CHANGE_NATIVE_FRAME_WITH_NULLS = native_pd.DataFrame(
    {
        "FR": [4.0405, 4.0963, None, 4.3149],
        "GR": [1.7246, None, None, 1.7482],
        "IT": [804.74, 810.01, 860.13, None],
    },
    index=["1980-01-01", "1980-02-01", "1980-03-01", "1980-04-01"],
)


@pytest.mark.parametrize(
    "native_data",
    [PCT_CHANGE_NATIVE_FRAME, PCT_CHANGE_NATIVE_FRAME_WITH_NULLS],
    ids=["nonulls", "withnulls"],
)
@pytest.mark.parametrize(
    "periods",
    [-1, 0, 1, 2],
)
@pytest.mark.parametrize(
    "fill_method", [None, no_default, "ffill", "pad", "backfill", "bfill"]
)
@pytest.mark.parametrize("axis", ["rows", "columns"])
@sql_count_checker(query_count=1)
def test_pct_change_simple(native_data, periods, fill_method, axis):
    # Check deprecation warnings for non-None fill_method
    if fill_method is no_default:
        cm = pytest.warns(
            FutureWarning, match="default fill_method='pad' .* deprecated"
        )
    elif fill_method is not None:
        cm = pytest.warns(
            FutureWarning, match="'fill_method' keyword being not None .* deprecated"
        )
    else:
        cm = contextlib.nullcontext()
    with cm:
        eval_snowpark_pandas_result(
            pd.DataFrame(native_data),
            native_data,
            lambda df: df.pct_change(
                periods=periods, fill_method=fill_method, axis=axis
            ),
        )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "periods",
    [-1, 0, 1, 2],
)
@pytest.mark.parametrize(
    "fill_method", [None, no_default, "ffill", "pad", "backfill", "bfill"]
)
def test_axis_0_with_timedelta(periods, fill_method):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "timedelta": [
                    pd.Timedelta(100),
                    pd.Timedelta(25),
                    None,
                    pd.Timedelta(75),
                ],
                "float": [4.0405, 4.0963, 4.3149, None],
            }
        ),
        lambda df: df.pct_change(periods=periods, fill_method=fill_method),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize(
    "periods",
    [-1, 0, 1, 2],
)
@pytest.mark.parametrize(
    "fill_method", [None, no_default, "ffill", "pad", "backfill", "bfill"]
)
def test_axis_1_with_timedelta_columns(periods, fill_method):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "col0": [pd.Timedelta(100), pd.Timedelta(25)],
                "col1": [pd.Timedelta(50), None],
                "col3": [pd.Timedelta(75), pd.Timedelta(75)],
                "col4": [None, pd.Timedelta(150)],
            }
        ),
        lambda df: df.pct_change(axis=1, periods=periods, fill_method=fill_method),
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "data",
    [
        {"timedelta": [pd.Timedelta(1)], "string": ["value"]},
        {"timedelta": [pd.Timedelta(1)], "int": [0]},
    ],
)
def test_axis_1_with_timedelta_and_non_timedelta_column_invalid(data):
    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.pct_change(axis=1),
        expect_exception=True,
        # pandas exception depends on the type of the non-timedelta column, so
        # we don't try to match the pandas exception.
        assert_exception_equal=False,
        expect_exception_match=re.escape(
            _TIMEDELTA_PCT_CHANGE_AXIS_1_MIXED_TYPE_ERROR_MESSAGE
        ),
    )


@pytest.mark.parametrize("params", [{"limit": 2}, {"freq": "ME"}])
@sql_count_checker(query_count=0)
def test_pct_change_unsupported_args(params):
    with pytest.raises(NotImplementedError):
        pd.DataFrame(PCT_CHANGE_NATIVE_FRAME).pct_change(**params)


@sql_count_checker(query_count=0)
def test_pct_change_bad_periods():
    # native pandas does not preemptively type check the periods argument
    with pytest.raises(
        TypeError, match="'>' not supported between instances of 'str' and 'int'"
    ):
        PCT_CHANGE_NATIVE_FRAME.pct_change(periods="2", fill_method=None)
    with pytest.raises(
        TypeError, match="periods must be an int. got <class 'str'> instead"
    ):
        pd.DataFrame(PCT_CHANGE_NATIVE_FRAME).pct_change(periods="2", fill_method=None)


@pytest.mark.parametrize(
    "data",
    [
        {"col": ["abc", "def"]},
        {"col": [pd.Timestamp("00:00:00").time(), pd.Timestamp("01:00:00").time()]},
    ],
)
@sql_count_checker(query_count=0)
def test_pct_change_bad_dtypes(data):
    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.pct_change(fill_method=None),
        expect_exception=True,
        expect_exception_type=TypeError,
        expect_exception_match="cannot perform pct_change on non-numeric column with dtype",
        assert_exception_equal=False,
    )
