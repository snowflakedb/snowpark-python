#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# tests pulled from pandas/pandas/tests/groupby/test_min_max.py
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_frame_equal,
    create_test_dfs,
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


def eval_snowpark_pandas_result(*args, **kwargs):
    # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


@pytest.mark.parametrize(
    "data",
    [
        {"nn": [11, 11, 22, 22], "ii": [0, 2, 3, 4], "ss": [-1, 2, 0, 0]},
        {
            "nn": ["aa", "aa", "bb", "bb"],
            "ii": [False, True, True, True],
            "ss": [True, True, False, False],
        },
    ],
)
@sql_count_checker(query_count=2)
def test_all_any_basic(data):
    eval_snowpark_pandas_result(
        *create_test_dfs(data), lambda df: df.groupby("nn").all()
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(data), lambda df: df.groupby("nn").any()
    )


@pytest.mark.parametrize("agg_func", ["all", "any"])
@pytest.mark.parametrize("by", ["A", "B"])
@sql_count_checker(query_count=1)
def test_timedelta(agg_func, by):
    native_df = native_pd.DataFrame(
        {
            "A": native_pd.to_timedelta(["1 days 06:05:01.00003", "15.5us", "15.5us"]),
            "B": [10, 8, 12],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: getattr(df.groupby(by), agg_func)()
    )


@sql_count_checker(query_count=2)
def test_all_any_empty():
    data = {"nn": [11, 11, 22, 22]}
    eval_snowpark_pandas_result(
        *create_test_dfs(data), lambda df: df.groupby("nn").all()
    )
    eval_snowpark_pandas_result(
        *create_test_dfs(data), lambda df: df.groupby("nn").any()
    )


@pytest.mark.parametrize(
    "data, msg",
    [
        (
            {"by": ["a", "b", "a", "c"], "value": ["a", "", None, "abc"]},
            "Boolean value 'a' is not recognized",
        ),
        (
            {"by": ["a", "b", "a", "c"], "value": [1.1, 2.0, 0.0, np.nan]},
            re.escape(
                """invalid type [TO_BOOLEAN("values"."value")] for parameter 'TO_BOOLEAN'"""
            ),
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_all_any_invalid_types(data, msg):
    with pytest.raises(SnowparkSQLException, match=msg):
        pd.DataFrame(data).groupby("by").all().to_pandas()
    with pytest.raises(SnowparkSQLException, match=msg):
        pd.DataFrame(data).groupby("by").any().to_pandas()


@sql_count_checker(query_count=4, join_count=1, udtf_count=1)
@pytest.mark.udf
def test_all_any_chained():
    data = {
        "by": ["a", "a", "b", "c", "c"],
        "value": [
            "a",
            "",
            None,
            "abc",
            "def",
        ],
    }
    # Even though we don't yet support all/any on string data, pre-processing it into bool/int is fine
    # For strings, we can get the same results by performing an apply to get the string's length
    eval_snowpark_pandas_result(
        *create_test_dfs(data),
        lambda df: df.groupby("by").apply(
            lambda df: df.apply(lambda ser: ser.str.len())
        )
    )


@sql_count_checker(query_count=1)
def test_timedelta_any_with_nulls():
    """
    Test this case separately because pandas behavior is different from Snowpark pandas behavior.

    pandas bug that does not apply to Snowpark pandas:
    https://github.com/pandas-dev/pandas/issues/59712
    """
    snow_df, native_df = create_test_dfs(
        {
            "key": ["a"],
            "A": native_pd.Series([pd.NaT], dtype="timedelta64[ns]"),
        },
    )
    assert_frame_equal(
        native_df.groupby("key").any(),
        native_pd.DataFrame({"A": [True]}, index=native_pd.Index(["a"], name="key")),
    )
    assert_frame_equal(
        snow_df.groupby("key").any(),
        native_pd.DataFrame({"A": [False]}, index=native_pd.Index(["a"], name="key")),
    )
