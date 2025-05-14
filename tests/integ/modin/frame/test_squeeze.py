#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter


@pytest.fixture(params=[0, "index", 1, "columns", None])
def axis(request):
    """
    cache keyword to pass to to_datetime.
    """
    return request.param


@pytest.mark.parametrize("dtype", ["int", "timedelta64[ns]"])
def test_n_by_1(axis, dtype):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            *create_test_dfs([1, 2, 3], dtype=dtype),
            lambda df: df.squeeze(axis=axis),
        )


@pytest.mark.parametrize("dtype", ["int", "timedelta64[ns]"])
def test_1_by_n(axis, dtype):
    if axis is None:
        expected_query_count = 2
    else:
        expected_query_count = 1
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            *create_test_dfs({"a": [1], "b": [2], "c": [3]}, dtype=dtype),
            lambda df: df.squeeze(axis=axis),
        )


def test_2d(axis):
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            *create_test_dfs(
                {
                    "A": [1, 2, 3],
                    "B": [2, 3, 4],
                    "Timedelta": native_pd.to_timedelta([5, 6, 7]),
                }
            ),
            lambda df: df.squeeze(axis=axis),
        )


@pytest.mark.parametrize(
    "scalar", [param(pd.Timedelta(1), id="timedelta"), param(1, id="int")]
)
def test_scalar(axis, scalar):
    snow_df, native_df = create_test_dfs([scalar])
    with SqlCounter(query_count=1):
        if axis is None:
            assert scalar == snow_df.squeeze()
        else:
            # still return a dataframe/series
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: df.squeeze(axis=axis),
            )


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Transpose produces a column with both an int value and a timedelta value, so it can't preserve the timedelta type for the timedelta row.",
)
@pytest.mark.parametrize("axis", [0, "index", None])
def test_timedelta_1_by_n_horizontal(axis):
    eval_snowpark_pandas_result(
        *create_test_dfs([[1, pd.Timedelta(2)]]),
        lambda df: df.squeeze(axis=axis),
    )
