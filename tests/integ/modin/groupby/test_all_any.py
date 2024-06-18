#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
# tests pulled from pandas/pandas/tests/groupby/test_min_max.py
#
import modin.pandas as pd
import numpy as np
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result


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
    "data",
    [
        {"by": ["a", "b", "a", "c"], "value": ["a", "", None, "abc"]},
        {"by": ["a", "b", "a", "c"], "value": [1.1, 2.0, 0.0, np.nan]},
    ],
)
@sql_count_checker(query_count=0)
def test_all_any_invalid_types(data):
    msg = "Snowpark pandas GroupBy.all API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        pd.DataFrame(data).groupby("by").all()
    msg = "Snowpark pandas GroupBy.any API doesn't yet support non-integer/boolean columns"
    with pytest.raises(NotImplementedError, match=msg):
        pd.DataFrame(data).groupby("by").any()


@sql_count_checker(query_count=5, join_count=1, udtf_count=1)
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
