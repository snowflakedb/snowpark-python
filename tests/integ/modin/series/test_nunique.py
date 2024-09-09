#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    assert_values_equal,
    create_test_series,
    eval_snowpark_pandas_result,
)


@pytest.mark.parametrize(
    "input_data",
    [
        [0, 1, 2, 3],
        [0.1, 0.2, 0.1, 0],
        [None, 0, None, 0],
        [None, None, None, None],
        [],
        ["B", "A", "A", "C", "B"],
        ["B", None, "A", "A", "C", None, "B"],
        [True] * 10,
        ["A"] * 10,
        [0] * 10,
        [True, None, False, True, None],
        [1.1, "a", None] * 4,
        [native_pd.to_datetime("2023-12-01"), native_pd.to_datetime("1999-09-09")] * 2,
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_series_nunique(input_data, dropna):
    eval_snowpark_pandas_result(
        *create_test_series(input_data),
        lambda ser: ser.nunique(dropna=dropna),
        comparator=assert_values_equal,
    )


@pytest.mark.parametrize(
    "input_data,expected",
    [([pd.NaT, np.nan, pd.NA], 1), ([pd.NaT, np.nan, pd.NA, 7, None], 2)],
)
@sql_count_checker(query_count=1)
def test_series_nunique_deviating_nan_behavior(input_data, expected):
    # Snowpark pandas regards pd.NaT, pd.NA, np.na and None to be the same NaN value
    assert pd.Series(input_data).nunique(dropna=False) == expected


@pytest.mark.parametrize(
    "index",
    [
        pytest.param(None, id="default_index"),
        pytest.param(
            [["bar", "bar", "baz", "foo"], ["one", "two", "one", "two"]], id="2D_index"
        ),
    ],
)
def test_dataframe_nunique_multiindex(index):
    data = [0.1, 0.2, 0.1, 0]
    with SqlCounter(query_count=1, join_count=0 if index is None else 2):
        eval_snowpark_pandas_result(
            *create_test_series(data, index=index),
            lambda ser: ser.nunique(),
            comparator=assert_values_equal,
        )
