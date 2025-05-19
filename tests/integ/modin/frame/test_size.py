#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "args, kwargs, expected_query_count",
    [
        ([{"A": [1, 2], "B": [3, 4], "C": [5, 6]}], {}, 1),
        ([{"A": [], "B": []}], {}, 1),
        ([np.random.rand(100, 10)], {}, 4),
        (
            [{"Value": [10, 20, 30, 40]}],
            {
                "index": native_pd.MultiIndex.from_arrays(
                    [["A", "A", "B", "B"], [1, 2, 1, 2]], names=["Letter", "Number"]
                )
            },
            1,
        ),
        ([[pd.Timedelta(1), 1]], {}, 1),
    ],
    ids=[
        "non-empty 2x3",
        "empty column",
        "100x10 random dataframe",
        "multi-index",
        "frame_with_timedelta",
    ],
)
def test_dataframe_size_param(args, kwargs, expected_query_count):
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            pd.DataFrame(*args, **kwargs),
            native_pd.DataFrame(*args, **kwargs),
            lambda df: df.size,
            comparator=lambda x, y: x == y,
        )


@sql_count_checker(query_count=1)
def test_dataframe_size_index_empty(empty_index_native_pandas_dataframe):
    eval_snowpark_pandas_result(
        pd.DataFrame(empty_index_native_pandas_dataframe),
        empty_index_native_pandas_dataframe,
        lambda df: df.size,
        comparator=lambda x, y: x == y,
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("func", ["size", len])
def test_dataframe_agg_size_axis_1(func):
    native_df = native_pd.DataFrame(
        [[1, 2, np.nan], [4, 5, np.nan]], columns=list("ABC")
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.agg(func=func, axis=1),
        test_attrs=False,  # native pandas does not propagate attrs for size, but snowpark pandas does
    )
