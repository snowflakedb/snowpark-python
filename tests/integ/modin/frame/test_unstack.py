#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "index_names",
    [
        [None, None],
        ["hello", "world"],
        [None, "world"],
        ["hello", None],
    ],
)
@pytest.mark.parametrize(
    "dtype",
    [
        float,
        param(
            "timedelta64[ns]",
            marks=pytest.mark.xfail(strict=True, raises=NotImplementedError),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_unstack_input_no_multiindex(index_names, dtype):
    index = native_pd.MultiIndex.from_tuples(
        tuples=[("one", "a"), ("one", "b"), ("two", "a"), ("two", "b")],
        names=index_names,
    )
    # Note we call unstack below to create a dataframe without a multiindex before
    # calling unstack again
    native_df = native_pd.Series(np.arange(1.0, 5.0), index=index, dtype=dtype).unstack(
        level=0
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.unstack())


@pytest.mark.parametrize(
    "index_names",
    [
        [None, None],
        ["hello", "world"],
        ["hello", None],
        [None, "world"],
    ],
)
@sql_count_checker(query_count=1)
def test_unstack_multiindex(index_names):
    index = pd.MultiIndex.from_product(
        iterables=[[2, 1], ["a", "b"]], names=index_names
    )
    native_df = native_pd.DataFrame(np.random.randn(4), index=index, columns=["A"])
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.unstack())


@sql_count_checker(query_count=1, join_count=1)
def test_unstack_multiple_columns():
    index = pd.MultiIndex.from_product([[2, 1], ["a", "b"]])
    native_df = native_pd.DataFrame(
        {"A": [1, 2, 3, 4], "B": ["f", "o", "u", "r"]}, index=index
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df.unstack())


@sql_count_checker(query_count=0)
def test_unstack_sort_notimplemented():
    index = pd.MultiIndex.from_product([[2, 1], ["a", "b"]])
    native_df = native_pd.DataFrame(np.random.randn(4), index=index, columns=["A"])
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas DataFrame/Series.unstack does not yet support the `sort` parameter",
    ):
        snow_df.unstack(sort=False)


@sql_count_checker(query_count=0)
def test_unstack_non_integer_level_notimplemented():
    # Still requires one query at the frontend layer checking number of levels
    index = pd.MultiIndex.from_product([[2, 1], ["a", "b"]])
    native_df = native_pd.DataFrame(np.random.randn(4), index=index, columns=["A"])
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas DataFrame/Series.unstack does not yet support a non-integer `level` parameter",
    ):
        snow_df.unstack(level=[0, 1])
