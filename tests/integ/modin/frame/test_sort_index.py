#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@pytest.mark.parametrize("ignore_index", [True, False])
@pytest.mark.parametrize("inplace", [True, False])
@pytest.mark.parametrize(
    "native_df",
    [
        param(
            native_pd.DataFrame(
                [1, 2, None, 4, 5], index=[np.nan, 29, 234, 1, 150], columns=["A"]
            ),
            id="integers",
        ),
        param(
            # We have to construct the timedelta frame slightly differently to work
            # around https://github.com/pandas-dev/pandas/issues/60064
            native_pd.DataFrame(
                [1, 2, pd.NaT, 4, 5],
                index=[np.nan, 29, 234, 1, 150],
                columns=["A"],
                dtype="timedelta64[ns]",
            ),
            id="timedeltas",
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_sort_index_dataframe(ascending, na_position, ignore_index, inplace, native_df):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.sort_index(
            ascending=ascending,
            na_position=na_position,
            ignore_index=ignore_index,
            inplace=inplace,
        ),
        inplace=inplace,
    )


@sql_count_checker(query_count=0)
def test_sort_index_dataframe_axis_1_unsupported():
    snow_df = pd.DataFrame(
        [1, 2, np.nan, 4, 5], index=[np.nan, 29, 234, 1, 150], columns=["A"]
    )
    with pytest.raises(NotImplementedError):
        snow_df.sort_index(axis=1)


@sql_count_checker(query_count=0)
def test_sort_index_dataframe_multiindex_unsupported():
    arrays = [
        np.array(["qux", "qux", "foo", "foo", "baz", "baz", "bar", "bar"]),
        np.array(["two", "one", "two", "one", "two", "one", "two", "one"]),
    ]
    snow_df = pd.DataFrame([1, 2, 3, 4, 5, 6, 7, 8], index=arrays, columns=["A"])
    with pytest.raises(NotImplementedError):
        snow_df.sort_index(level=1)
    with pytest.raises(NotImplementedError):
        snow_df.sort_index(sort_remaining=True)
    with pytest.raises(NotImplementedError):
        snow_df.sort_index(ascending=[True, False])
