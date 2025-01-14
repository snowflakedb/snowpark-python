#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("na_position", ["first", "last"])
@pytest.mark.parametrize("ignore_index", [True, False])
@pytest.mark.parametrize("inplace", [True, False])
@pytest.mark.parametrize(
    "data",
    [
        ["a", "b", np.nan, "d"],
        [
            native_pd.Timedelta("1 days"),
            native_pd.Timedelta("2 days"),
            native_pd.Timedelta("3 days"),
            native_pd.Timedelta(None),
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_sort_index_series(ascending, na_position, ignore_index, inplace, data):
    native_series = native_pd.Series(data, index=[3, 2, 1, np.nan])
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda s: s.sort_index(
            ascending=ascending,
            na_position=na_position,
            ignore_index=ignore_index,
            inplace=inplace,
        ),
        inplace=inplace,
    )


@sql_count_checker(query_count=0)
def test_sort_index_series_multiindex_unsupported():
    arrays = [
        np.array(["qux", "qux", "foo", "foo", "baz", "baz", "bar", "bar"]),
        np.array(["two", "one", "two", "one", "two", "one", "two", "one"]),
    ]
    snow_series = pd.Series([1, 2, 3, 4, 5, 6, 7, 8], index=arrays)
    with pytest.raises(NotImplementedError):
        snow_series.sort_index(level=1)
    with pytest.raises(NotImplementedError):
        snow_series.sort_index(sort_remaining=True)
    with pytest.raises(NotImplementedError):
        snow_series.sort_index(ascending=[True, False])
