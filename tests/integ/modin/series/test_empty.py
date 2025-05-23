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


@pytest.mark.parametrize(
    "args, kwargs",
    [
        ([{"A": 1}], {}),
        ([{"A": []}], {}),
        ([[]], {}),
        ([[np.nan]], {}),
        ([np.nan], {}),
        ([None], {}),
        ([], {"index": []}),
    ],
    ids=[
        "simple non-empty",
        "empty column",
        "no name empty column",
        "no name only containing np.nan column",
        "data only has np.nan",
        "data is None",
        "empty series with only index",
    ],
)
@sql_count_checker(query_count=0)
def test_series_empty(args, kwargs):
    eval_snowpark_pandas_result(
        pd.Series(*args, **kwargs),
        native_pd.Series(*args, **kwargs),
        lambda df: df.empty,
        comparator=lambda x, y: x == y,
    )


@sql_count_checker(query_count=5, join_count=2)
def test_empty_series_type():
    def check_dtype(series):
        assert series.to_pandas().dtype == series.dtype

    check_dtype(pd.Series())
    check_dtype(pd.Series([]))
    check_dtype(pd.Series([], name="A"))
    check_dtype(pd.Series([], index=pd.Index([], dtype="int64")))
    check_dtype(pd.Series([], name="A", index=pd.Index([], dtype="int64")))
