#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.mark.parametrize(
    "args, kwargs",
    [
        ([{"A": [1, 2, 3]}], {}),
        ([{"A": []}], {}),
        ([[]], {}),
        ([None], {}),
        (
            [[1, 2, 3, 4]],
            {
                "index": native_pd.MultiIndex.from_product(
                    [["A", "B"], ["C", "D"]], names=["Index1", "Index2"]
                )
            },
        ),
    ],
    ids=[
        "simple non-empty",
        "empty column",
        "no name empty column",
        "data is None",
        "multi index",
    ],
)
def test_series_size(args, kwargs):
    with SqlCounter(
        query_count=1,
        join_count=2
        if isinstance(kwargs.get("index", None), native_pd.MultiIndex)
        else 0,
    ):
        eval_snowpark_pandas_result(
            pd.Series(*args, **kwargs),
            native_pd.Series(*args, **kwargs),
            lambda df: df.size,
            comparator=lambda x, y: x == y,
        )
