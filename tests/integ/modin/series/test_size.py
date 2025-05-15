#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter


@pytest.mark.parametrize(
    "args, kwargs, expected_query_count",
    [
        ([{"A": [1, 2, 3]}], {}, 0),
        ([{"A": []}], {}, 0),
        ([[]], {}, 0),
        ([None], {}, 0),
        (
            [[1, 2, 3, 4]],
            {
                "index": native_pd.MultiIndex.from_product(
                    [["A", "B"], ["C", "D"]], names=["Index1", "Index2"]
                )
            },
            1,
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
def test_series_size(args, kwargs, expected_query_count):
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            pd.Series(*args, **kwargs),
            native_pd.Series(*args, **kwargs),
            lambda df: df.size,
            comparator=lambda x, y: x == y,
        )
