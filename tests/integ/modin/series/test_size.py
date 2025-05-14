#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


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
@sql_count_checker(query_count=0)
def test_series_size(args, kwargs):
    eval_snowpark_pandas_result(
        pd.Series(*args, **kwargs),
        native_pd.Series(*args, **kwargs),
        lambda df: df.size,
        comparator=lambda x, y: x == y,
    )
