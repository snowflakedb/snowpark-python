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
        ([{"A": [1, 2, 3]}], {}),
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
def test_series_shape(args, kwargs):
    eval_snowpark_pandas_result(
        pd.Series(*args, **kwargs),
        native_pd.Series(*args, **kwargs),
        lambda df: df.shape,
        comparator=lambda x, y: x == y,
    )
