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
    "series_input",
    [
        {"A": [[1]]},
        None,
    ],
    ids=[
        "list entry",
        "empty column",
    ],
)
@sql_count_checker(query_count=0)
def test_series_ndim(series_input):
    eval_snowpark_pandas_result(
        pd.Series(series_input),
        native_pd.Series(series_input),
        lambda df: df.empty,
        comparator=lambda x, y: x == y,
    )
