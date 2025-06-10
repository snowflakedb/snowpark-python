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
    "dataframe_input, test_case_name",
    [
        (
            {
                "A": [1, 2, 3],
                "B": [4, 5, 6],
                "C": native_pd.timedelta_range(1, periods=3),
            },
            "simple non-empty",
        ),
        ({"A": [], "B": []}, "empty column"),
        ({"A": [np.nan]}, "np nan column"),
    ],
)
@sql_count_checker(query_count=0)
def test_dataframe_empty_param(dataframe_input, test_case_name):
    eval_snowpark_pandas_result(
        pd.DataFrame(dataframe_input),
        native_pd.DataFrame(dataframe_input),
        lambda df: df.empty,
        comparator=lambda x, y: x == y,
    )


@sql_count_checker(query_count=0)
def test_dataframe_empty_only_index(empty_index_native_pandas_dataframe):
    eval_snowpark_pandas_result(
        pd.DataFrame(empty_index_native_pandas_dataframe),
        empty_index_native_pandas_dataframe,
        lambda df: df.empty,
        comparator=lambda x, y: x == y,
    )
