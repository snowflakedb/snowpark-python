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
    "dataframe_input",
    [
        ({"A": [1, 2], "B": [3, 4]}),
        ({"A": [1, 2], "B": [3, 4], "C": [5, 6]}),
        ({"A": [], "B": []}),
        ({"A": [np.nan]}),
        ({"A": [pd.Timedelta(1)]}),
    ],
    ids=[
        "non-empty 2x2",
        "non-empty 2x3",
        "empty column",
        "np nan column",
        "timedelta",
    ],
)
@sql_count_checker(query_count=0)
def test_dataframe_shape_param(dataframe_input):
    eval_snowpark_pandas_result(
        pd.DataFrame(dataframe_input),
        native_pd.DataFrame(dataframe_input),
        lambda df: df.shape,
        comparator=lambda x, y: x == y,
    )


@sql_count_checker(query_count=0)
def test_dataframe_shape_index_empty(empty_index_native_pandas_dataframe):
    eval_snowpark_pandas_result(
        pd.DataFrame(empty_index_native_pandas_dataframe),
        empty_index_native_pandas_dataframe,
        lambda df: df.shape,
        comparator=lambda x, y: x == y,
    )
