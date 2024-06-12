#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


@sql_count_checker(query_count=2)
def test_chained_op1():
    # bug fix SNOW-1348886
    data = {"X": [1, 2, 3], "Y": [4, 5, 6]}
    snow_df = pd.DataFrame(data)
    # Add row_count column
    snow_df.__repr__()

    native_df = native_pd.DataFrame(data)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df["X"].sort_values(ascending=True)
    )
