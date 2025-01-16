#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import multithreaded_run


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


@multithreaded_run()
@sql_count_checker(query_count=1, join_count=2)
def test_mul_after_add():
    # bug fix SNOW-1632454
    native_df1 = native_pd.DataFrame({"X": [1, 2, 3]})
    native_df2 = native_pd.DataFrame({"X": [4, 5, 6]})
    native_result = native_df1.mul(native_df2).add(native_df2)

    snow_df1 = pd.DataFrame(native_df1)
    snow_df2 = pd.DataFrame(native_df2)
    snow_result = snow_df1.mul(snow_df2).add(snow_df2)

    assert_frame_equal(snow_result, native_result)


@sql_count_checker(query_count=1, join_count=3)
def test_tree_of_binary_operations():
    # bug fix SNOW-1518846
    native_df1 = native_pd.DataFrame([[1, 2]])
    native_df2 = native_pd.DataFrame([[3, 4]])
    native_result = (native_df1 == native_df2) | (native_df1.isna() & native_df2.isna())

    snow_df1 = pd.DataFrame(native_df1)
    snow_df2 = pd.DataFrame(native_df2)
    snow_result = (snow_df1 == snow_df2) | (snow_df1.isna() & snow_df2.isna())

    assert_frame_equal(snow_result, native_result)
