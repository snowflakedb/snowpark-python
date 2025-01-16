#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture(scope="function")
def base_dataframe():
    return pd.DataFrame(
        {"a": [2, 1], "b": [3, 4]}, index=native_pd.Index([0, 8], name="ind")
    )


@pytest.fixture()
def snow_df(base_dataframe):
    return base_dataframe.copy()


@pytest.fixture(scope="function")
def native_df(snow_df):
    return snow_df.to_pandas()


@pytest.mark.parametrize("deep", [None, True, False])
@sql_count_checker(query_count=1)
def test_copy(deep, snow_df, native_df):
    # Verify copy is same as original
    assert_snowpark_pandas_equal_to_pandas(snow_df.copy(deep=deep), native_df)


@sql_count_checker(query_count=0)
def test_copy_deep_true_column_names(snow_df):
    snow_df_copy = snow_df.copy(deep=True)
    snow_df_copy.columns = ["c", "d"]

    assert list(snow_df.columns) == ["a", "b"]
    assert list(snow_df_copy.columns) == ["c", "d"]


@sql_count_checker(query_count=0)
def test_copy_deep_false_column_names(snow_df):
    snow_df_copy = snow_df.copy(deep=False)
    snow_df_copy.columns = ["c", "d"]

    assert list(snow_df.columns) == ["c", "d"]
    assert list(snow_df_copy.columns) == ["c", "d"]


@pytest.mark.parametrize(
    "operation",
    [
        lambda df: df.insert(0, "c", 1),
        lambda df: df.sort_values("a", inplace=True),
        lambda df: df.reset_index(inplace=True),
        lambda df: df.rename(columns={"a": "new_a"}, inplace=True),
    ],
)
@sql_count_checker(query_count=1)
def test_copy_inplace_operations_on_deep_copy(snow_df, native_df, operation):
    snow_df_copy = snow_df.copy(deep=True)
    operation(snow_df_copy)

    # Verify that 'snow_df' is unchanged.
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df)


@pytest.mark.parametrize(
    "operation",
    [
        lambda df: df.insert(0, "c", 1),
        lambda df: df.sort_values("a", inplace=True),
        lambda df: df.reset_index(inplace=True),
        lambda df: df.rename(columns={"a": "new_a"}, inplace=True),
    ],
)
@sql_count_checker(query_count=2)
def test_copy_inplace_operations_on_shallow_copy(snow_df, operation):
    snow_df_copy = snow_df.copy(deep=False)
    operation(snow_df_copy)
    # Verify that 'snow_df' is also changed.
    native_df_copy = snow_df_copy.to_pandas()
    assert_snowpark_pandas_equal_to_pandas(snow_df, native_df_copy)
