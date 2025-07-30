#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import tempfile
import pandas as native_pd
import pytest

# We're comparing an object with the native pandas backend, so we use the pandas testing utility
# here rather than our own internal one.
from pandas.testing import assert_series_equal, assert_frame_equal

import modin.pandas as pd
from modin.config import context as config_context
import snowflake.snowpark.modin.plugin  # noqa: F401

from tests.integ.utils.sql_counter import sql_count_checker, SqlCounter


CSV_CONTENT = """fruit,score
apple,1
orange,2
melon,3
grape,4
raisin,-1
"""


# When automatic backend switching is enabled, read_csv should end up in native pandas.
@sql_count_checker(query_count=0)
def test_read_csv_local():
    # delete=False is necessary to allow re-opening the file for reading on windows
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(CSV_CONTENT)
        f.close()
        fruits = pd.read_csv(f.name)
        assert fruits.get_backend() == "Pandas"


@sql_count_checker(query_count=0)
def test_read_csv_remote_backend():
    df = pd.read_csv(
        "s3://sfquickstarts/intro-to-machine-learning-with-snowpark-ml-for-python/diamonds.csv"
    )
    assert df.get_backend() == "Pandas"


# When automatic backend switching is enabled, data made from native lists should end up in native pandas.
@sql_count_checker(query_count=0)
def test_from_list(us_holidays_data):
    # Create DataFrame
    df_us_holidays = pd.DataFrame(us_holidays_data, columns=["Holiday", "Date"])
    # Convert Date column to datetime
    df_us_holidays["Date"] = pd.to_datetime(df_us_holidays["Date"])
    assert (
        df_us_holidays.get_backend() == "Pandas"
    )  # with auto, we should expect this to be local
    # Add new columns for transformations
    df_us_holidays["Day_of_Week"] = df_us_holidays["Date"].dt.day_name()
    df_us_holidays["Month"] = df_us_holidays["Date"].dt.month_name()
    # TODO: test pd.explain output
    # iterrows should not error
    native_df_us_holidays = native_pd.DataFrame(
        us_holidays_data, columns=["Holiday", "Date"]
    )
    native_df_us_holidays["Date"] = native_pd.to_datetime(df_us_holidays["Date"])
    native_df_us_holidays["Day_of_Week"] = native_df_us_holidays["Date"].dt.day_name()
    native_df_us_holidays["Month"] = native_df_us_holidays["Date"].dt.month_name()
    for (index, row), (native_index, native_row) in zip(
        df_us_holidays.iterrows(), native_df_us_holidays.iterrows()
    ):
        assert index == native_index
        assert_series_equal(row.to_pandas(), native_row)


@pytest.mark.udf
def test_move_threshold_setting():
    with config_context(NativePandasMaxRows=10):
        with SqlCounter(
            query_count=11,
            join_count=3,
            udtf_count=2,
            high_count_expected=True,
            high_count_reason="Apply in Snowflake creates UDTF",
        ):
            df_small = pd.DataFrame({"a": [1] * 20, "b": [2] * 20}).move_to("Snowflake")
            assert df_small.get_backend() == "Snowflake"
            # Above threshold; no move
            df_small = df_small.apply(lambda x: x + 1)
            assert df_small.get_backend() == "Snowflake"
        with SqlCounter(query_count=2):
            # Below threshold; should move before apply is performed
            df_small = df_small.head(5)
            df_small = df_small.apply(lambda x: x + 1)
            assert df_small.get_backend() == "Pandas"


@sql_count_checker(query_count=0)
def test_constructor_does_not_double_move():
    # Discovered in 5/1/25 bug bash, fixed in this commit:
    # https://github.com/snowflakedb/snowpark-python/commit/c37b30fb7e478c66e0937f6289603d5d9cc2e1b2
    input_df = pd.DataFrame(list(range(10))).move_to("snowflake")
    assert input_df.get_backend() == "Snowflake"
    assert pd.DataFrame(input_df).get_backend() == "Snowflake"
    assert pd.DataFrame({"col0": input_df[0]}).get_backend() == "Snowflake"
    assert pd.DataFrame(input_df[0]).get_backend() == "Snowflake"

    pandas_df = pd.DataFrame(list(range(10)))
    assert pandas_df.get_backend() == "Pandas"
    assert pd.DataFrame(pandas_df).get_backend() == "Pandas"
    assert pd.DataFrame({"col0": pandas_df[0]}).get_backend() == "Pandas"
    assert pd.DataFrame(pandas_df[0]).get_backend() == "Pandas"


@sql_count_checker(query_count=0)
def test_native_series_argument():
    # SNOW-2173648: Operations like this assignment failed in QueryCompiler.move_to_me_cost()
    df = pd.DataFrame({"a": [1, 2, 3]})
    result_frame = pd.DataFrame(df["a"].to_pandas())
    assert result_frame.get_backend() == "Pandas"
    assert_frame_equal(
        result_frame.to_pandas(),
        native_pd.DataFrame({"a": [1, 2, 3]}),
        check_column_type=False,
        check_index_type=False,
    )
