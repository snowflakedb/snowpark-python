#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas.errors import MergeError

from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import multithreaded_run


@pytest.fixture(scope="function")
def left_right_native_df():
    df1 = native_pd.DataFrame(
        {
            "a": [1, 5, 10],
            "left_val": ["a", "b", "c"],
            "another_col": [10, 12, 14],
        },
    )
    df2 = native_pd.DataFrame(
        {
            "a": [1, 2, 3, 6, 7],
            "right_val": [1, 2, 3, 6, 7],
            "another_col_right": [0, np.nan, 3, 6, 8],
        },
    )
    return df1, df2


@pytest.fixture(scope="function")
def left_right_native_df_non_numeric_on():
    df1 = native_pd.DataFrame(
        {
            "a": ["t", "e", "s", "t"],
            "left_val": [1, 3, 5, 7],
            "another_col": [10, 12, 14, 16],
        },
    )
    df2 = native_pd.DataFrame(
        {
            "a": ["h", "e", "l", "l", "o"],
            "right_val": [1, 2, 3, 6, 7],
            "another_col_right": [0, np.nan, 3, 6, 8],
        },
    )
    return df1, df2


@pytest.fixture(scope="function")
def left_right_native_df_switch_column_order():
    df1 = native_pd.DataFrame(
        {
            "left_val": ["a", "b", "c"],
            "a": [1, 5, 10],
            "another_col": [10, 12, 14],
        },
    )
    df2 = native_pd.DataFrame(
        {
            "right_val": [1, 2, 3, 6, 7],
            "another_col_right": [0, np.nan, 3, 6, 8],
            "a": [1, 2, 3, 6, 7],
        },
    )
    return df1, df2


@pytest.fixture(scope="function")
def left_right_native_df_left_right_on():
    df1 = native_pd.DataFrame(
        {
            "a": [1, 5, 10],
            "left_val": ["a", "b", "c"],
            "another_col": [10, 12, 14],
        },
    )
    df2 = native_pd.DataFrame(
        {
            "right_val": [1, 2, 3, 6, 7],
            "another_col_right": [0, np.nan, 3, 6, 8],
            "b": [1, 2, 3, 6, 7],
        },
    )
    return df1, df2


@pytest.fixture(scope="function")
def left_right_timestamp_data():
    quotes = native_pd.DataFrame(
        {
            "time": [
                pd.Timestamp("2016-05-25 13:30:00.023"),
                pd.Timestamp("2016-05-25 13:30:00.023"),
                pd.Timestamp("2016-05-25 13:30:00.030"),
                pd.Timestamp("2016-05-25 13:30:00.041"),
                pd.Timestamp("2016-05-25 13:30:00.048"),
                pd.Timestamp("2016-05-25 13:30:00.049"),
                pd.Timestamp("2016-05-25 13:30:00.072"),
                pd.Timestamp("2016-05-25 13:30:00.075"),
            ],
            "ticker": ["GOOG", "MSFT", "MSFT", "MSFT", "GOOG", "AAPL", "GOOG", "MSFT"],
            "bid": [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01],
            "ask": [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03],
        }
    )
    trades = native_pd.DataFrame(
        {
            "time": [
                pd.Timestamp("2016-05-25 13:30:00.023"),
                pd.Timestamp("2016-05-25 13:30:00.038"),
                pd.Timestamp("2016-05-25 13:30:00.048"),
                pd.Timestamp("2016-05-25 13:30:00.048"),
                pd.Timestamp("2016-05-25 13:30:00.048"),
            ],
            "ticker": ["MSFT", "MSFT", "GOOG", "GOOG", "AAPL"],
            "price": [51.95, 51.95, 720.77, 720.92, 98.0],
            "quantity": [75, 155, 100, 100, 100],
        }
    )
    return quotes, trades


allow_exact_matches = pytest.mark.parametrize("allow_exact_matches", [True, False])
direction = pytest.mark.parametrize("direction", ["backward", "forward"])


@multithreaded_run()
@pytest.mark.parametrize("on", ["a", ["a"]])
@allow_exact_matches
@direction
def test_merge_asof_on(left_right_native_df, on, allow_exact_matches, direction):
    left_native_df, right_native_df = left_right_native_df
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    with SqlCounter(query_count=1, join_count=1):
        native_output = native_pd.merge_asof(
            left_native_df,
            right_native_df,
            on=on,
            allow_exact_matches=allow_exact_matches,
            direction=direction,
        )
        snow_output = pd.merge_asof(
            left_snow_df,
            right_snow_df,
            on=on,
            allow_exact_matches=allow_exact_matches,
            direction=direction,
        )
        assert_snowpark_pandas_equal_to_pandas(snow_output, native_output)
    with SqlCounter(query_count=1, join_count=1):
        native_output = native_pd.merge_asof(
            left_native_df,
            right_native_df,
            left_on=on,
            right_on=on,
            allow_exact_matches=allow_exact_matches,
            direction=direction,
        )
        snow_output = pd.merge_asof(
            left_snow_df,
            right_snow_df,
            left_on=on,
            right_on=on,
            allow_exact_matches=allow_exact_matches,
            direction=direction,
        )
        assert_snowpark_pandas_equal_to_pandas(snow_output, native_output)


@allow_exact_matches
@direction
@sql_count_checker(query_count=1, join_count=1)
def test_merge_asof_on_switch_column_order(
    left_right_native_df_switch_column_order, allow_exact_matches, direction
):
    left_native_df, right_native_df = left_right_native_df_switch_column_order
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(
        left_native_df,
        right_native_df,
        on="a",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    snow_output = pd.merge_asof(
        left_snow_df,
        right_snow_df,
        on="a",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    assert_snowpark_pandas_equal_to_pandas(snow_output, native_output)


@allow_exact_matches
@direction
@sql_count_checker(query_count=1, join_count=1)
def test_merge_asof_left_right_on(
    left_right_native_df_left_right_on, allow_exact_matches, direction
):
    left_native_df, right_native_df = left_right_native_df_left_right_on
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(
        left_native_df,
        right_native_df,
        left_on="a",
        right_on="b",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    snow_output = pd.merge_asof(
        left_snow_df,
        right_snow_df,
        left_on="a",
        right_on="b",
        allow_exact_matches=allow_exact_matches,
        direction=direction,
    )
    assert_snowpark_pandas_equal_to_pandas(snow_output, native_output)


@allow_exact_matches
@direction
@sql_count_checker(query_count=1, join_count=1)
def test_merge_asof_left_right_index(allow_exact_matches, direction):
    native_left = native_pd.DataFrame({"left_val": ["a", "b", "c"]}, index=[1, 5, 10])
    native_right = native_pd.DataFrame(
        {"right_val": [1, 2, 3, 6, 7]}, index=[1, 2, 3, 6, 7]
    )

    snow_left = pd.DataFrame(native_left)
    snow_right = pd.DataFrame(native_right)

    native_output = native_pd.merge_asof(
        native_left,
        native_right,
        left_index=True,
        right_index=True,
        direction=direction,
        allow_exact_matches=allow_exact_matches,
    )
    snow_output = pd.merge_asof(
        snow_left,
        snow_right,
        left_index=True,
        right_index=True,
        direction=direction,
        allow_exact_matches=allow_exact_matches,
    )
    assert_snowpark_pandas_equal_to_pandas(snow_output, native_output)


@pytest.mark.parametrize("by", ["ticker", ["ticker"]])
@sql_count_checker(query_count=1, join_count=1)
def test_merge_asof_by(left_right_timestamp_data, by):
    left_native_df, right_native_df = left_right_timestamp_data
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(
        left_native_df, right_native_df, on="time", by=by
    )
    snow_output = pd.merge_asof(left_snow_df, right_snow_df, on="time", by=by)
    assert_snowpark_pandas_equal_to_pandas(snow_output, native_output)


@pytest.mark.parametrize(
    "left_by, right_by",
    [
        ("ticker", "ticker"),
        (["ticker", "bid"], ["ticker", "price"]),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_merge_asof_left_right_by(left_right_timestamp_data, left_by, right_by):
    left_native_df, right_native_df = left_right_timestamp_data
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(
        left_native_df, right_native_df, on="time", left_by=left_by, right_by=right_by
    )
    snow_output = pd.merge_asof(
        left_snow_df, right_snow_df, on="time", left_by=left_by, right_by=right_by
    )
    assert_snowpark_pandas_equal_to_pandas(snow_output, native_output)


@sql_count_checker(query_count=1, join_count=1)
def test_merge_asof_date(left_right_timestamp_data):
    left_native_df, right_native_df = left_right_timestamp_data
    left_native_df["time"] = native_pd.to_datetime(left_native_df["time"])
    right_native_df["time"] = native_pd.to_datetime(right_native_df["time"])
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    native_output = native_pd.merge_asof(
        left_native_df, right_native_df, on="time", by="ticker"
    )
    snow_output = pd.merge_asof(left_snow_df, right_snow_df, on="time", by="ticker")
    assert_snowpark_pandas_equal_to_pandas(snow_output, native_output)


@sql_count_checker(query_count=0)
def test_merge_asof_negative_non_numeric_on(left_right_native_df_non_numeric_on):
    left_native_df, right_native_df = left_right_native_df_non_numeric_on
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    # pandas raises a ValueError with incompatible merge dtype
    with pytest.raises(
        MergeError,
        match=re.escape(
            "Incompatible merge dtype, dtype('O') and dtype('O'), both sides must have numeric dtype"
        ),
    ):
        native_pd.merge_asof(left_native_df, right_native_df, on="a")
    # Snowpark pandas raises a SnowparkSQLException
    # MATCH_CONDITION clause is invalid: The left and right side expressions must be numeric or timestamp expressions.
    with pytest.raises(
        SnowparkSQLException,
    ):
        pd.merge_asof(left_snow_df, right_snow_df, on="a").to_pandas()


@sql_count_checker(query_count=0)
def test_merge_asof_negative_multiple_on(left_right_native_df):
    left_native_df, right_native_df = left_right_native_df
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )

    # MergeError is raised when multiple columns passed to on
    with pytest.raises(MergeError, match="can only asof on a key for left"):
        native_pd.merge_asof(
            left_native_df, right_native_df, on=["a", "another_col_right"]
        )
    with pytest.raises(MergeError, match="can only asof on a key for left"):
        pd.merge_asof(left_snow_df, right_snow_df, on=["a", "another_col_right"])
    # MergeError is raised when multiple columns passed to left_on and right_on
    with pytest.raises(MergeError, match="can only asof on a key for left"):
        native_pd.merge_asof(
            left_native_df,
            right_native_df,
            left_on=["a", "another_col"],
            right_on=["a", "another_col_right"],
        )
    with pytest.raises(MergeError, match="can only asof on a key for left"):
        pd.merge_asof(
            left_snow_df,
            right_snow_df,
            left_on=["a", "another_col"],
            right_on=["a", "another_col_right"],
        )
    # ValueError is raised when left_on and right_on are lists not of the same length
    with pytest.raises(
        ValueError, match=re.escape("len(right_on) must equal len(left_on)")
    ):
        native_pd.merge_asof(
            left_native_df,
            right_native_df,
            left_on=["a"],
            right_on=["a", "another_col_right"],
        )
    with pytest.raises(
        ValueError, match=re.escape("len(right_on) must equal len(left_on)")
    ):
        pd.merge_asof(
            left_snow_df,
            right_snow_df,
            left_on=["a"],
            right_on=["a", "another_col_right"],
        )
    # ValueError is raised when left_on is a list of length > 1 and right_on is a scalar
    with pytest.raises(
        ValueError, match=re.escape("len(right_on) must equal len(left_on)")
    ):
        native_pd.merge_asof(
            left_native_df, right_native_df, left_on=["a", "another_col"], right_on="a"
        )
    with pytest.raises(
        ValueError, match=re.escape("len(right_on) must equal len(left_on)")
    ):
        pd.merge_asof(
            left_snow_df, right_snow_df, left_on=["a", "another_col"], right_on="a"
        )


@sql_count_checker(query_count=0)
def test_merge_asof_nearest_unsupported(left_right_timestamp_data):
    left_native_df, right_native_df = left_right_timestamp_data
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas merge_asof method only supports directions 'forward' and 'backward'",
    ):
        pd.merge_asof(left_snow_df, right_snow_df, on="time", direction="nearest")


@sql_count_checker(query_count=0)
def test_merge_asof_params_unsupported(left_right_timestamp_data):
    left_native_df, right_native_df = left_right_timestamp_data
    left_snow_df, right_snow_df = pd.DataFrame(left_native_df), pd.DataFrame(
        right_native_df
    )
    with pytest.raises(
        NotImplementedError,
        match=(
            "Snowpark pandas merge_asof method only supports directions 'forward' and 'backward'"
        ),
    ):
        pd.merge_asof(
            left_snow_df, right_snow_df, on="time", by="price", direction="nearest"
        )
    with pytest.raises(
        NotImplementedError,
        match=(
            "Snowpark pandas merge_asof method does not currently support parameters "
            + "'suffixes', or 'tolerance'"
        ),
    ):
        pd.merge_asof(
            left_snow_df,
            right_snow_df,
            on="time",
            suffixes=("_hello", "_world"),
        )
    with pytest.raises(
        NotImplementedError,
        match=(
            "Snowpark pandas merge_asof method does not currently support parameters "
            + "'suffixes', or 'tolerance'"
        ),
    ):
        pd.merge_asof(
            left_snow_df,
            right_snow_df,
            on="time",
            tolerance=native_pd.Timedelta("3s"),
        )
