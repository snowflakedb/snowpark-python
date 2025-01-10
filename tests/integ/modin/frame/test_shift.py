#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import random

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._libs.lib import no_default
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

TEST_DATAFRAMES = [
    native_pd.DataFrame(),
    native_pd.DataFrame(
        {
            "a": [None, 1, 2, 3],
            "b": ["a", "b", None, "c"],
            "c": [1.0, None, 3.141, 42.3567],
        }
    ),
    native_pd.DataFrame(
        {
            "A": [1, 10, -1, 100, 0, -11],
            "B": [321.2, 312.6, 123.7, 11.32, -0.231, -213],
            "C": [False, True, None, True, False, False],
            "a": ["abc", " ", "", "ABC", "_", "XYZ"],
            "b": ["1", "10", "xyz", "0", "2", "abc"],
            "A_none": [1, None, -1, 100, 0, None],
            "B_none": [None, -312.2, 123.9867, 0.132, None, -0.213],
            "a_none": [None, " ", "", "ABC", "_", "XYZ"],
            "b_none": ["1", "10", None, "0", "2", "abc"],
        },
        index=native_pd.Index([1, 2, 3, 8, 9, 10], name="ind"),
    ),
]


@pytest.mark.parametrize("df", TEST_DATAFRAMES)
@pytest.mark.parametrize(
    "periods", [0, -1, 1, 3, -3, 10, -10]
)  # test here special cases and periods larger than number of rows of dataframe
@pytest.mark.parametrize("fill_value", [None, no_default, 42])
@pytest.mark.parametrize("axis", [0, 1])
@sql_count_checker(query_count=1)
def test_dataframe_with_values_shift(df, periods, fill_value, axis):

    snow_df = pd.DataFrame(df)
    native_df = df.copy()

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.shift(periods=periods, fill_value=fill_value, axis=axis),
        check_column_type=False,
    )


@pytest.mark.parametrize(
    "periods", [0, -1, 1, 3, -3, 10, -10]
)  # test here special cases and periods larger than number of rows of dataframe
@pytest.mark.parametrize(
    "fill_value",
    [
        None,
        no_default,
        pd.Timedelta(42),
        datetime.timedelta(42),
        np.timedelta64(42),
        "42",
    ],
)
@sql_count_checker(query_count=1)
def test_dataframe_with_values_shift_timedelta_axis_0(periods, fill_value):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [pd.Timedelta(1), None, pd.Timedelta(2), pd.Timedelta(3), pd.Timedelta(4)]
        ),
        lambda df: df.shift(periods=periods, fill_value=fill_value),
    )


@pytest.mark.parametrize("fill_value", ["not_a_timedelta", 42, pd.Timestamp(42)])
@sql_count_checker(query_count=0)
def test_dataframe_with_values_shift_timedelta_axis_0_invalid_fill_values(fill_value):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [pd.Timedelta(1), None, pd.Timedelta(2), pd.Timedelta(3), pd.Timedelta(4)]
        ),
        lambda df: df.shift(periods=1, fill_value=fill_value),
        expect_exception=True,
        expect_exception_type=TypeError,
    )


@pytest.mark.parametrize("periods", [0, -1, 1, 3, -3, 10, -10])
@pytest.mark.parametrize(
    "fill_value",
    [
        param(42, id="int"),
        param(pd.Timedelta(42), id="timedelta"),
        param("42", id="string"),
        None,
        no_default,
    ],
)
@sql_count_checker(query_count=1)
def test_shift_axis_1_with_timedelta_column(periods, fill_value):
    eval_snowpark_pandas_result(
        *create_test_dfs(
            {
                "int": [0],
                "string": ["a"],
                "timedelta": [pd.Timedelta(0)],
                "date": [pd.Timestamp(0)],
                "list": [[0]],
            }
        ),
        lambda df: df.shift(periods=periods, fill_value=fill_value, axis=1),
    )


# TODO: SNOW-1023324, implement shifting index. This is a test that must work when specifying freq.
@pytest.mark.parametrize(
    "index",
    [
        native_pd.to_datetime("2021-03-03")
        + native_pd.timedelta_range("1day", "700 days", freq="3D")
    ],
)
@pytest.mark.parametrize("periods", [-1, 0, 1, 10])
@pytest.mark.parametrize("freq", ["1Y", "1D", "4D", "1M"])
@pytest.mark.xfail(reason="to be done in SNOW-1023324, remove negativity of this test")
def test_time_index_shift_negative(index, periods, freq):

    # the data is unaffected, therefore simply generate random data according to the length of the index
    # and sprinkle in some NULLs (20%)
    rng = np.random.default_rng(12345)
    data = rng.random(len(index))
    for _ in range(len(index) // 5):
        data[random.randint(0, len(data) - 1)] = np.nan

    native_df = native_pd.DataFrame(data, index=index)
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.shift(periods=periods, freq=freq),
        check_index_type=False,
    )


@pytest.mark.parametrize(
    "params, match",
    [
        ({"periods": [1, 2, 3]}, "periods"),  # sequence of periods is unsupported
        ({"periods": [1], "suffix": "_suffix"}, "suffix"),  # suffix is unsupported
    ],
)
@sql_count_checker(query_count=0)
def test_shift_unsupported_args(params, match):
    df = pd.DataFrame(TEST_DATAFRAMES[2])
    with pytest.raises(NotImplementedError, match=match):
        df.shift(**params).to_pandas()
