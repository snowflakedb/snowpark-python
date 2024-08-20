#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import random

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._libs.lib import no_default

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result

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
@pytest.mark.parametrize(
    "fill_value", [None, no_default, 42]
)  # no_default is the default value, so test explicitly as well. 42 is added to test for "type" conflicts.
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
