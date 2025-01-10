#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest
from pandas._libs.lib import no_default

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

TEST_SERIES = [
    native_pd.Series(),
    native_pd.Series(index=[1, 2, 3, 4]),
    native_pd.Series([1, 2, 3, 4]),
    native_pd.Series(["a", None, 1, 2, 4.5]),
    native_pd.Series([2.0, None, 3.6, -10], index=[1, 2, 3, 4]),
    native_pd.Series(
        [
            native_pd.Timedelta("1 days"),
            native_pd.Timedelta("2 days"),
            native_pd.Timedelta("3 days"),
        ],
    ),
]


@pytest.mark.parametrize("series", TEST_SERIES)
@pytest.mark.parametrize(
    "periods", [0, -1, 1, 3, -3, 10, -10]
)  # test here special cases and periods larger than number of rows of dataframe
@pytest.mark.parametrize(
    "fill_value", [None, no_default, 42]
)  # no_default is the default value, so test explicitly as well. 42 will lead to conflicts
@sql_count_checker(query_count=1)
def test_series_with_values_shift(series, periods, fill_value):

    snow_series = pd.Series(series)
    native_series = series.copy()

    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda s: s.shift(
            periods=periods,
            fill_value=pd.Timedelta(fill_value)
            if s.dtype == "timedelta64[ns]" and fill_value is not no_default
            else fill_value,
        ),
    )


@sql_count_checker(query_count=0)
def test_series_negative_axis_1():
    snow_series = pd.Series(TEST_SERIES[0])
    with pytest.raises(ValueError, match="No axis named 1 for object type Series"):
        snow_series.shift(periods=1, axis=1)


# TODO: SNOW-1023324 add tests for freq != None


@pytest.mark.parametrize(
    "params, match",
    [
        ({"periods": [1, 2, 3]}, "periods"),  # sequence of periods is unsupported
        ({"periods": [1], "suffix": "_suffix"}, "suffix"),  # suffix is unsupported
    ],
)
@sql_count_checker(query_count=0)
def test_shift_unsupported_args(params, match):
    df = pd.Series(TEST_SERIES[2])
    with pytest.raises(NotImplementedError, match=match):
        df.shift(**params).to_pandas()
