#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas._testing import assert_almost_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    create_test_series,
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def eval_snowpark_pandas_result(*args, **kwargs):
    # Inexplicably, native pandas does not propagate attrs for series.quantile but does for dataframe.quantile
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


NUMERIC_DATA = [-5, -2, -1, 0, 1, 3, 4, 5]
DATETIME_DATA = [
    pd.NaT,
    pd.Timestamp("1940-04-25"),
    pd.Timestamp("2000-10-10"),
    pd.Timestamp("2020-12-31"),
]


@pytest.mark.parametrize(
    "q, expected_union_count",
    [
        ([0.1, 0.2, 0.8], 0),
        (
            [0.2, 0.8, 0.1],
            2,
        ),  # output will not be sorted by quantile, and thus cannot use the no-union code
    ],
)
def test_quantile_basic(q, expected_union_count):
    snow_ser = pd.Series(NUMERIC_DATA)
    native_ser = native_pd.Series(NUMERIC_DATA)
    with SqlCounter(query_count=1, union_count=expected_union_count):
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            lambda df: df.quantile(q),
        )


@sql_count_checker(query_count=1)
def test_quantile_withna():
    # nans in the data do not affect the quantile
    data = [np.nan, 25, 0, 75, 50, 100, np.nan]
    snow_ser = pd.Series(data)
    native_ser = native_pd.Series(data)
    q = [0, 0.25, 0.5, 0.75, 1]
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda df: df.quantile(q))


@pytest.mark.xfail(
    reason="SNOW-1062839: PERCENTILE_DISC does not give desired behavior",
    strict=True,
    raises=AssertionError,
)
@sql_count_checker(query_count=1)
def test_quantile_nearest_negative():
    q = [0.1, 0.2, 0.8]
    snow_ser = pd.Series(NUMERIC_DATA)
    native_ser = native_pd.Series(NUMERIC_DATA)
    # The calculated value for the 0.1 quantile differs -- pandas reports -2, while
    # Snowflake reports -5. Since there are 8 elements, -2 would be at the ~0.14 percentile in
    # the input array, meaning Snowflake's result is incorrect.
    #
    # This may be a bug with PERCENTILE_DISC in Snowflake SQL (which claims to pick the nearest
    # value), as the following query will produce -5:
    #
    # SELECT PERCENTILE_DISC(0.1) WITHIN GROUP(ORDER BY column1
    #     FROM VALUES (-5), (-2), (-1), (0), (1), (3), (4), (5);
    #
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda df: df.quantile(q, interpolation="nearest"),
    )


@sql_count_checker(query_count=1)
def test_quantile_returns_scalar():
    snow_ser = pd.Series(NUMERIC_DATA)
    native_ser = native_pd.Series(NUMERIC_DATA)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda df: df.quantile(0.1),
        comparator=assert_almost_equal,
    )


@sql_count_checker(query_count=1)
def test_quantile_empty_args():
    # by default, returns the median (q=0.5)
    snow_ser = pd.Series(NUMERIC_DATA)
    native_ser = native_pd.Series(NUMERIC_DATA)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda df: df.quantile(),
        comparator=assert_almost_equal,
    )


def test_quantile_empty_ser():
    # ser.quantile() where ser is empty should still have the correct columns
    snow_ser = pd.Series([], dtype=int)
    native_ser = native_pd.Series([], dtype=int)
    with SqlCounter(query_count=1):
        # should return nan
        eval_snowpark_pandas_result(
            snow_ser,
            native_ser,
            lambda df: df.quantile(0.1),
            comparator=assert_almost_equal,
        )
    with SqlCounter(query_count=1):
        # should return series of NaNs
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda df: df.quantile([0.1, 0.2])
        )


@pytest.mark.parametrize("interpolation", ["lower", "higher", "midpoint"])
@sql_count_checker(query_count=0)
def test_quantile_unsupported_args_negative(interpolation):
    snow_ser = pd.Series(NUMERIC_DATA)
    with pytest.raises(NotImplementedError):
        snow_ser.quantile(interpolation=interpolation),


@sql_count_checker(query_count=0)
def test_quantile_datetime_negative():
    # Snowflake PERCENTILE_* functions do not operate on datetimes, so it should fail
    snow_ser = pd.Series(DATETIME_DATA)
    with pytest.raises(NotImplementedError):
        snow_ser.quantile()


@sql_count_checker(query_count=4)
def test_quantile_large():
    q = np.linspace(0, 1, 16)
    eval_snowpark_pandas_result(
        *create_test_series(range(1000)),
        lambda df: df.quantile(q, "linear"),
    )


@pytest.mark.parametrize("q", [-10.0, 8.3])
@sql_count_checker(query_count=0)
def test_quantile_out_of_0_1_negative(q):
    snow_series = pd.Series(range(10))

    with pytest.raises(
        ValueError, match=re.escape("percentiles should all be in the interval [0, 1]")
    ):
        snow_series.quantile(q)
