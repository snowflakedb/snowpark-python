#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker

SERIES_LENGTH = 32


@sql_count_checker(query_count=0)
def test_series_diff_axis_kwarg_fails_negative():
    native_ser = native_pd.Series([])
    snow_ser = pd.Series(native_ser)
    # pandas always includes the name of the method
    # when an unexpected kwarg is passed, but Snowpark
    # pandas does not.
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.diff(axis=0),
        expect_exception=True,
        expect_exception_match=r"got an unexpected keyword argument 'axis'",
        expect_exception_type=TypeError,
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=0)
def test_series_diff_invalid_periods_negative():
    native_ser = native_pd.Series()
    snow_ser = pd.Series(native_ser)

    # Unclear why pandas allows period to be a float for Series but not
    # DF.
    # Filed https://github.com/pandas-dev/pandas/issues/56607.
    with pytest.raises(ValueError, match="periods must be an integer"):
        snow_ser.diff(0.5).to_pandas()

    with pytest.raises(ValueError, match="periods must be an integer"):
        snow_ser.diff("1").to_pandas()


@pytest.mark.parametrize(
    "ser_type", [bool, int, object, "timedelta64[ns]", "datetime64[ns]"]
)
@pytest.mark.parametrize(
    "periods",
    [
        -1,
        0,
        1,
        SERIES_LENGTH / 2,
        -1 * SERIES_LENGTH / 2,
        SERIES_LENGTH - 1,
        -1 * (SERIES_LENGTH - 1),
        SERIES_LENGTH,
        -1 * SERIES_LENGTH,
    ],
    ids=[
        "with_row_after",
        "with_self",
        "with_row_before",
        "with_len(df)/2_rows_before",
        "with_len(df)/2_rows_after",
        "with_len(df)-1_rows_before",
        "with_len(df)-1_rows_after",
        "with_len(df)_rows_before",
        "with_len(df)_rows_after",
    ],
)
@sql_count_checker(query_count=1)
def test_series_diff(ser_type, periods):
    native_ser = native_pd.Series(np.arange(SERIES_LENGTH)).astype(ser_type)
    snow_ser = pd.Series(native_ser).astype(ser_type)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: ser.diff(periods=periods)
    )


@sql_count_checker(query_count=1)
def test_series_diff_mixed_ints_and_bools():
    bool_indices = [1, 3, 12, 15, 19, 20, 22, 23, 30]
    native_ser = native_pd.Series(
        [i if i not in bool_indices else bool(i) for i in range(SERIES_LENGTH)]
    )
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.diff())


@sql_count_checker(query_count=0)
def test_series_diff_string_type_negative():
    native_ser = native_pd.Series(["a", "b", "c", "d"])
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.diff(),
        expect_exception=True,
        expect_exception_type=SnowparkSQLException,
        expect_exception_match="Numeric value 'a' is not recognized",
        assert_exception_equal=False,
    )


@sql_count_checker(query_count=0)
def test_series_diff_custom_object_negative():
    class CustomObject(dict):
        def __rsub__(self, other):
            if isinstance(other, bool):
                return 3
            elif isinstance(other, type(self)):
                return "self"
            else:
                return 4

        def __sub__(self, other):
            if isinstance(other, bool):
                return -3
            elif isinstance(other, type(self)):
                return "self"
            else:
                return -4

    native_ser = native_pd.Series([CustomObject(), True, 3, CustomObject()])
    snow_ser = pd.Series(native_ser)
    with pytest.raises(
        SnowparkSQLException, match=r"Failed to cast variant value \{\} to REAL"
    ):
        snow_ser.diff().to_pandas()


@sql_count_checker(query_count=1)
def test_series_diff_variant_with_na_values():
    native_ser = native_pd.Series(
        [1, 2, True, None, 4, np.nan, 5, None, np.nan, False, np.nan, 6, None]
    )
    snow_ser = pd.Series(native_ser)
    # Vanilla pandas fails because we attempt to subtract None and a bool, but Snowpark pandas
    # succeeds.
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
        snow_ser.diff(), native_pd.Series([np.nan, 1, -1] + [np.nan] * 10)
    )


@sql_count_checker(query_count=4)
def test_series_diff_strided_row_access():
    native_series = native_pd.Series(np.arange(1000))
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series, native_series, lambda series: series.iloc[::2].diff()
    )
