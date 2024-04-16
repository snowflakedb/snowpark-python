#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime

import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result

dt_properties = pytest.mark.parametrize(
    "property_name",
    ["date", "hour", "minute", "second", "year", "month", "day", "quarter"],
)


@pytest.mark.parametrize(
    "datetime_index_value",
    [
        ["2014-04-04 23:56", "2014-07-18 21:24", "2015-11-22 22:14"],
        ["04/04/2014", "07/18/2013", "11/22/2015"],
        ["2014-04-04 23:56", pd.NaT, "2014-07-18 21:24", "2015-11-22 22:14", pd.NaT],
        [
            pd.Timestamp(2017, 1, 1, 12),
            pd.Timestamp(2018, 2, 1, 10),
            pd.Timestamp(2000, 2, 1, 10),
        ],
    ],
)
@sql_count_checker(query_count=1)
def test_date(datetime_index_value):
    native_ser = native_pd.Series(native_pd.DatetimeIndex(datetime_index_value))
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(snow_ser, native_ser, lambda ser: ser.dt.date)


@dt_properties
@sql_count_checker(query_count=1)
def test_dt_property_with_tz(property_name):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56",
            "2014-07-18 21:24",
            "2015-11-22 22:14",
            "2015-11-23",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    native_ser = native_pd.Series(datetime_index)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.dt, property_name)
    )


@dt_properties
@pytest.mark.parametrize("freq", ["d", "h", "min", "s", "y", "m", "D", "3m"])
@sql_count_checker(query_count=1)
def test_dt_properties(property_name, freq):
    native_ser = native_pd.Series(
        native_pd.date_range(start="2021-01-01", periods=5, freq=freq),
        index=[2, 6, 7, 8, 11],
        name="test",
    )
    native_ser.iloc[2] = native_pd.NaT
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: getattr(ser.dt, property_name)
    )


@pytest.mark.parametrize(
    "data, data_type",
    [
        ([1, 2, 3, 4, 5], "int"),
        ([1.1, 2.0, None, 4.0, 5.3], "float"),
        (["a", "b", "c", "dd"], None),
        (
            [
                datetime.date(2019, 12, 4),
                datetime.date(2019, 12, 5),
                datetime.date(2019, 12, 6),
            ],
            None,
        ),
        (
            [
                datetime.time(11, 12, 13),
                datetime.time(12, 21, 5),
                datetime.time(5, 2, 6),
            ],
            None,
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_dt_invalid_dtypes(data, data_type):
    native_ser = native_pd.Series(data)
    if data_type:
        native_ser.astype(data_type)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda ser: ser.dt,
        expect_exception=True,
        expect_exception_match="Can only use .dt accessor with datetimelike values",
    )
