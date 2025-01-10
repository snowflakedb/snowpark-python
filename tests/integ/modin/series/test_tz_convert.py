#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest
import pytz

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

timezones = pytest.mark.parametrize(
    "tz",
    [
        None,
        # Use a subset of pytz.common_timezones containing a few timezones in each
        *[
            param_for_one_tz
            for tz in [
                "Africa/Abidjan",
                "Africa/Timbuktu",
                "America/Adak",
                "America/Yellowknife",
                "Antarctica/Casey",
                "Asia/Dhaka",
                "Asia/Manila",
                "Asia/Shanghai",
                "Atlantic/Stanley",
                "Australia/Sydney",
                "Canada/Pacific",
                "Europe/Chisinau",
                "Europe/Luxembourg",
                "Indian/Christmas",
                "Pacific/Chatham",
                "Pacific/Wake",
                "US/Arizona",
                "US/Central",
                "US/Eastern",
                "US/Hawaii",
                "US/Mountain",
                "US/Pacific",
                "UTC",
            ]
            for param_for_one_tz in (
                pytz.timezone(tz),
                tz,
            )
        ],
    ],
)


@sql_count_checker(query_count=1)
@timezones
def test_tz_convert(tz):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2015-04-03",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    native_ser = native_pd.Series([1, None, 3], datetime_index)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.tz_convert(tz),
    )


@pytest.mark.parametrize(
    "tz, axis, level, copy, exception",
    [
        ("UTC", 1, None, None, ValueError),
        ("UTC", "columns", None, None, ValueError),
        ("UTC", 0, 1, None, NotImplementedError),
        ("UTC", 0, None, False, NotImplementedError),
        ("UTC+09:00", 0, None, None, NotImplementedError),
    ],
)
@sql_count_checker(query_count=0)
def test_tz_convert_negative(tz, axis, level, copy, exception):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2015-04-03",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    native_ser = native_pd.Series([1, None, 3], datetime_index)
    snow_ser = pd.Series(native_ser)

    with pytest.raises(exception):
        snow_ser.tz_convert(
            tz=tz,
            axis=axis,
            level=level,
            copy=copy,
        )
