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
    native_df = native_pd.DataFrame(
        [[None, 2, 3], [4, None, 6], [7, 8, None]], datetime_index
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.tz_convert(tz),
    )


@pytest.mark.parametrize(
    "axis, level, copy, tz, exception",
    [
        (1, None, None, "UTC", NotImplementedError),
        ("columns", None, None, "UTC", NotImplementedError),
        (0, 1, None, "UTC", NotImplementedError),
        (0, None, False, "UTC", NotImplementedError),
        (0, None, None, "UTC+09:00", NotImplementedError),
    ],
)
@sql_count_checker(query_count=0)
def test_tz_convert_negative(axis, level, copy, tz, exception):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2015-04-03",
            pd.NaT,
        ],
        tz="US/Eastern",
    )
    native_df = native_pd.DataFrame(
        [[None, 2, 3], [4, None, 6], [7, 8, None]], datetime_index
    )
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(exception):
        snow_df.tz_convert(
            tz=tz,
            axis=axis,
            level=level,
            copy=copy,
        )
