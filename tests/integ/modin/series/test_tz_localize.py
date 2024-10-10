#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
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
def test_tz_localize(tz):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2015-04-03",
            pd.NaT,
        ],
    )
    native_ser = native_pd.Series([1, None, 3], datetime_index)
    snow_ser = pd.Series(native_ser)

    eval_snowpark_pandas_result(
        snow_ser,
        native_ser,
        lambda s: s.tz_localize(tz),
    )


@pytest.mark.parametrize(
    "axis, level, copy, ambiguous, nonexistent, exception",
    [
        (1, None, True, "raise", "raise", "UTC", ValueError),
        ("columns", None, True, "raise", "raise", "UTC", ValueError),
        (0, 1, None, "raise", "raise", "UTC", NotImplementedError),
        (0, None, False, "raise", "raise", "UTC", NotImplementedError),
        (0, None, True, "infer", "raise", "UTC", NotImplementedError),
        (0, None, True, "NaT", "raise", "UTC", NotImplementedError),
        (
            0,
            None,
            True,
            np.array([True, True, False]),
            "raise",
            "UTC",
            NotImplementedError,
        ),
        (0, None, True, "raise", "shift_forward", "UTC", NotImplementedError),
        (0, None, True, "raise", "shift_backward", "UTC", NotImplementedError),
        (0, None, True, "raise", "NaT", "UTC", NotImplementedError),
        (0, None, True, "raise", pd.Timedelta("1h"), "UTC", NotImplementedError),
        (0, None, True, "infer", "shift_forward", "UTC", NotImplementedError),
        (0, None, True, "raise", "raise", "UTC+09:00", NotImplementedError),
    ],
)
@sql_count_checker(query_count=0)
def test_tz_localize_negative(axis, level, copy, ambiguous, nonexistent, tz, exception):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2015-04-03",
            pd.NaT,
        ],
    )
    native_ser = native_pd.Series([1, None, 3], datetime_index)
    snow_ser = pd.Series(native_ser)

    with pytest.raises(exception):
        snow_ser.tz_localize(
            tz=tz,
            axis=axis,
            level=level,
            copy=copy,
            ambiguous=ambiguous,
            nonexistent=nonexistent,
        )
