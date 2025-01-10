#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
    native_df = native_pd.DataFrame(
        [[None, 2, 3], [4, None, 6], [7, 8, None]], datetime_index
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.tz_localize(tz),
    )


@pytest.mark.parametrize(
    "tz, axis, level, copy, ambiguous, nonexistent, exception",
    [
        ("UTC", 1, None, None, "raise", "raise", NotImplementedError),
        ("UTC", "columns", None, None, "raise", "raise", NotImplementedError),
        ("UTC", 0, 1, None, "raise", "raise", NotImplementedError),
        ("UTC", 0, None, False, "raise", "raise", NotImplementedError),
        ("UTC", 0, None, True, "infer", "raise", NotImplementedError),
        ("UTC", 0, None, True, "NaT", "raise", NotImplementedError),
        (
            "UTC",
            0,
            None,
            True,
            np.array([True, True, False]),
            "raise",
            NotImplementedError,
        ),
        ("UTC", 0, None, True, "raise", "shift_forward", NotImplementedError),
        ("UTC", 0, None, True, "raise", "shift_backward", NotImplementedError),
        ("UTC", 0, None, True, "raise", "NaT", NotImplementedError),
        ("UTC", 0, None, True, "raise", pd.Timedelta("1h"), NotImplementedError),
        ("UTC", 0, None, True, "infer", "shift_forward", NotImplementedError),
        ("UTC+09:00", 0, None, None, "raise", "raise", NotImplementedError),
    ],
)
@sql_count_checker(query_count=0)
def test_tz_localize_negative(tz, axis, level, copy, ambiguous, nonexistent, exception):
    datetime_index = native_pd.DatetimeIndex(
        [
            "2014-04-04 23:56:01.000000001",
            "2015-04-03",
            pd.NaT,
        ],
    )
    native_df = native_pd.DataFrame(
        [[None, 2, 3], [4, None, 6], [7, 8, None]], datetime_index
    )
    snow_df = pd.DataFrame(native_df)

    with pytest.raises(exception):
        snow_df.tz_localize(
            tz=tz,
            axis=axis,
            level=level,
            copy=copy,
            ambiguous=ambiguous,
            nonexistent=nonexistent,
        )
