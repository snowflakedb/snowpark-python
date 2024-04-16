#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas


# TODO (SNOW-863790): This test file comes from pandas/tests/series/accessors/test_dt_accessor.py.
#              Pull all tests from this file to enhance the coverage for series.dt methods
class TestSeriesDatetimeValues:
    @pytest.mark.parametrize("freq", ["D", "s", "ms"])
    @sql_count_checker(query_count=9, fallback_count=1, sproc_count=1)
    def test_dt_namespace_accessor_datetime64(self, freq):
        # GH#7207, GH#11128
        # test .dt namespace accessor

        # datetimeindex
        dti = native_pd.date_range("20130101", periods=5, freq=freq)
        ser = pd.Series(dti, name="xxx")

        freq_result = ser.dt.freq
        assert freq_result == native_pd.DatetimeIndex(ser.values, freq="infer").freq

    @pytest.mark.parametrize(
        "method, dates",
        [
            ["round", ["2012-01-02", "2012-01-02", "2012-01-01"]],
            ["floor", ["2012-01-01", "2012-01-01", "2012-01-01"]],
            ["ceil", ["2012-01-02", "2012-01-02", "2012-01-02"]],
        ],
    )
    @sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
    def test_dt_round(self, method, dates):
        # round
        ser = pd.Series(
            native_pd.to_datetime(
                ["2012-01-01 13:00:00", "2012-01-01 12:01:00", "2012-01-01 08:00:00"]
            ),
            name="xxx",
        )
        result = getattr(ser.dt, method)("D")
        expected = native_pd.Series(native_pd.to_datetime(dates), name="xxx")
        assert_snowpark_pandas_equal_to_pandas(result, expected)

    @pytest.mark.parametrize(
        "date, format_string, expected",
        [
            (
                native_pd.date_range("20130101", periods=5),
                "%Y/%m/%d",
                native_pd.Series(
                    [
                        "2013/01/01",
                        "2013/01/02",
                        "2013/01/03",
                        "2013/01/04",
                        "2013/01/05",
                    ]
                ),
            ),
            (
                native_pd.date_range("2015-02-03 11:22:33.4567", periods=5),
                "%Y/%m/%d %H-%M-%S",
                native_pd.Series(
                    [
                        "2015/02/03 11-22-33",
                        "2015/02/04 11-22-33",
                        "2015/02/05 11-22-33",
                        "2015/02/06 11-22-33",
                        "2015/02/07 11-22-33",
                    ]
                ),
            ),
        ],
    )
    @sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
    def test_strftime(self, date, format_string, expected):
        # GH 10086
        ser = pd.Series(date)
        result = ser.dt.strftime(format_string)
        assert_snowpark_pandas_equal_to_pandas(result, expected)
