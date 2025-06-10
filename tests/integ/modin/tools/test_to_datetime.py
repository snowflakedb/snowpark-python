#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

""" test to_datetime """

import calendar
import locale
import re
from datetime import datetime, timedelta, timezone

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pandas._testing as tm
import pytest
import pytz
from modin.pandas import NaT, Series, Timestamp, to_datetime
from pandas import DatetimeIndex
from pandas.core.arrays import DatetimeArray
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import (
    SnowparkFetchDataException,
    SnowparkSQLException,
)
from tests.integ.modin.utils import (
    assert_index_equal,
    assert_series_equal,
    assert_snowpark_pandas_equal_to_pandas,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture(params=[True])
def cache(request):
    """
    cache keyword to pass to to_datetime.
    """
    return request.param


@pytest.fixture(params=[True, False])
def utc(request):
    """
    utc keyword to pass to to_datetime.
    """
    return request.param


class TestTimeConversionFormats:
    @pytest.mark.modin_sp_precommit
    @pytest.mark.parametrize("readonly", [True, False])
    @sql_count_checker(query_count=2)
    def test_to_datetime_readonly(self, readonly):
        # GH#34857
        arr = np.array([], dtype=object)
        if readonly:
            arr.setflags(write=False)
        result = to_datetime(arr)
        expected = pd.DatetimeIndex([])
        assert_index_equal(result, expected)

    @pytest.mark.parametrize("box", [Series, pd.Index])
    @pytest.mark.parametrize(
        "format, expected",
        [
            [
                "%d/%m/%Y",
                [Timestamp("20000101"), Timestamp("20000201"), Timestamp("20000301")],
            ],
            [
                "%m/%d/%Y",
                [Timestamp("20000101"), Timestamp("20000102"), Timestamp("20000103")],
            ],
        ],
    )
    @sql_count_checker(query_count=4)
    def test_to_datetime_format(self, cache, box, format, expected):
        values = box(["1/1/2000", "1/2/2000", "1/3/2000"])
        result = to_datetime(values, format=format, cache=cache)
        expected = box(expected)
        if box is Series:
            assert_series_equal(result, expected)
        else:
            assert_index_equal(result, expected)

        # cache values is ignored at Snowpark pandas so only test here to make sure it works as well
        result = to_datetime(values, format=format, cache=False)
        if box is Series:
            assert_series_equal(result, expected)
        else:
            assert_index_equal(result, expected)

    @pytest.mark.parametrize(
        "arg, expected, format",
        [
            ["1/1/2000", "20000101", "%d/%m/%Y"],
            ["1/1/2000", "20000101", "%m/%d/%Y"],
            ["1/2/2000", "20000201", "%d/%m/%Y"],
            ["1/2/2000", "20000102", "%m/%d/%Y"],
            ["1/3/2000", "20000301", "%d/%m/%Y"],
            ["1/3/2000", "20000103", "%m/%d/%Y"],
        ],
    )
    @sql_count_checker(query_count=0)
    def test_to_datetime_format_scalar(self, cache, arg, expected, format):
        result = to_datetime(arg, format=format, cache=cache)
        expected = Timestamp(expected)
        assert result == expected

    @pytest.mark.parametrize(
        "arg, format",
        [
            ["1/1/2000", "%d/%w/%Y"],
        ],
    )
    @sql_count_checker(query_count=0)
    def test_to_datetime_format_unimplemented(self, cache, arg, format):
        with pytest.raises(NotImplementedError):
            assert to_datetime(
                pd.Index([arg]), format=format, cache=cache
            ) == native_pd.to_datetime(arg, format=format, cache=cache)

    @pytest.mark.parametrize(
        "arg, format",
        [
            ["1-1-2000", "%d/%m/%Y"],
        ],
    )
    @sql_count_checker(query_count=0)
    def test_to_datetime_format_not_match(self, cache, arg, format):
        with pytest.raises(
            SnowparkSQLException,
            match=f"Can't parse '{arg}' as timestamp with format 'DD/MM/YYYY'",
        ):
            to_datetime(pd.Index([arg]), format=format, cache=cache).to_pandas()

    @sql_count_checker(query_count=2, udf_count=0)
    def test_to_datetime_format_YYYYMMDD(self, cache):
        data = [19801222, 19801222] + [19810105] * 5
        ser = Series(data)
        native_ser_str = native_pd.Series(data).apply(str)
        expected = native_pd.Series([Timestamp(x) for x in native_ser_str])

        result = to_datetime(ser, format="%Y%m%d", cache=cache)
        assert_snowpark_pandas_equal_to_pandas(result, expected)

        ser = Series(native_ser_str)
        result = to_datetime(ser, format="%Y%m%d", cache=cache)
        assert_snowpark_pandas_equal_to_pandas(result, expected)

    @sql_count_checker(query_count=1)
    def test_to_datetime_format_YYYYMMDD_with_nat(self):
        ser = Series([19801222, 19801222, np.nan] + [19810105] * 5)
        # with NaT
        expected = native_pd.Series(
            [Timestamp("19801222"), Timestamp("19801222"), pd.NaT]
            + [Timestamp("19810105")] * 5
        )
        result = to_datetime(ser, format="%Y%m%d")
        assert_snowpark_pandas_equal_to_pandas(result, expected)

    @pytest.mark.parametrize(
        "nat_str", ["nan", "NAN", "NaN", "nAn", "nAt", "nat", "NaT", "NAT"]
    )
    @sql_count_checker(query_count=1)
    def test_to_datetime_format_YYYYMMDD_with_str_nat(self, nat_str):
        ser2 = Series(["19801222", "19801222", nat_str] + ["19810105"] * 5)
        result = to_datetime(ser2, format="%Y%m%d")

        expected = native_pd.Series(
            [Timestamp("19801222"), Timestamp("19801222"), pd.NaT]
            + [Timestamp("19810105")] * 5
        )
        assert_snowpark_pandas_equal_to_pandas(result, expected)

    @pytest.mark.xfail(
        strict=True,
        reason="SNOW-1170304: out of bounds datetime convert to datetime64[us] instead of raising error",
    )
    @sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
    def test_to_datetime_format_YYYYMMDD_ignore(self, cache):
        out_of_bound_sample = [20121231, 20141231, 99991231]
        # When errors="ignore", the values are transparently passed through
        expected = native_pd.Series(
            [datetime(2012, 12, 31), datetime(2014, 12, 31), datetime(9999, 12, 31)],
            dtype=object,
        )
        assert_series_equal(
            native_pd.to_datetime(
                native_pd.Series(out_of_bound_sample),
                format="%Y%m%d",
                errors="ignore",
                cache=cache,
            ),
            expected,
        )

        snow = to_datetime(
            pd.Series(out_of_bound_sample),
            format="%Y%m%d",
            errors="ignore",
            cache=cache,
        )
        # TODO SNOW-894362 Snowflake Python Connector does not support pulling out of bound datetime. If a datetime is
        # out of bound, pandas use Python datetime object to hold the value instead of numpy datetime64[ns]. The
        # Snowflake Python Connector does not support this today. It will raise an error.
        with pytest.raises(
            SnowparkFetchDataException,
            match=re.escape(
                "Casting from timestamp[us] to timestamp[ns] would result in out of bounds timestamp"
            ),
        ):
            snow.to_pandas()

    @sql_count_checker(query_count=2)
    def test_to_datetime_format_YYYYMMDD_coercion(self, cache):
        # coercion
        # GH 7930
        ser = Series([20121231, 20141231, 99991231])
        result = to_datetime(ser, format="%Y%m%d", errors="coerce", cache=cache)
        expected = Series(["20121231", "20141231", "NaT"], dtype="M8[ns]")
        assert_series_equal(result, expected)

    @pytest.mark.parametrize(
        "input_s",
        [
            # Null values with Strings
            ["19801222", "20010112", None],
            ["19801222", "20010112", np.nan],
            ["19801222", "20010112", "NaT"],
            # Null values with Integers
            [19801222, 20010112, None],
            [19801222, 20010112, np.nan],
            [19801222, 20010112, NaT],
            [19801222, 20010112, "NaT"],
        ],
    )
    @sql_count_checker(query_count=2)
    def test_to_datetime_format_YYYYMMDD_with_none(self, input_s):
        # GH 30011
        # format='yyyymmdd'
        # with None
        expected = pd.DatetimeIndex([Timestamp("19801222"), Timestamp("20010112"), NaT])
        result = to_datetime(input_s, format="%Y%m%d")
        assert_index_equal(result, expected)

    @pytest.mark.parametrize(
        "input, expected",
        [
            # NaN before strings with invalid date values
            [
                ["19801222", np.nan, "20010012", "10019999"],
                [Timestamp("19801222"), np.nan, np.nan, np.nan],
            ],
            # NaN after strings with invalid date values
            [
                ["19801222", "20010012", "10019999", np.nan],
                [Timestamp("19801222"), np.nan, np.nan, np.nan],
            ],
            # NaN before integers with invalid date values
            [
                [20190813, np.nan, 20010012, 20019999],
                [Timestamp("20190813"), np.nan, np.nan, np.nan],
            ],
            # NaN after integers with invalid date values
            [
                [20190813, 20010012, np.nan, 20019999],
                [Timestamp("20190813"), np.nan, np.nan, np.nan],
            ],
        ],
    )
    @sql_count_checker(query_count=2)
    def test_to_datetime_format_YYYYMMDD_overflow(self, input, expected):
        # GH 25512
        # format='yyyymmdd', errors='coerce'
        result = to_datetime(Series(input), format="%Y%m%d", errors="coerce")
        assert_series_equal(result, Series(expected))

    @pytest.mark.parametrize(
        "data, format, expected",
        [
            ([pd.NA], "yyyymmdd%H%M%S", DatetimeIndex(["NaT"])),
            ([pd.NA], None, DatetimeIndex(["NaT"])),
            (
                [pd.NA, "20210202202020"],
                "yyyymmdd%H%M%S",
                DatetimeIndex(["NaT", "2021-02-02 20:20:20"]),
            ),
            (["201010", pd.NA], "%d%m%y", DatetimeIndex(["2010-10-20", "NaT"])),
            (
                ["201010", pd.NA],
                None,
                DatetimeIndex(["1970-01-03 07:50:10", "NaT"]),
            ),  # different from pandas "2010-10-20"
            ([None, np.nan, pd.NA], None, DatetimeIndex(["NaT", "NaT", "NaT"])),
            ([None, np.nan, pd.NA], "%Y%m%d", DatetimeIndex(["NaT", "NaT", "NaT"])),
        ],
    )
    @sql_count_checker(query_count=2)
    def test_to_datetime_with_NA(self, data, format, expected):
        # GH#42957
        result = to_datetime(pd.Index(data), format=format)
        assert_index_equal(result, pd.DatetimeIndex(expected))

    @sql_count_checker(query_count=1, udf_count=0)
    def test_to_datetime_format_integer_year_only(self, cache):
        # GH 10178
        data = [2000, 2001, 2002]
        ser = Series(data)
        native_ser = native_pd.Series(data)
        expected = native_pd.Series([Timestamp(x) for x in native_ser.apply(str)])

        result = to_datetime(ser, format="%Y", cache=cache)
        assert_series_equal(result, expected, check_index_type=False)

    @sql_count_checker(query_count=1, udf_count=0)
    def test_to_datetime_format_integer_year_month(self, cache):
        data = [200001, 200105, 200206]
        ser = Series(data)

        native_ser = native_pd.Series(data)
        expected = native_pd.Series(
            [Timestamp(x[:4] + "-" + x[4:]) for x in native_ser.apply(str)]
        )
        result = to_datetime(ser, format="%Y%m", cache=cache)
        assert_series_equal(result, expected, check_index_type=False)

    @sql_count_checker(query_count=0)
    def test_to_datetime_format_microsecond(self, cache):
        month_abbr = calendar.month_abbr[4]
        val = f"01-{month_abbr}-2011 00:00:01.978"

        format = "%d-%b-%Y %H:%M:%S.%f"
        result = to_datetime(val, format=format, cache=cache)
        exp = np.datetime64(datetime.strptime(val, format))
        assert result == exp

    @pytest.mark.parametrize(
        "value, format, dt",
        [
            ["01/10/2010 15:20", "%m/%d/%Y %H:%M", Timestamp("2010-01-10 15:20")],
            ["01/10/2010 05:43", "%m/%d/%Y %I:%M", Timestamp("2010-01-10 05:43")],
            [
                "01/10/2010 13:56:01",
                "%m/%d/%Y %H:%M:%S",
                Timestamp("2010-01-10 13:56:01"),
            ],
            # The 3 tests below are locale-dependent.
            # They pass, except when the machine locale is zh_CN or it_IT .
            pytest.param(
                "01/10/2010 08:14 PM",
                "%m/%d/%Y %I:%M %p",
                Timestamp("2010-01-10 20:14"),
                marks=pytest.mark.xfail(
                    locale.getlocale()[0] in ("zh_CN", "it_IT"),
                    reason="fail on a CI build with LC_ALL=zh_CN.utf8/it_IT.utf8",
                    strict=False,
                ),
            ),
            pytest.param(
                "01/10/2010 07:40 AM",
                "%m/%d/%Y %I:%M %p",
                Timestamp("2010-01-10 07:40"),
                marks=pytest.mark.xfail(
                    locale.getlocale()[0] in ("zh_CN", "it_IT"),
                    reason="fail on a CI build with LC_ALL=zh_CN.utf8/it_IT.utf8",
                    strict=False,
                ),
            ),
            pytest.param(
                "01/10/2010 09:12:56 AM",
                "%m/%d/%Y %I:%M:%S %p",
                Timestamp("2010-01-10 09:12:56"),
                marks=pytest.mark.xfail(
                    locale.getlocale()[0] in ("zh_CN", "it_IT"),
                    reason="fail on a CI build with LC_ALL=zh_CN.utf8/it_IT.utf8",
                    strict=False,
                ),
            ),
        ],
    )
    @sql_count_checker(query_count=1)
    def test_to_datetime_format_time(self, cache, value, format, dt):
        assert (
            to_datetime(pd.Index([value]), format=format, cache=cache).to_pandas() == dt
        )

    @sql_count_checker(query_count=0)
    def test_to_datetime_with_non_exact_unimplemented(self, cache):
        # GH 10834
        # 8904
        # exact kw
        ser = Series(
            ["19MAY11", "foobar19MAY11", "19MAY11:00:00:00", "19MAY11 00:00:00Z"]
        )
        with pytest.raises(NotImplementedError):
            to_datetime(ser, format="%d%b%y", exact=False, cache=cache)

    @pytest.mark.parametrize(
        "arg",
        [
            "2012-01-01 09:00:00.000000001",
            "2012-01-01 09:00:00.000001",
            "2012-01-01 09:00:00.001",
            "2012-01-01 09:00:00.001000",
            "2012-01-01 09:00:00.001000000",
        ],
    )
    @sql_count_checker(query_count=1, join_count=1)
    def test_parse_nanoseconds_with_formula(self, cache, arg):
        arg = pd.Index([arg])
        # GH8989
        # truncating the nanoseconds when a format was provided
        expected = to_datetime(arg, cache=cache)
        result = to_datetime(arg, format="%Y-%m-%d %H:%M:%S.%f", cache=cache)
        assert result == expected

    @pytest.mark.parametrize(
        "value,fmt,expected",
        [
            ["2009324", "%Y%W%w", Timestamp("2009-08-13")],
            ["2013020", "%Y%U%w", Timestamp("2013-01-13")],
        ],
    )
    @sql_count_checker(query_count=0)
    def test_to_datetime_format_weeks(self, value, fmt, expected, cache):
        with pytest.raises(NotImplementedError):
            assert (
                to_datetime(pd.Index([value]), format=fmt, cache=cache).to_pandas()[0]
                == expected
            )

    @pytest.mark.parametrize(
        "fmt,dates,expected_dates",
        [
            [
                "%Y-%m-%d %H:%M:%S%z",
                ["2010-01-01 12:00:00+0100"] * 2,
                [Timestamp("2010-01-01 12:00:00", tzinfo=pytz.FixedOffset(60))] * 2,
            ],
            [
                "%Y-%m-%d %H:%M:%S %z",
                ["2010-01-01 12:00:00 +0100"] * 2,
                [Timestamp("2010-01-01 12:00:00", tzinfo=pytz.FixedOffset(60))] * 2,
            ],
            [
                "%Y-%m-%d %H:%M:%S %z",
                ["2010-01-01 12:00:00 +0100", "2010-01-01 12:00:00 -0100"],
                [
                    Timestamp("2010-01-01 12:00:00", tzinfo=pytz.FixedOffset(60)),
                    Timestamp("2010-01-01 12:00:00", tzinfo=pytz.FixedOffset(-60)),
                ],
            ],
            [
                "%Y-%m-%d %H:%M:%S %z",
                ["2010-01-01 12:00:00 Z", "2010-01-01 12:00:00 Z"],
                [
                    Timestamp(
                        "2010-01-01 12:00:00", tzinfo=pytz.FixedOffset(0)
                    ),  # pytz coerces to UTC
                    Timestamp("2010-01-01 12:00:00", tzinfo=pytz.FixedOffset(0)),
                ],
            ],
        ],
    )
    @sql_count_checker(query_count=1)
    def test_to_datetime_parse_tzname_or_tzoffset(self, fmt, dates, expected_dates):
        # GH 13486
        result = to_datetime(dates, format=fmt).to_list()
        # with SqlCounter(query_count=1):
        tm.assert_equal(result, expected_dates)

    @pytest.mark.parametrize(
        "fmt,dates,expected_dates",
        [
            [
                "%Y-%m-%d %H:%M:%S %Z",  # %Z is not supported
                ["2010-01-01 12:00:00 UTC"] * 2,
                [Timestamp("2010-01-01 12:00:00", tz="UTC")] * 2,
            ],
            [
                "%Y-%m-%d %H:%M:%S %Z",  # %Z is not supported
                [
                    "2010-01-01 12:00:00 UTC",
                    "2010-01-01 12:00:00 GMT",
                    "2010-01-01 12:00:00 US/Pacific",
                ],
                [
                    Timestamp("2010-01-01 12:00:00", tz="UTC"),
                    Timestamp("2010-01-01 12:00:00", tz="GMT"),
                    Timestamp("2010-01-01 12:00:00", tz="US/Pacific"),
                ],
            ],
        ],
    )
    @sql_count_checker(query_count=0)
    def test_to_datetime_parse_tzname_or_tzoffset_fallback(
        self, fmt, dates, expected_dates
    ):
        # GH 13486
        with pytest.raises(NotImplementedError):
            to_datetime(pd.Index(dates), format=fmt).to_list()

    @sql_count_checker(query_count=4)
    def test_to_datetime_parse_tzname_or_tzoffset_different_tz_to_utc(self):
        # GH 32792
        dates = [
            "2010-01-01 12:00:00 +0100",
            "2010-01-01 12:00:00 -0100",
            "2010-01-01 12:00:00 +0300",
            "2010-01-01 12:00:00 +0400",
        ]
        expected_dates = [
            Timestamp("2010-01-01 11:00:00+00:00"),
            Timestamp("2010-01-01 13:00:00+00:00"),
            Timestamp("2010-01-01 09:00:00+00:00"),
            Timestamp("2010-01-01 08:00:00+00:00"),
        ]
        fmt = "%Y-%m-%d %H:%M:%S %z"

        result = to_datetime(dates, format=fmt, utc=True)
        expected = pd.DatetimeIndex(expected_dates)
        assert_index_equal(result, expected)
        result2 = to_datetime(dates, utc=True)
        assert_index_equal(result2, expected)

    @pytest.mark.parametrize(
        "offset", ["+0", "-1foo", "UTCbar", ":10", "+01:000:01", ""]
    )
    @sql_count_checker(query_count=0)
    def test_to_datetime_parse_timezone_malformed(self, offset):
        fmt = "%Y-%m-%d %H:%M:%S %z"
        date = "2010-01-01 12:00:00 " + offset

        # pandas will raise a VauleError with error msg = "does not match format|unconverted data remains"
        with pytest.raises(
            SnowparkSQLException,
            match="Can't parse|as timestamp with format 'YYYY-MM-DD HH24:MI:SS TZHTZM'",
        ):
            to_datetime(pd.Index([date]), format=fmt).to_pandas()

    @sql_count_checker(query_count=0)
    def test_to_datetime_parse_timezone_keeps_name(self):
        # GH 21697
        fmt = "%Y-%m-%d %H:%M:%S %z"
        arg = pd.Index(["2010-01-01 12:00:00 Z"], name="foo")
        result = to_datetime(arg, format=fmt)
        assert result.name == arg.name


class TestToDatetime:
    @sql_count_checker(query_count=3)
    def test_to_datetime_mixed_datetime_and_string(self):
        d1 = datetime(2020, 1, 1, 17, tzinfo=timezone(-timedelta(hours=1)))
        d2 = datetime(2020, 1, 1, 18, tzinfo=timezone(-timedelta(hours=1)))
        res = to_datetime(pd.Index(["2020-01-01 17:00:00 -0100", d2]))
        # The input will become a series with variant type and the timezone is unaware by the Snowflake engine, so the
        # result ignores the timezone by default
        expected = native_pd.DatetimeIndex(
            [datetime(2020, 1, 1, 17), datetime(2020, 1, 1, 18)]
        )
        assert_index_equal(res, expected)
        # Set utc=True to make sure timezone aware in to_datetime
        res = to_datetime(pd.Index(["2020-01-01 17:00:00 -0100", d2]), utc=True)
        expected = pd.DatetimeIndex([d1, d2], tz="UTC")
        assert_index_equal(res, expected)

    @pytest.mark.parametrize(
        "tz",
        [
            None,
            pytest.param("US/Central"),
        ],
    )
    @sql_count_checker(query_count=2)
    def test_to_datetime_dtarr(self, tz):
        # DatetimeArray
        dti = native_pd.date_range("1965-04-03", periods=19, freq="2W", tz=tz)
        arr = DatetimeArray(dti)
        # Use assert_series_equal to ignore timezone difference in dtype.
        assert_series_equal(
            Series(to_datetime(arr)),
            Series(arr),
            check_dtype=False,
        )

    @sql_count_checker(query_count=0)
    def test_to_datetime_pydatetime(self):
        actual = to_datetime(pd.Index([datetime(2008, 1, 15)]))
        assert actual == np.datetime64(datetime(2008, 1, 15))

    @pytest.mark.parametrize(
        "dt", [np.datetime64("2000-01-01"), np.datetime64("2000-01-02")]
    )
    @sql_count_checker(query_count=1)
    def test_to_datetime_dt64s(self, cache, dt):
        assert to_datetime(pd.Index([dt]), cache=cache)[0] == Timestamp(dt)

    @pytest.mark.parametrize(
        "sample",
        [
            {"year": [2015, 2016], "month": [2, 3], "day": [4, 5]},  # minimal keys
            {
                "yEaR": [2015, 2016],  # case insensitive
                "month": [2, 3],
                "day": [4, 1],
                "minute": [1, 2],
                "second": [0, -1],
                "ms": [1, 2],
                "us": [-1, 2],
                "ns": [1, 2],
            },  # full keys
            {
                "yEaR": [2015, 2016],
                "month": [2, 3],
                "day": [4, 1],
                "minute": [1, 2],
                "second": [0, -1],
                "ms": [1, 2],
            },  # ms, us, ns is optinal
        ],
    )
    @sql_count_checker(query_count=1)
    def test_to_datetime_df(self, sample):
        eval_snowpark_pandas_result(
            pd.DataFrame(sample),
            native_pd.DataFrame(sample),
            lambda df: pd.to_datetime(df)
            if isinstance(df, pd.DataFrame)
            else native_pd.to_datetime(df),
        )

    @pytest.mark.parametrize(
        "sample",
        [
            {"year": [2015, 2016], "month": [2, 3], "day": [4, 5]},  # minimal keys
            {"years": [2015, 2016], "months": [2, 3], "days": [4, 5]},  # plurals OK
            {
                "years": [2015, 2016],
                "year": [2000, 2001],
                "months": [2, 3],
                "days": [4, 5],
                "seconds": [300, 400],
                "second": [700, 800],
                "s": [100, 200],
            },  # if same key has plural/abbreviated form, use the last specified key
            {
                "yEaR": [2015, 2016],  # case insensitive
                "month": [2, 3],
                "day": [4, 1],
                "minute": [1, 2],
                "second": [0, -1],
                "ms": [1, 2],
                "us": [-1, 2],
                "ns": [1, 2],
            },  # full keys
            {
                "yEaR": [2015, 2016],
                "month": [2, 3],
                "day": [4, 1],
                "minute": [1, 2],
                "second": [0, -1],
                "ms": [1, 2],
            },  # ms, us, ns is optional
        ],
    )
    @sql_count_checker(query_count=1)
    def test_to_datetime_dict(self, sample):
        assert_snowpark_pandas_equal_to_pandas(
            pd.to_datetime(sample),
            native_pd.to_datetime(sample),
        )

    @pytest.mark.parametrize(
        "sample",
        [
            {
                "year": ["2015", "2016"],
                "month": [2.0, 3.1],
                "day": [4.9, 0],
            },  # non int types
        ],
    )
    @sql_count_checker(query_count=0)
    def test_to_datetime_df_fallback(self, sample):
        with pytest.raises(NotImplementedError):
            eval_snowpark_pandas_result(
                pd.DataFrame(sample),
                native_pd.DataFrame(sample),
                lambda df: pd.to_datetime(df)
                if isinstance(df, pd.DataFrame)
                else native_pd.to_datetime(df),
            )

    @pytest.mark.parametrize(
        "origin,unit",
        [
            (pd.Timestamp("1960-01-01"), "D"),
            (pd.Timestamp("1960-01-01"), "s"),
            (2000, "ns"),
            (1e6, "us"),
            (1e3, "ms"),
            (1e6, "ms"),
            (1e6, "s"),
            (1000, "D"),
            ("2000-01-01", "D"),
            ("2000-01-01", "us"),
        ],
    )
    @sql_count_checker(query_count=1)
    def test_to_datetime_origin(self, origin, unit):
        sample = [1, 2, 3]
        eval_snowpark_pandas_result(
            pd.Series(sample),
            native_pd.Series(sample),
            lambda df: pd.to_datetime(df, unit=unit, origin=origin)
            if isinstance(df, pd.Series)
            else native_pd.to_datetime(df, unit=unit, origin=origin),
        )

    @sql_count_checker(query_count=0)
    def test_to_datetime_origin_negative(self):
        # `origin` argument raises an error for non-numeric dataframes
        sample_dict = {"year": [2000], "month": [3], "day": [1]}
        origin = 1e9
        eval_snowpark_pandas_result(
            pd.DataFrame(sample_dict),
            native_pd.DataFrame(sample_dict),
            lambda df: pd.to_datetime(df, origin=origin)
            if isinstance(df, pd.DataFrame)
            else native_pd.to_datetime(df, origin=origin),
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match="arg must be a string, datetime, list, tuple, 1-d array, or Series",
        )

        sample = [1, 2, 3]
        origin_complex = 1j
        # complex values are invalid
        eval_snowpark_pandas_result(
            pd.Series(sample),
            native_pd.Series(sample),
            lambda df: pd.to_datetime(df, origin=origin_complex)
            if isinstance(df, pd.Series)
            else native_pd.to_datetime(df, origin=origin_complex),
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                "Cannot convert input [1j] of type <class 'complex'> to Timestamp"
            ),
        )

    @sql_count_checker(query_count=0)
    def test_to_datetime_df_negative(self):
        sample_empty = {}
        eval_snowpark_pandas_result(
            pd.DataFrame(sample_empty),
            native_pd.DataFrame(sample_empty),
            lambda df: pd.to_datetime(df)
            if isinstance(df, pd.DataFrame)
            else native_pd.to_datetime(df),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match=re.escape(
                "to assemble mappings requires at least that [year, month, day] be specified: [day,month,year] is missing"
            ),
        )

        sample_wo_year = {"month": [2, 3], "day": [4, 5]}
        eval_snowpark_pandas_result(
            pd.DataFrame(sample_wo_year),
            native_pd.DataFrame(sample_wo_year),
            lambda df: pd.to_datetime(df)
            if isinstance(df, pd.DataFrame)
            else native_pd.to_datetime(df),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match=re.escape(
                "to assemble mappings requires at least that [year, month, day] be specified: [year] is missing"
            ),
        )

        sample_wo_year_month_day = {"ns": [2, 3], "ms": [4, 5]}
        eval_snowpark_pandas_result(
            pd.DataFrame(sample_wo_year_month_day),
            native_pd.DataFrame(sample_wo_year_month_day),
            lambda df: pd.to_datetime(df)
            if isinstance(df, pd.DataFrame)
            else native_pd.to_datetime(df),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match=re.escape(
                "to assemble mappings requires at least that [year, month, day] be specified: [day,month,year] is missing"
            ),
        )

        sample_extra = {
            "year": [2015, 2016],
            "month": [2, 3],
            "day": [4, 5],
            "xxx": [4, 5],
        }
        eval_snowpark_pandas_result(
            pd.DataFrame(sample_extra),
            native_pd.DataFrame(sample_extra),
            lambda df: pd.to_datetime(df)
            if isinstance(df, pd.DataFrame)
            else native_pd.to_datetime(df),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match=re.escape(
                "extra keys have been passed to the datetime assemblage: [xxx]"
            ),
        )

        sample_2d_array = [[1]]
        # we don't match pandas here since pandas raises an AttributeError: 'int' object has no attribute 'lower'
        with pytest.raises(
            ValueError,
            match=re.escape(
                "extra keys have been passed to the datetime assemblage: [0]"
            ),
        ):
            pd.to_datetime(pd.DataFrame(sample_2d_array))

    @pytest.mark.parametrize(
        "sample",
        [
            {"arg": 86400, "unit": "D"},
            {"arg": 1490195805, "unit": "s"},
            {"arg": 1490195805433, "unit": "ms"},
            {"arg": 1490195805433502, "unit": "us"},
            {"arg": 1490195805433502912, "unit": "ns"},
        ],
    )
    @sql_count_checker(query_count=1, join_count=0)
    def test_to_datetime_unit(self, sample):
        assert pd.to_datetime(pd.Index([sample["arg"]]), unit=sample["unit"])[
            0
        ] == native_pd.to_datetime(sample["arg"], unit=sample["unit"])

    @sql_count_checker(query_count=0)
    def test_to_datetime_unit_negative(self):
        invalid_unit = "NS"
        eval_snowpark_pandas_result(
            pd.Series([1490195805, 1490195805]),
            native_pd.Series([1490195805, 1490195805]),
            lambda df: pd.to_datetime(df, unit=invalid_unit)
            if isinstance(df, pd.Series)
            else native_pd.to_datetime(df, unit=invalid_unit),
            expect_exception=True,
            expect_exception_type=ValueError,
            expect_exception_match="Unrecognized unit NS",
            assert_exception_equal=False,  # pandas may raise either ValueError or OutOfBoundsDatetime
        )

    @sql_count_checker(query_count=0)
    def test_none(self):
        assert pd.to_datetime(None) == native_pd.to_datetime(None)

    @sql_count_checker(query_count=0)
    @pytest.mark.parametrize(
        "input_data,dtype_description_in_error",
        [
            param([True, False], "bool", id="bool"),
            param(pd.Timedelta(1), "timedelta64[ns]", id="timedelta"),
        ],
    )
    def test_invalid_input_type(self, input_data, dtype_description_in_error):
        eval_snowpark_pandas_result(
            *create_test_series(input_data),
            lambda df: pd.to_datetime(df)
            if isinstance(df, pd.Series)
            else native_pd.to_datetime(df),
            expect_exception=True,
            expect_exception_type=TypeError,
            expect_exception_match=re.escape(
                f"dtype {dtype_description_in_error} cannot be converted to datetime64[ns]"
            ),
        )

    @pytest.mark.xfail(
        strict=True,
        reason="SNOW-1170304: out of bounds datetime convert to datetime64[us] instead of raising error",
    )
    @sql_count_checker(query_count=10, fallback_count=1, sproc_count=1)
    def test_out_of_bound(self):
        sample = ["13000101"]
        # both pandas and Snowpark pandas raise exception. pandas will raise an out of bound datetime exception while
        # Snowpark pandas raises SnowparkFetchDataException because Snowflake backend supports those out of bound timestamps
        # but Snowflake Python connector that failed to fetch it and build a pandas dataframe.
        eval_snowpark_pandas_result(
            pd.Series(sample),
            native_pd.Series(sample),
            lambda df: pd.to_datetime(df, format="%Y%m%d")
            if isinstance(df, pd.Series)
            else native_pd.to_datetime(df, format="%Y%m%d"),
            expect_exception=True,
            expect_exception_type=SnowparkFetchDataException,
            expect_exception_match=re.escape("Failed to fetch a pandas Dataframe"),
            assert_exception_equal=False,
        )

        # change errors from raises to coerce and both system should return NaT
        eval_snowpark_pandas_result(
            pd.Series(sample),
            native_pd.Series(sample),
            lambda df: pd.to_datetime(df, format="%Y%m%d", errors="coerce")
            if isinstance(df, pd.Series)
            else native_pd.to_datetime(df, format="%Y%m%d", errors="coerce"),
        )

        # change errors to "ignore":
        # based on pandas' doc: "If a date does not meet the timestamp limitations, passing errors='ignore' will return
        # the original input instead of raising any exception.", so this should return the original value but here it
        # shows a bug which returns a datetime
        assert native_pd.to_datetime(
            native_pd.Series(sample), format="%Y%m%d", errors="ignore"
        ).to_list()[0] == datetime(year=1300, month=1, day=1)
        # if we change the sample to below case, it returns original values
        sample = ["13000101", "abc"]
        eval_snowpark_pandas_result(
            pd.Series(sample),
            native_pd.Series(sample),
            lambda df: pd.to_datetime(df, format="%Y%m%d", errors="ignore")
            if isinstance(df, pd.Series)
            else native_pd.to_datetime(df, format="%Y%m%d", errors="ignore"),
        )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("samples", [["0101", "20000101"], ["20100101", "20000101"]])
def test_errors_ignore(samples):
    eval_snowpark_pandas_result(
        pd.Series(samples),
        native_pd.Series(samples),
        lambda df: pd.to_datetime(df, format="%Y%m%d", errors="ignore")
        if isinstance(df, pd.Series)
        else native_pd.to_datetime(df, format="%Y%m%d", errors="ignore"),
    )
