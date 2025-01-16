#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.

"""This module contains DatetimeIndex docstrings that override modin's docstrings."""

from __future__ import annotations

from .index import Index


class DatetimeIndex(Index):
    def __new__():
        """
        Create new instance of DatetimeIndex. This overrides behavior of Index.__new__.

        Parameters
        ----------
        data : array-like (1-dimensional), pandas.Index, modin.pandas.Series, optional
            Datetime-like data to construct index with.
        freq : str or pandas offset object, optional
            One of pandas date offset strings or corresponding objects. The string
            'infer' can be passed in order to set the frequency of the index as the
            inferred frequency upon creation.
        tz : pytz.timezone or dateutil.tz.tzfile or datetime.tzinfo or str
            Set the Timezone of the data.
        normalize : bool, default False
            Normalize start/end dates to midnight before generating date range.
        closed : {'left', 'right'}, optional
            Set whether to include `start` and `end` that are on the
            boundary. The default includes boundary points on either end.
        ambiguous : 'infer', bool-ndarray, 'NaT', default 'raise'
            When clocks moved backward due to DST, ambiguous times may arise.
            For example in Central European Time (UTC+01), when going from 03:00
            DST to 02:00 non-DST, 02:30:00 local time occurs both at 00:30:00 UTC
            and at 01:30:00 UTC. In such a situation, the `ambiguous` parameter
            dictates how ambiguous times should be handled.

            - 'infer' will attempt to infer fall dst-transition hours based on
              order
            - bool-ndarray where True signifies a DST time, False signifies a
              non-DST time (note that this flag is only applicable for ambiguous
              times)
            - 'NaT' will return NaT where there are ambiguous times
            - 'raise' will raise an AmbiguousTimeError if there are ambiguous times.
        dayfirst : bool, default False
            If True, parse dates in `data` with the day first order.
        yearfirst : bool, default False
            If True parse dates in `data` with the year first order.
        dtype : numpy.dtype or DatetimeTZDtype or str, default None
            Note that the only NumPy dtype allowed is `datetime64[ns]`.
        copy : bool, default False
            Make a copy of input ndarray.
        name : label, default None
            Name to be stored in the index.
        query_compiler : SnowflakeQueryCompiler, optional
            A query compiler object to create the ``Index`` from.

        Returns:
            New instance of DatetimeIndex.
        """

    def __init__() -> None:
        """
        Immutable ndarray-like of datetime64 data.

        Parameters
        ----------
        data : array-like (1-dimensional), pandas.Index, modin.pandas.Series, optional
            Datetime-like data to construct index with.
        freq : str or pandas offset object, optional
            One of pandas date offset strings or corresponding objects. The string
            'infer' can be passed in order to set the frequency of the index as the
            inferred frequency upon creation.
        tz : pytz.timezone or dateutil.tz.tzfile or datetime.tzinfo or str
            Set the Timezone of the data.
        normalize : bool, default False
            Normalize start/end dates to midnight before generating date range.
        closed : {'left', 'right'}, optional
            Set whether to include `start` and `end` that are on the
            boundary. The default includes boundary points on either end.
        ambiguous : 'infer', bool-ndarray, 'NaT', default 'raise'
            When clocks moved backward due to DST, ambiguous times may arise.
            For example in Central European Time (UTC+01), when going from 03:00
            DST to 02:00 non-DST, 02:30:00 local time occurs both at 00:30:00 UTC
            and at 01:30:00 UTC. In such a situation, the `ambiguous` parameter
            dictates how ambiguous times should be handled.

            - 'infer' will attempt to infer fall dst-transition hours based on
              order
            - bool-ndarray where True signifies a DST time, False signifies a
              non-DST time (note that this flag is only applicable for ambiguous
              times)
            - 'NaT' will return NaT where there are ambiguous times
            - 'raise' will raise an AmbiguousTimeError if there are ambiguous times.
        dayfirst : bool, default False
            If True, parse dates in `data` with the day first order.
        yearfirst : bool, default False
            If True parse dates in `data` with the year first order.
        dtype : numpy.dtype or DatetimeTZDtype or str, default None
            Note that the only NumPy dtype allowed is `datetime64[ns]`.
        copy : bool, default False
            Make a copy of input ndarray.
        name : label, default None
            Name to be stored in the index.
        query_compiler : SnowflakeQueryCompiler, optional
            A query compiler object to create the ``Index`` from.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00", "2/1/2020 11:00:00+00:00"], tz="America/Los_Angeles")
        >>> idx
        DatetimeIndex(['2020-01-01 02:00:00-08:00', '2020-02-01 03:00:00-08:00'], dtype='datetime64[ns, UTC-08:00]', freq=None)
        """

    def _dt_property():
        """
        Get the datetime property.

        Parameters
        ----------
        property_name : str
            The name of the datetime property.

        Returns
        -------
        Index
            The datetime property.
        """

    @property
    def year():
        """
        The year of the datetime.

        Returns
        -------
        An Index with the year of the datetime.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="YE")
        >>> idx
        DatetimeIndex(['2000-12-31', '2001-12-31', '2002-12-31'], dtype='datetime64[ns]', freq=None)
        >>> idx.year
        Index([2000, 2001, 2002], dtype='int64')
        """

    @property
    def month():
        """
        The month as January=1, December=12.

        Returns
        -------
        An Index with the month of the datetime.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="ME")
        >>> idx
        DatetimeIndex(['2000-01-31', '2000-02-29', '2000-03-31'], dtype='datetime64[ns]', freq=None)
        >>> idx.month
        Index([1, 2, 3], dtype='int64')
        """

    @property
    def day():
        """
        The day of the datetime.

        Returns
        -------
        An Index with the day of the datetime.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="D")
        >>> idx
        DatetimeIndex(['2000-01-01', '2000-01-02', '2000-01-03'], dtype='datetime64[ns]', freq=None)
        >>> idx.day
        Index([1, 2, 3], dtype='int64')
        """

    @property
    def hour():
        """
        The hours of the datetime.

        Returns
        -------
        An Index with the hours of the datetime.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="h")
        >>> idx
        DatetimeIndex(['2000-01-01 00:00:00', '2000-01-01 01:00:00',
                       '2000-01-01 02:00:00'],
                      dtype='datetime64[ns]', freq=None)
        >>> idx.hour
        Index([0, 1, 2], dtype='int64')
        """

    @property
    def minute():
        """
        The minutes of the datetime.

        Returns
        -------
        An Index with the minutes of the datetime.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="min")
        >>> idx
        DatetimeIndex(['2000-01-01 00:00:00', '2000-01-01 00:01:00',
                       '2000-01-01 00:02:00'],
                      dtype='datetime64[ns]', freq=None)
        >>> idx.minute
        Index([0, 1, 2], dtype='int64')
        """

    @property
    def second():
        """
        The seconds of the datetime.

        Returns
        -------
        An Index with the seconds of the datetime.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="s")
        >>> idx
        DatetimeIndex(['2000-01-01 00:00:00', '2000-01-01 00:00:01',
                       '2000-01-01 00:00:02'],
                      dtype='datetime64[ns]', freq=None)
        >>> idx.second
        Index([0, 1, 2], dtype='int64')
        """

    @property
    def microsecond():
        """
        The microseconds of the datetime.

        Returns
        -------
        An Index with the microseconds of the datetime.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="us")
        >>> idx
        DatetimeIndex([       '2000-01-01 00:00:00', '2000-01-01 00:00:00.000001',
                       '2000-01-01 00:00:00.000002'],
                      dtype='datetime64[ns]', freq=None)
        >>> idx.microsecond
        Index([0, 1, 2], dtype='int64')
        """

    @property
    def nanosecond():
        """
        The nanoseconds of the datetime.

        Returns
        -------
        An Index with the nanoseconds of the datetime.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="ns")
        >>> idx
        DatetimeIndex([          '2000-01-01 00:00:00',
                       '2000-01-01 00:00:00.000000001',
                       '2000-01-01 00:00:00.000000002'],
                      dtype='datetime64[ns]', freq=None)
        >>> idx.nanosecond
        Index([0, 1, 2], dtype='int64')
        """

    @property
    def date():
        """
        Returns the date part of Timestamps without time and timezone information.

        Returns
        -------
        Returns an Index with the date part of Timestamps. Note this is different
        from native pandas which returns a python array.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00",
        ...                         "2/1/2020 11:00:00+00:00"])
        >>> idx.date
        Index([2020-01-01, 2020-02-01], dtype='object')
        """

    @property
    def dayofweek():
        """
        The day of the week with Monday=0, Sunday=6.

        Return the day of the week. It is assumed the week starts on
        Monday, which is denoted by 0 and ends on Sunday which is denoted
        by 6. This method is available on both Series with datetime
        values (using the `dt` accessor) or DatetimeIndex.

        Returns
        -------
        An Index Containing integers indicating the day number.

        Examples
        --------
        >>> idx = pd.date_range('2016-12-31', '2017-01-08', freq='D')
        >>> idx.dayofweek
        Index([5, 6, 0, 1, 2, 3, 4, 5, 6], dtype='int64')
        """

    day_of_week = dayofweek
    weekday = dayofweek

    @property
    def dayofyear():
        """
        The ordinal day of the year.

        Returns
        -------
        An Index Containing integers indicating the ordinal day of the year.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00",
        ...                         "2/1/2020 11:00:00+00:00"])
        >>> idx.dayofyear
        Index([1, 32], dtype='int64')
        """

    day_of_year = dayofyear

    @property
    def quarter():
        """
        The quarter of the date.

        Returns
        -------
        An Index Containing quarter of the date.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00",
        ...                         "2/1/2020 11:00:00+00:00"])
        >>> idx.quarter
        Index([1, 1], dtype='int64')
        """

    @property
    def is_month_start():
        """
        Indicates whether the date is the first day of the month.

        Returns
        -------
        An Index with boolean values. Note this is different from native pandas which
        returns a python array.

        See Also
        --------
        is_month_end : Similar property indicating the last day of the month.

        Examples
        --------
        >>> idx = pd.date_range("2018-02-27", periods=3)
        >>> idx.is_month_start
        Index([False, False, True], dtype='bool')
        """

    @property
    def is_month_end():
        """
        Indicates whether the date is the last day of the month.

        Returns
        -------
        An Index with boolean values. Note this is different from native pandas which
        returns a python array.

        See Also
        --------
        is_month_start : Similar property indicating the first day of the month.

        Examples
        --------

        >>> idx = pd.date_range("2018-02-27", periods=3)
        >>> idx.is_month_end
        Index([False, True, False], dtype='bool')
        """

    @property
    def is_quarter_start():
        """
        Indicator for whether the date is the first day of a quarter.

        Returns
        -------
        An Index with boolean values. Note this is different from native pandas which
        returns a python array.

        See Also
        --------
        is_quarter_end : Similar property indicating the last day of the quarter.

        Examples
        --------
        >>> idx = pd.date_range('2017-03-30', periods=4)
        >>> idx
        DatetimeIndex(['2017-03-30', '2017-03-31', '2017-04-01', '2017-04-02'], dtype='datetime64[ns]', freq=None)

        >>> idx.is_quarter_start
        Index([False, False, True, False], dtype='bool')
        """

    @property
    def is_quarter_end():
        """
        Indicator for whether the date is the last day of a quarter.

        Returns
        -------
        An Index with boolean values. Note this is different from native pandas which
        returns a python array.

        See Also
        --------
        is_quarter_start: Similar property indicating the first day of the quarter.

        Examples
        --------
        >>> idx = pd.date_range('2017-03-30', periods=4)
        >>> idx
        DatetimeIndex(['2017-03-30', '2017-03-31', '2017-04-01', '2017-04-02'], dtype='datetime64[ns]', freq=None)

        >>> idx.is_quarter_end
        Index([False, True, False, False], dtype='bool')
        """

    @property
    def is_year_start():
        """
        Indicate whether the date is the first day of a year.

        Returns
        -------
        An Index with boolean values. Note this is different from native pandas which
        returns a python array.

        See Also
        --------
        is_year_end : Similar property indicating the last day of the year.

        Examples
        --------
        >>> idx = pd.date_range("2017-12-30", periods=3)
        >>> idx
        DatetimeIndex(['2017-12-30', '2017-12-31', '2018-01-01'], dtype='datetime64[ns]', freq=None)

        >>> idx.is_year_start
        Index([False, False, True], dtype='bool')
        """

    @property
    def is_year_end():
        """
        Indicate whether the date is the last day of the year.

        Returns
        -------
        An Index with boolean values. Note this is different from native pandas which
        returns a python array.

        See Also
        --------
        is_year_start : Similar property indicating the start of the year.

        Examples
        --------
        >>> idx = pd.date_range("2017-12-30", periods=3)
        >>> idx
        DatetimeIndex(['2017-12-30', '2017-12-31', '2018-01-01'], dtype='datetime64[ns]', freq=None)

        >>> idx.is_year_end
        Index([False, True, False], dtype='bool')
        """

    @property
    def is_leap_year():
        """
        Boolean indicator if the date belongs to a leap year.

        A leap year is a year, which has 366 days (instead of 365) including
        29th of February as an intercalary day.
        Leap years are years which are multiples of four except for  years
        divisible by 100 but not by 400.

        Returns
        -------
        An Index with boolean values. Note this is different from native pandas which
        returns a python array.

        Examples
        --------
        >>> idx = pd.date_range("2012-01-01", "2015-01-01", freq="YE")
        >>> idx
        DatetimeIndex(['2012-12-31', '2013-12-31', '2014-12-31'], dtype='datetime64[ns]', freq=None)
        >>> idx.is_leap_year
        Index([True, False, False], dtype='bool')
        """

    @property
    def time():
        """
        Returns the time part of the Timestamps.

        Returns
        -------
        An Index with the time part of the Timestamps.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00",
        ...                         "2/1/2020 11:00:00+00:00"])
        >>> idx.time
        Index([10:00:00, 11:00:00], dtype='object')
        """

    @property
    def timetz():
        """
        Returns the time part of the Timestamps with timezone.

        Returns
        -------
        An Index with the time part with timezone of the Timestamps.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00",
        ...                         "2/1/2020 11:00:00+00:00"])
        >>> idx.timetz  # doctest: +SKIP
        Index(["10:00:00+00:00", "11:00:00+00:00"], dtype='object')
        """

    @property
    def tz():
        """
        Return the timezone.

        Returns
        -------
        datetime.tzinfo, pytz.tzinfo.BaseTZInfo, dateutil.tz.tz.tzfile, or None
            Returns None when the array is tz-naive.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00",
        ...                         "2/1/2020 11:00:00+00:00"])
        >>> idx.tz  # doctest: +SKIP
        datetime.timezone.utc
        """

    @property
    def freq():
        """
        Return the frequency object if it's set, otherwise None.

        Examples
        --------
        >>> idx = pd.date_range("2000-01-01", periods=3, freq="YE")
        >>> idx.freq  # doctest: +SKIP
        <YearEnd: month=12>
        """

    @property
    def freqstr():
        """
        Return the frequency object as a string if it's set, otherwise None.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00"], freq="D")
        >>> idx.freqstr  # doctest: +SKIP
        'D'

        The frequency can be inferred if there are more than 2 points:

        >>> idx = pd.DatetimeIndex(["2018-01-01", "2018-01-03", "2018-01-05"],
        ...                        freq="infer")
        >>> idx.freqstr  # doctest: +SKIP
        '2D'
        """

    @property
    def inferred_freq():
        """
        Tries to return a string representing a frequency generated by infer_freq.

        Returns None if it can't autodetect the frequency.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["2018-01-01", "2018-01-03", "2018-01-05"])
        >>> idx.inferred_freq  # doctest: +SKIP
        '2D'
        """

    def indexer_at_time():
        """
        Return index locations of values at particular time of day.

        Parameters
        ----------
        time : datetime.time or str
            Time passed in either as object (datetime.time) or as string in
            appropriate format ("%H:%M", "%H%M", "%I:%M%p", "%I%M%p",
            "%H:%M:%S", "%H%M%S", "%I:%M:%S%p", "%I%M%S%p").

        Returns
        -------
        np.ndarray[np.intp]

        See Also
        --------
        indexer_between_time : Get index locations of values between particular
            times of day.
        DataFrame.at_time : Select values at particular time of day.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00", "2/1/2020 11:00",
        ...                         "3/1/2020 10:00"])
        >>> idx.indexer_at_time("10:00")  # doctest: +SKIP
        array([0, 2])
        """

    def indexer_between_time():
        """
        Return index locations of values between particular times of day.

        Parameters
        ----------
        start_time, end_time : datetime.time, str
            Time passed either as object (datetime.time) or as string in
            appropriate format ("%H:%M", "%H%M", "%I:%M%p", "%I%M%p",
            "%H:%M:%S", "%H%M%S", "%I:%M:%S%p","%I%M%S%p").
        include_start : bool, default True
        include_end : bool, default True

        Returns
        -------
        np.ndarray[np.intp]

        See Also
        --------
        indexer_at_time : Get index locations of values at particular time of day.
        DataFrame.between_time : Select values between particular times of day.

        Examples
        --------
        >>> idx = pd.date_range("2023-01-01", periods=4, freq="h")
        >>> idx
        DatetimeIndex(['2023-01-01 00:00:00', '2023-01-01 01:00:00',
                       '2023-01-01 02:00:00', '2023-01-01 03:00:00'],
                      dtype='datetime64[ns]', freq=None)
        >>> idx.indexer_between_time("00:00", "2:00", include_end=False)  # doctest: +SKIP
        array([0, 1])
        """

    def normalize():
        """
        Convert times to midnight.

        The time component of the date-time is converted to midnight i.e.
        00:00:00. This is useful in cases, when the time does not matter.
        Length is unaltered. The timezones are unaffected.

        This method is available on Series with datetime values under
        the ``.dt`` accessor, and directly on Datetime Array/Index.

        Returns
        -------
        DatetimeArray, DatetimeIndex or Series
            The same type as the original data. Series will have the same
            name and index. DatetimeIndex will have the same name.

        See Also
        --------
        floor : Floor the datetimes to the specified freq.
        ceil : Ceil the datetimes to the specified freq.
        round : Round the datetimes to the specified freq.

        Examples
        --------
        >>> idx = pd.date_range(start='2014-08-01 10:00', freq='h',
        ...                     periods=3, tz='Asia/Calcutta')  # doctest: +SKIP
        >>> idx  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 10:00:00+05:30',
                       '2014-08-01 11:00:00+05:30',
                       '2014-08-01 12:00:00+05:30'],
                        dtype='datetime64[ns, Asia/Calcutta]', freq=None)
        >>> idx.normalize()  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 00:00:00+05:30',
                       '2014-08-01 00:00:00+05:30',
                       '2014-08-01 00:00:00+05:30'],
                       dtype='datetime64[ns, Asia/Calcutta]', freq=None)
        """

    def strftime():
        """
        Convert to Index using specified date_format.

        Return an Index of formatted strings specified by date_format, which
        supports the same string format as the python standard library. Details
        of the string format can be found in `python string format
        doc <%(URL)s>`__.

        Formats supported by the C `strftime` API but not by the python string format
        doc (such as `"%%R"`, `"%%r"`) are not officially supported and should be
        preferably replaced with their supported equivalents (such as `"%%H:%%M"`,
        `"%%I:%%M:%%S %%p"`).

        Note that `PeriodIndex` support additional directives, detailed in
        `Period.strftime`.

        Parameters
        ----------
        date_format : str
            Date format string (e.g. "%%Y-%%m-%%d").

        Returns
        -------
        ndarray[object]
            NumPy ndarray of formatted strings.

        See Also
        --------
        to_datetime : Convert the given argument to datetime.
        DatetimeIndex.normalize : Return DatetimeIndex with times to midnight.
        DatetimeIndex.round : Round the DatetimeIndex to the specified freq.
        DatetimeIndex.floor : Floor the DatetimeIndex to the specified freq.
        Timestamp.strftime : Format a single Timestamp.
        Period.strftime : Format a single Period.

        Examples
        --------
        >>> rng = pd.date_range(pd.Timestamp("2018-03-10 09:00"),
        ...                     periods=3, freq=None)
        >>> rng.strftime('%%B %%d, %%Y, %%r')  # doctest: +SKIP
        Index(['March 10, 2018, 09:00:00 AM', 'March 10, 2018, 09:00:01 AM',
               'March 10, 2018, 09:00:02 AM'],
              dtype='object')
        """

    def snap():
        """
        Snap time stamps to nearest occurring frequency.

        Returns
        -------
        DatetimeIndex

        Examples
        --------
        >>> idx = pd.DatetimeIndex(['2023-01-01', '2023-01-02',
        ...                        '2023-02-01', '2023-02-02'])
        >>> idx
        DatetimeIndex(['2023-01-01', '2023-01-02', '2023-02-01', '2023-02-02'], dtype='datetime64[ns]', freq=None)
        >>> idx.snap('MS')  # doctest: +SKIP
        DatetimeIndex(['2023-01-01', '2023-01-01', '2023-02-01', '2023-02-01'], dtype='datetime64[ns]', freq=None)
        """

    def tz_convert():
        """
        Convert tz-aware Datetime Array/Index from one time zone to another.

        Parameters
        ----------
        tz : str, pytz.timezone, dateutil.tz.tzfile, datetime.tzinfo or None
            Time zone for time. Corresponding timestamps would be converted
            to this time zone of the Datetime Array/Index. A `tz` of None will
            convert to UTC and remove the timezone information.

        Returns
        -------
        Array or Index

        Raises
        ------
        TypeError
            If Datetime Array/Index is tz-naive.

        See Also
        --------
        DatetimeIndex.tz : A timezone that has a variable offset from UTC.
        DatetimeIndex.tz_localize : Localize tz-naive DatetimeIndex to a
            given time zone, or remove timezone from a tz-aware DatetimeIndex.

        Examples
        --------
        With the `tz` parameter, we can change the DatetimeIndex
        to other time zones:

        >>> dti = pd.date_range(start='2014-08-01 09:00',
        ...                     freq='h', periods=3, tz='Europe/Berlin')  # doctest: +SKIP

        >>> dti  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 09:00:00+02:00',
                       '2014-08-01 10:00:00+02:00',
                       '2014-08-01 11:00:00+02:00'],
                      dtype='datetime64[ns, Europe/Berlin]', freq=None)

        >>> dti.tz_convert('US/Central')  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 02:00:00-05:00',
                       '2014-08-01 03:00:00-05:00',
                       '2014-08-01 04:00:00-05:00'],
                      dtype='datetime64[ns, US/Central]', freq='h')

        With the ``tz=None``, we can remove the timezone (after converting
        to UTC if necessary):

        >>> dti = pd.date_range(start='2014-08-01 09:00', freq='h',
        ...                     periods=3, tz='Europe/Berlin')  # doctest: +SKIP

        >>> dti  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 09:00:00+02:00',
                       '2014-08-01 10:00:00+02:00',
                       '2014-08-01 11:00:00+02:00'],
                        dtype='datetime64[ns, Europe/Berlin]', freq=None)

        >>> dti.tz_convert(None)  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 07:00:00',
                       '2014-08-01 08:00:00',
                       '2014-08-01 09:00:00'],
                        dtype='datetime64[ns]', freq='h')
        """

    def tz_localize():
        """
        Localize tz-naive Datetime Array/Index to tz-aware Datetime Array/Index.

        This method takes a time zone (tz) naive Datetime Array/Index object
        and makes this time zone aware. It does not move the time to another
        time zone.

        This method can also be used to do the inverse -- to create a time
        zone unaware object from an aware object. To that end, pass `tz=None`.

        Parameters
        ----------
        tz : str, pytz.timezone, dateutil.tz.tzfile, datetime.tzinfo or None
            Time zone to convert timestamps to. Passing ``None`` will
            remove the time zone information preserving local time.
        ambiguous : 'infer', 'NaT', bool array, default 'raise'
            When clocks moved backward due to DST, ambiguous times may arise.
            For example in Central European Time (UTC+01), when going from
            03:00 DST to 02:00 non-DST, 02:30:00 local time occurs both at
            00:30:00 UTC and at 01:30:00 UTC. In such a situation, the
            `ambiguous` parameter dictates how ambiguous times should be
            handled.

            - 'infer' will attempt to infer fall dst-transition hours based on
              order
            - bool-ndarray where True signifies a DST time, False signifies a
              non-DST time (note that this flag is only applicable for
              ambiguous times)
            - 'NaT' will return NaT where there are ambiguous times
            - 'raise' will raise an AmbiguousTimeError if there are ambiguous
              times.

        nonexistent : 'shift_forward', 'shift_backward, 'NaT', timedelta, \
default 'raise'
            A nonexistent time does not exist in a particular timezone
            where clocks moved forward due to DST.

            - 'shift_forward' will shift the nonexistent time forward to the
              closest existing time
            - 'shift_backward' will shift the nonexistent time backward to the
              closest existing time
            - 'NaT' will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - 'raise' will raise an NonExistentTimeError if there are
              nonexistent times.

        Returns
        -------
        Same type as self
            Array/Index converted to the specified time zone.

        Raises
        ------
        TypeError
            If the Datetime Array/Index is tz-aware and tz is not None.

        See Also
        --------
        DatetimeIndex.tz_convert : Convert tz-aware DatetimeIndex from
            one time zone to another.

        Examples
        --------
        >>> tz_naive = pd.date_range('2018-03-01 09:00', periods=3)
        >>> tz_naive
        DatetimeIndex(['2018-03-01 09:00:00', '2018-03-02 09:00:00',
                       '2018-03-03 09:00:00'],
                      dtype='datetime64[ns]', freq=None)

        Localize DatetimeIndex in US/Eastern time zone:

        >>> tz_aware = tz_naive.tz_localize(tz='US/Eastern')
        >>> tz_aware
        DatetimeIndex(['2018-03-01 09:00:00-05:00', '2018-03-02 09:00:00-05:00',
                       '2018-03-03 09:00:00-05:00'],
                      dtype='datetime64[ns, UTC-05:00]', freq=None)

        With the ``tz=None``, we can remove the time zone information
        while keeping the local time (not converted to UTC):

        >>> tz_aware.tz_localize(None)
        DatetimeIndex(['2018-03-01 09:00:00', '2018-03-02 09:00:00',
                       '2018-03-03 09:00:00'],
                      dtype='datetime64[ns]', freq=None)
        """

    def round():
        """
        Perform round operation on the data to the specified `freq`.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to {op} the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end). See
            frequency aliases for a list of possible `freq` values.
        ambiguous : 'infer', bool-ndarray, 'NaT', default 'raise'
            This parameter is only supported for 'raise'.
            Only relevant for DatetimeIndex:

            - 'infer' will attempt to infer fall dst-transition hours based on
              order
            - bool-ndarray where True signifies a DST time, False designates
              a non-DST time (note that this flag is only applicable for
              ambiguous times)
            - 'NaT' will return NaT where there are ambiguous times
            - 'raise' will raise an AmbiguousTimeError if there are ambiguous
              times.

        nonexistent : 'shift_forward', 'shift_backward', 'NaT', timedelta, default 'raise'
            This parameter is only supported for 'raise'.
            A nonexistent time does not exist in a particular timezone
            where clocks moved forward due to DST.

            - 'shift_forward' will shift the nonexistent time forward to the
              closest existing time
            - 'shift_backward' will shift the nonexistent time backward to the
              closest existing time
            - 'NaT' will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - 'raise' will raise an NonExistentTimeError if there are
              nonexistent times.

        Returns
        -------
        DatetimeIndex with round values.

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        **DatetimeIndex**

        >>> rng = pd.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng
        DatetimeIndex(['2018-01-01 11:59:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:01:00'],
                      dtype='datetime64[ns]', freq=None)

        >>> rng.round('h')
        DatetimeIndex(['2018-01-01 12:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:00:00'],
                      dtype='datetime64[ns]', freq=None)
        """

    def floor():
        """
        Perform floor operation on the data to the specified `freq`.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to {op} the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end). See
            frequency aliases for a list of possible `freq` values.
        ambiguous : 'infer', bool-ndarray, 'NaT', default 'raise'
            This parameter is only supported for 'raise'.
            Only relevant for DatetimeIndex:

            - 'infer' will attempt to infer fall dst-transition hours based on
              order
            - bool-ndarray where True signifies a DST time, False designates
              a non-DST time (note that this flag is only applicable for
              ambiguous times)
            - 'NaT' will return NaT where there are ambiguous times
            - 'raise' will raise an AmbiguousTimeError if there are ambiguous
              times.

        nonexistent : 'shift_forward', 'shift_backward', 'NaT', timedelta, default 'raise'
            This parameter is only supported for 'raise'.
            A nonexistent time does not exist in a particular timezone
            where clocks moved forward due to DST.

            - 'shift_forward' will shift the nonexistent time forward to the
              closest existing time
            - 'shift_backward' will shift the nonexistent time backward to the
              closest existing time
            - 'NaT' will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - 'raise' will raise an NonExistentTimeError if there are
              nonexistent times.

        Returns
        -------
        DatetimeIndex with floor values.

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        **DatetimeIndex**

        >>> rng = pd.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng
        DatetimeIndex(['2018-01-01 11:59:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:01:00'],
                      dtype='datetime64[ns]', freq=None)

        >>> rng.floor('h')
        DatetimeIndex(['2018-01-01 11:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:00:00'],
                      dtype='datetime64[ns]', freq=None)
        """

    def ceil():
        """
        Perform ceil operation on the data to the specified `freq`.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to {op} the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end). See
            frequency aliases for a list of possible `freq` values.
        ambiguous : 'infer', bool-ndarray, 'NaT', default 'raise'
            This parameter is only supported for 'raise'.
            Only relevant for DatetimeIndex:

            - 'infer' will attempt to infer fall dst-transition hours based on
              order
            - bool-ndarray where True signifies a DST time, False designates
              a non-DST time (note that this flag is only applicable for
              ambiguous times)
            - 'NaT' will return NaT where there are ambiguous times
            - 'raise' will raise an AmbiguousTimeError if there are ambiguous
              times.

        nonexistent : 'shift_forward', 'shift_backward', 'NaT', timedelta, default 'raise'
            This parameter is only supported for 'raise'.
            A nonexistent time does not exist in a particular timezone
            where clocks moved forward due to DST.

            - 'shift_forward' will shift the nonexistent time forward to the
              closest existing time
            - 'shift_backward' will shift the nonexistent time backward to the
              closest existing time
            - 'NaT' will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - 'raise' will raise an NonExistentTimeError if there are
              nonexistent times.

        Returns
        -------
        DatetimeIndex with ceil values.

        Raises
        ------
        ValueError if the `freq` cannot be converted.

        Examples
        --------
        **DatetimeIndex**

        >>> rng = pd.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng
        DatetimeIndex(['2018-01-01 11:59:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:01:00'],
                      dtype='datetime64[ns]', freq=None)

        >>> rng.ceil('h')
        DatetimeIndex(['2018-01-01 12:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 13:00:00'],
                      dtype='datetime64[ns]', freq=None)
        """

    def month_name():
        """
        Return the month names with specified locale.

        Parameters
        ----------
        locale : str, optional
            Locale determining the language in which to return the month name.
            Default is English locale (``'en_US.utf8'``). Use the command
            ``locale -a`` on your terminal on Unix systems to find your locale
            language code.

        Returns
        -------
            Index of month names.

        Examples
        --------
        >>> idx = pd.date_range(start='2018-01', freq='ME', periods=3)
        >>> idx
        DatetimeIndex(['2018-01-31', '2018-02-28', '2018-03-31'], dtype='datetime64[ns]', freq=None)
        >>> idx.month_name()
        Index(['January', 'February', 'March'], dtype='object')

        Using the ``locale`` parameter you can set a different locale language,
        for example: ``idx.month_name(locale='pt_BR.utf8')`` will return month
        names in Brazilian Portuguese language.

        >>> idx = pd.date_range(start='2018-01', freq='ME', periods=3)
        >>> idx
        DatetimeIndex(['2018-01-31', '2018-02-28', '2018-03-31'], dtype='datetime64[ns]', freq=None)
        >>> idx.month_name(locale='pt_BR.utf8')  # doctest: +SKIP
        Index(['Janeiro', 'Fevereiro', 'Março'], dtype='object')
        """

    def day_name():
        """
        Return the day names with specified locale.

        Parameters
        ----------
        locale : str, optional
            Locale determining the language in which to return the day name.
            Default is English locale (``'en_US.utf8'``). Use the command
            ``locale -a`` on your terminal on Unix systems to find your locale
            language code.

        Returns
        -------
            Index of day names.

        Examples
        --------
        >>> idx = pd.date_range(start='2018-01-01', freq='D', periods=3)
        >>> idx
        DatetimeIndex(['2018-01-01', '2018-01-02', '2018-01-03'], dtype='datetime64[ns]', freq=None)
        >>> idx.day_name()
        Index(['Monday', 'Tuesday', 'Wednesday'], dtype='object')

        Using the ``locale`` parameter you can set a different locale language,
        for example: ``idx.day_name(locale='pt_BR.utf8')`` will return day
        names in Brazilian Portuguese language.

        >>> idx = pd.date_range(start='2018-01-01', freq='D', periods=3)
        >>> idx
        DatetimeIndex(['2018-01-01', '2018-01-02', '2018-01-03'], dtype='datetime64[ns]', freq=None)
        >>> idx.day_name(locale='pt_BR.utf8')  # doctest: +SKIP
        Index(['Segunda', 'Terça', 'Quarta'], dtype='object')
        """

    def as_unit():
        """
        Convert to a dtype with the given unit resolution.

        Parameters
        ----------
        unit : {'s', 'ms', 'us', 'ns'}

        Returns
        -------
        same type as self

        Examples
        --------
        >>> idx = pd.DatetimeIndex(['2020-01-02 01:02:03.004005006'])
        >>> idx
        DatetimeIndex(['2020-01-02 01:02:03.004005006'], dtype='datetime64[ns]', freq=None)
        >>> idx.as_unit('s')  # doctest: +SKIP
        DatetimeIndex(['2020-01-02 01:02:03'], dtype='datetime64[s]', freq=None)
        """

    def to_period():
        """
        Cast to PeriodArray/PeriodIndex at a particular frequency.

        Converts DatetimeArray/Index to PeriodArray/PeriodIndex.

        Parameters
        ----------
        freq : str or Period, optional
            One of pandas' period aliases or a Period object.
            Will be inferred by default.

        Returns
        -------
        PeriodArray/PeriodIndex

        Raises
        ------
        ValueError
            When converting a DatetimeArray/Index with non-regular values,
            so that a frequency cannot be inferred.

        See Also
        --------
        PeriodIndex: Immutable ndarray holding ordinal values.
        DatetimeIndex.to_pydatetime: Return DatetimeIndex as object.

        Examples
        --------
        >>> df = pd.DataFrame({"y": [1, 2, 3]},
        ...                   index=pd.to_datetime(["2000-03-31 00:00:00",
        ...                                         "2000-05-31 00:00:00",
        ...                                         "2000-08-31 00:00:00"]))
        >>> df.index.to_period("M")  # doctest: +SKIP
        PeriodIndex(['2000-03', '2000-05', '2000-08'],
                    dtype='period[M]')

        Infer the daily frequency

        >>> idx = pd.date_range("2017-01-01", periods=2)
        >>> idx.to_period()  # doctest: +SKIP
        PeriodIndex(['2017-01-01', '2017-01-02'], dtype='period[D]')
        """

    def to_pydatetime():
        """
        Return a ndarray of ``datetime.datetime`` objects.

        Returns
        -------
        numpy.ndarray

        Examples
        --------
        >>> idx = pd.date_range('2018-02-27', periods=3)
        >>> idx.to_pydatetime()  # doctest: +SKIP
        array([datetime.datetime(2018, 2, 27, 0, 0),
               datetime.datetime(2018, 2, 28, 0, 0),
               datetime.datetime(2018, 3, 1, 0, 0)], dtype=object)
        """

    def mean():
        """
        Return the mean value of the Array.

        Parameters
        ----------
        skipna : bool, default True
            Whether to ignore any NaT elements.
        axis : int, optional, default 0
            The axis to calculate the mean over.
            This parameter is ignored - 0 is the only valid axis.

        Returns
        -------
            scalar Timestamp

        See Also
        --------
        numpy.ndarray.mean : Returns the average of array elements along a given axis.
        Series.mean : Return the mean value in a Series.

        Notes
        -----
        mean is only defined for Datetime and Timedelta dtypes, not for Period.

        Examples
        --------
        >>> idx = pd.date_range('2001-01-01 00:00', periods=3)
        >>> idx
        DatetimeIndex(['2001-01-01', '2001-01-02', '2001-01-03'], dtype='datetime64[ns]', freq=None)
        >>> idx.mean()
        Timestamp('2001-01-02 00:00:00')
        """

    def std():
        """
        Return sample standard deviation over requested axis.

        Normalized by `N-1` by default. This can be changed using ``ddof``.

        Parameters
        ----------
        axis : int, optional
            The axis to calculate the standard deviation over.
            This parameter is ignored - 0 is the only valid axis.
        ddof : int, default 1
            Degrees of Freedom. The divisor used in calculations is `N - ddof`,
            where `N` represents the number of elements.
            This parameter is not yet supported.
        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is ``NA``, the result
            will be ``NA``.

        Returns
        -------
        Timedelta

        See Also
        --------
        numpy.ndarray.std : Returns the standard deviation of the array elements
            along given axis.
        Series.std : Return sample standard deviation over requested axis.

        Examples
        --------
        For :class:`pandas.DatetimeIndex`:

        >>> idx = pd.date_range('2001-01-01 00:00', periods=3)
        >>> idx
        DatetimeIndex(['2001-01-01', '2001-01-02', '2001-01-03'], dtype='datetime64[ns]', freq=None)
        >>> idx.std()
        Timedelta('1 days 00:00:00')
        """
