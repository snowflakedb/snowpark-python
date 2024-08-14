#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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

"""
Module houses ``DatetimeIndex`` class, that is distributed version of
``pandas.DatetimeIndex``.
"""

from __future__ import annotations

from datetime import tzinfo

import modin
import numpy as np
import pandas as native_pd
from pandas._libs import lib
from pandas._typing import ArrayLike, Dtype, Frequency, Hashable, TimeAmbiguous

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.extensions.index import Index
from snowflake.snowpark.modin.plugin.utils.error_message import (
    datetime_index_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage

_CONSTRUCTOR_DEFAULTS = {
    "freq": lib.no_default,
    "tz": lib.no_default,
    "normalize": lib.no_default,
    "closed": lib.no_default,
    "ambiguous": "raise",
    "dayfirst": False,
    "yearfirst": False,
    "dtype": None,
    "copy": False,
    "name": None,
}


class DatetimeIndex(Index):

    # Equivalent index type in native pandas
    _NATIVE_INDEX_TYPE = native_pd.DatetimeIndex

    def __new__(cls, *args, **kwargs):
        """
        Create new instance of DatetimeIndex. This overrides behavior of Index.__new__.
        Args:
            *args: arguments.
            **kwargs: keyword arguments.

        Returns:
            New instance of DatetimeIndex.
        """
        return object.__new__(cls)

    def __init__(
        self,
        data: ArrayLike | native_pd.Index | modin.pandas.Sereis | None = None,
        freq: Frequency | lib.NoDefault = _CONSTRUCTOR_DEFAULTS["freq"],
        tz=_CONSTRUCTOR_DEFAULTS["tz"],
        normalize: bool | lib.NoDefault = _CONSTRUCTOR_DEFAULTS["normalize"],
        closed=_CONSTRUCTOR_DEFAULTS["closed"],
        ambiguous: TimeAmbiguous = _CONSTRUCTOR_DEFAULTS["ambiguous"],
        dayfirst: bool = _CONSTRUCTOR_DEFAULTS["dayfirst"],
        yearfirst: bool = _CONSTRUCTOR_DEFAULTS["yearfirst"],
        dtype: Dtype | None = _CONSTRUCTOR_DEFAULTS["dtype"],
        copy: bool = _CONSTRUCTOR_DEFAULTS["copy"],
        name: Hashable | None = _CONSTRUCTOR_DEFAULTS["name"],
        query_compiler: SnowflakeQueryCompiler = None,
    ) -> None:
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
        DatetimeIndex(['2020-01-01 02:00:00-08:00', '2020-02-01 03:00:00-08:00'], dtype='datetime64[ns, America/Los_Angeles]', freq=None)
        """
        if query_compiler:
            # Raise error if underlying type is not a TimestampType.
            current_dtype = query_compiler.index_dtypes[0]
            if not current_dtype == np.dtype("datetime64[ns]"):
                raise ValueError(
                    "DatetimeIndex can only be created from a query compiler with TimestampType."
                )
        kwargs = {
            "freq": freq,
            "tz": tz,
            "normalize": normalize,
            "closed": closed,
            "ambiguous": ambiguous,
            "dayfirst": dayfirst,
            "yearfirst": yearfirst,
            "dtype": dtype,
            "copy": copy,
            "name": name,
        }
        self._init_index(data, _CONSTRUCTOR_DEFAULTS, query_compiler, **kwargs)

    def _dt_property(self, property_name: str) -> Index:
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
        if property_name in (
            "date",
            "is_month_start",
            "is_month_end",
            "is_quarter_start",
            "is_quarter_end",
            "is_year_start",
            "is_year_end",
            "is_leap_year",
        ):
            WarningMessage.single_warning(
                f"For DatetimeIndex.{property_name} native pandas returns a python array but Snowpark pandas returns a lazy Index."
            )
        return Index(
            query_compiler=self._query_compiler.dt_property(
                property_name, include_index=True
            )
        )

    @property
    def year(self) -> Index:
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
        Index([2000, 2001, 2002], dtype='int16')
        """
        return self._dt_property("year")

    @property
    def month(self) -> Index:
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
        Index([1, 2, 3], dtype='int8')
        """
        return self._dt_property("month")

    @property
    def day(self) -> Index:
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
        Index([1, 2, 3], dtype='int8')
        """
        return self._dt_property("day")

    @property
    def hour(self) -> Index:
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
        Index([0, 1, 2], dtype='int8')
        """
        return self._dt_property("hour")

    @property
    def minute(self) -> Index:
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
        Index([0, 1, 2], dtype='int8')
        """
        return self._dt_property("minute")

    @property
    def second(self) -> Index:
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
        Index([0, 1, 2], dtype='int8')
        """
        return self._dt_property("second")

    @property
    def microsecond(self) -> Index:
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
        return self._dt_property("microsecond")

    @property
    def nanosecond(self) -> Index:
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
        Index([0, 1, 2], dtype='int32')
        """
        return self._dt_property("nanosecond")

    @property
    def date(self) -> Index:
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
        return self._dt_property("date")

    @property
    def dayofweek(self) -> Index:
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
        Index([5, 6, 0, 1, 2, 3, 4, 5, 6], dtype='int16')
        """
        return self._dt_property("dayofweek")

    day_of_week = dayofweek
    weekday = dayofweek

    @property
    def dayofyear(self) -> Index:
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
        Index([1, 32], dtype='int16')
        """
        return self._dt_property("dayofyear")

    day_of_year = dayofyear

    @property
    def quarter(self) -> Index:
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
        Index([1, 1], dtype='int8')
        """
        return self._dt_property("quarter")

    @property
    def is_month_start(self) -> Index:
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
        return self._dt_property("is_month_start")

    @property
    def is_month_end(self) -> Index:
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
        return self._dt_property("is_month_end")

    @property
    def is_quarter_start(self) -> Index:
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
        return self._dt_property("is_quarter_start")

    @property
    def is_quarter_end(self) -> Index:
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
        return self._dt_property("is_quarter_end")

    @property
    def is_year_start(self) -> Index:
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
        return self._dt_property("is_year_start")

    @property
    def is_year_end(self) -> Index:
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
        return self._dt_property("is_year_end")

    @property
    def is_leap_year(self) -> Index:
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
        return self._dt_property("is_leap_year")

    @datetime_index_not_implemented()
    @property
    def time(self) -> Index:
        """
        Returns the time part of the Timestamps.

        Returns
        -------
        An Index with the time part of the Timestamps.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00",
        ...                         "2/1/2020 11:00:00+00:00"])
        >>> idx.time  # doctest: +SKIP
        Index(["10:00:00", "11:00:00"], dtype='object')
        """

    @datetime_index_not_implemented()
    @property
    def timetz(self) -> Index:
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

    @datetime_index_not_implemented()
    @property
    def tz(self) -> tzinfo | None:
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

    @datetime_index_not_implemented()
    @property
    def freqstr(self) -> str | None:
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

    @datetime_index_not_implemented()
    @property
    def inferred_freq(self) -> str | None:
        """
        Tries to return a string representing a frequency generated by infer_freq.

        Returns None if it can't autodetect the frequency.

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["2018-01-01", "2018-01-03", "2018-01-05"])
        >>> idx.inferred_freq  # doctest: +SKIP
        '2D'
        """
