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

"""This module contains TimedeltaIndex docstrings that override modin's docstrings."""

from __future__ import annotations

from snowflake.snowpark.modin.plugin.extensions.index import Index


class TimedeltaIndex(Index):
    def __new__():
        """
        Create new instance of TimedeltaIndex. This overrides behavior of Index.__new__.

        Parameters
        ----------
        data : array-like (1-dimensional), optional
            Optional timedelta-like data to construct index with.
        unit : {'D', 'h', 'm', 's', 'ms', 'us', 'ns'}, optional
            The unit of ``data``.

            .. deprecated:: 2.2.0
             Use ``pd.to_timedelta`` instead.

        freq : str or pandas offset object, optional
            One of pandas date offset strings or corresponding objects. The string
            ``'infer'`` can be passed in order to set the frequency of the index as
            the inferred frequency upon creation.
        dtype : numpy.dtype or str, default None
            Valid ``numpy`` dtypes are ``timedelta64[ns]``, ``timedelta64[us]``,
            ``timedelta64[ms]``, and ``timedelta64[s]``.
        copy : bool
            Make a copy of input array.
        name : object
            Name to be stored in the index.

        Returns:
            New instance of TimedeltaIndex.
        """

    def __init__() -> None:
        """
        Immutable Index of timedelta64 data.

        Represented internally as int64, and scalars returned Timedelta objects.

        Parameters
        ----------
        data : array-like (1-dimensional), optional
            Optional timedelta-like data to construct index with.
        unit : {'D', 'h', 'm', 's', 'ms', 'us', 'ns'}, optional
            The unit of ``data``.

            .. deprecated:: 2.2.0
             Use ``pd.to_timedelta`` instead.

        freq : str or pandas offset object, optional
            One of pandas date offset strings or corresponding objects. The string
            ``'infer'`` can be passed in order to set the frequency of the index as
            the inferred frequency upon creation.
        dtype : numpy.dtype or str, default None
            Valid ``numpy`` dtypes are ``timedelta64[ns]``, ``timedelta64[us]``,
            ``timedelta64[ms]``, and ``timedelta64[s]``.
        copy : bool
            Make a copy of input array.
        name : object
            Name to be stored in the index.

        Examples
        --------
        >>> pd.TimedeltaIndex(['0 days', '1 days', '2 days', '3 days', '4 days'])
        TimedeltaIndex(['0 days', '1 days', '2 days', '3 days', '4 days'], dtype='timedelta64[ns]', freq=None)

        We can also let pandas infer the frequency when possible.

        >>> pd.TimedeltaIndex(np.arange(5) * 24 * 3600 * 1e9, freq='infer')
        TimedeltaIndex(['0 days', '1 days', '2 days', '3 days', '4 days'], dtype='timedelta64[ns]', freq=None)
        """

    @property
    def days():
        """
        Number of days for each element.

        Returns
        -------
        An Index with the days component of the timedelta.

        Examples
        --------
        >>> idx = pd.to_timedelta(["0 days", "10 days", "20 days"])
        >>> idx
        TimedeltaIndex(['0 days', '10 days', '20 days'], dtype='timedelta64[ns]', freq=None)
        >>> idx.days
        Index([0, 10, 20], dtype='int64')
        """

    @property
    def seconds():
        """
        Number of seconds (>= 0 and less than 1 day) for each element.

        Returns
        -------
        An Index with seconds component of the timedelta.

        Examples
        --------
        >>> idx = pd.to_timedelta([1, 2, 3], unit='s')
        >>> idx
        TimedeltaIndex(['0 days 00:00:01', '0 days 00:00:02', '0 days 00:00:03'], dtype='timedelta64[ns]', freq=None)
        >>> idx.seconds
        Index([1, 2, 3], dtype='int64')
        """

    @property
    def microseconds():
        """
        Number of microseconds (>= 0 and less than 1 second) for each element.

        Returns
        -------
        An Index with microseconds component of the timedelta.

        Examples
        --------
        >>> idx = pd.to_timedelta([1, 2, 3], unit='us')
        >>> idx
        TimedeltaIndex(['0 days 00:00:00.000001', '0 days 00:00:00.000002',
                        '0 days 00:00:00.000003'],
                       dtype='timedelta64[ns]', freq=None)
        >>> idx.microseconds
        Index([1, 2, 3], dtype='int64')
        """

    @property
    def nanoseconds():
        """
        Number of nonoseconds (>= 0 and less than 1 microsecond) for each element.

        Returns
        -------
        An Index with nanoseconds compnent of the timedelta.

        Examples
        --------
        >>> idx = pd.to_timedelta([1, 2, 3], unit='ns')
        >>> idx
        TimedeltaIndex(['0 days 00:00:00.000000001', '0 days 00:00:00.000000002',
                        '0 days 00:00:00.000000003'],
                       dtype='timedelta64[ns]', freq=None)
        >>> idx.nanoseconds
        Index([1, 2, 3], dtype='int64')
        """

    @property
    def components():
        """
        Return a DataFrame of the individual resolution components of the Timedeltas.

        The components (days, hours, minutes seconds, milliseconds, microseconds,
        nanoseconds) are returned as columns in a DataFrame.

        Returns
        -------
        A DataFrame

        Examples
        --------
        >>> idx = pd.to_timedelta(['1 day 3 min 2 us 42 ns'])  # doctest: +SKIP
        >>> idx  # doctest: +SKIP
        TimedeltaIndex(['1 days 00:03:00.000002042'],
                       dtype='timedelta64[ns]', freq=None)
        >>> idx.components  # doctest: +SKIP
           days  hours  minutes  seconds  milliseconds  microseconds  nanoseconds
        0     1      0        3        0             0             2           42
        """

    @property
    def inferred_freq():
        """
        Tries to return a string representing a frequency generated by infer_freq.

        Returns None if it can't autodetect the frequency.

        Examples
        --------
        >>> idx = pd.to_timedelta(["0 days", "10 days", "20 days"])  # doctest: +SKIP
        >>> idx  # doctest: +SKIP
        TimedeltaIndex(['0 days', '10 days', '20 days'],
                       dtype='timedelta64[ns]', freq=None)
        >>> idx.inferred_freq  # doctest: +SKIP
        '10D'
        """

    def round():
        """
        Perform round operation on the data to the specified `freq`.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to round the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end). See
            frequency aliases for a list of possible `freq` values.

        Returns
        -------
        TimedeltaIndex with round values.

        Raises
        ------
        ValueError if the `freq` cannot be converted.
        """

    def floor():
        """
        Perform floor operation on the data to the specified `freq`.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to floor the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end). See
            frequency aliases for a list of possible `freq` values.

        Returns
        -------
        TimedeltaIndex with floor values.

        Raises
        ------
        ValueError if the `freq` cannot be converted.
        """

    def ceil():
        """
        Perform ceil operation on the data to the specified `freq`.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to ceil the index to. Must be a fixed
            frequency like 'S' (second) not 'ME' (month end). See
            frequency aliases for a list of possible `freq` values.

        Returns
        -------
        TimedeltaIndex with ceil values.

        Raises
        ------
        ValueError if the `freq` cannot be converted.
        """

    def to_pytimedelta():
        """
        Return an ndarray of datetime.timedelta objects.

        Returns
        -------
        numpy.ndarray

        Examples
        --------
        >>> idx = pd.to_timedelta([1, 2, 3], unit='D')  # doctest: +SKIP
        >>> idx  # doctest: +SKIP
        TimedeltaIndex(['1 days', '2 days', '3 days'],
                        dtype='timedelta64[ns]', freq=None)
        >>> idx.to_pytimedelta()  # doctest: +SKIP
        array([datetime.timedelta(days=1), datetime.timedelta(days=2),
               datetime.timedelta(days=3)], dtype=object)
        """

    def mean():
        """
        Return the mean value of the Timedelta values.

        Parameters
        ----------
        skipna : bool, default True
            Whether to ignore any NaT elements.
        axis : int, optional, default 0

        Returns
        -------
            scalar Timedelta

        Examples
        --------
        >>> idx = pd.to_timedelta([1, 2, 3, 1], unit='D')
        >>> idx
        TimedeltaIndex(['1 days', '2 days', '3 days', '1 days'], dtype='timedelta64[ns]', freq=None)
        >>> idx.mean()
        Timedelta('1 days 18:00:00')

        See Also
        --------
        numpy.ndarray.mean : Returns the average of array elements along a given axis.
        Series.mean : Return the mean value in a Series.
        """

    def as_unit():
        """
        Convert to a dtype with the given unit resolution.

        Parameters
        ----------
        unit : {'s', 'ms', 'us', 'ns'}

        Returns
        -------
        DatetimeIndex

        Examples
        --------
        >>> idx = pd.to_timedelta(['1 day 3 min 2 us 42 ns'])  # doctest: +SKIP
        >>> idx  # doctest: +SKIP
        TimedeltaIndex(['1 days 00:03:00.000002042'],
                        dtype='timedelta64[ns]', freq=None)
        >>> idx.as_unit('s')  # doctest: +SKIP
        TimedeltaIndex(['1 days 00:03:00'], dtype='timedelta64[s]', freq=None)
        """

    def total_seconds():
        """
        Return total duration of each element expressed in seconds.

        Returns
        -------
        An Index with float type.

        Examples:
        --------
        >>> idx = pd.to_timedelta(np.arange(5), unit='d')
        >>> idx
        TimedeltaIndex(['0 days', '1 days', '2 days', '3 days', '4 days'], dtype='timedelta64[ns]', freq=None)
        >>> idx.total_seconds()
        Index([0.0, 86400.0, 172800.0, 259200.0, 345600.0], dtype='float64')
        """
