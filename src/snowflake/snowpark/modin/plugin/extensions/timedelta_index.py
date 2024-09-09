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
Module houses ``TimedeltaIndex`` class, that is distributed version of
``pandas.TimedeltaIndex``.
"""

from __future__ import annotations

import numpy as np
import pandas as native_pd
from pandas._libs import lib
from pandas._typing import ArrayLike, AxisInt, Dtype, Frequency, Hashable
from pandas.core.dtypes.common import is_timedelta64_dtype

from snowflake.snowpark.modin.pandas import DataFrame, Series
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.extensions.index import Index
from snowflake.snowpark.modin.plugin.utils.error_message import (
    timedelta_index_not_implemented,
)

_CONSTRUCTOR_DEFAULTS = {
    "unit": lib.no_default,
    "freq": lib.no_default,
    "dtype": None,
    "copy": False,
    "name": None,
}


class TimedeltaIndex(Index):

    # Equivalent index type in native pandas
    _NATIVE_INDEX_TYPE = native_pd.TimedeltaIndex

    def __new__(
        cls,
        data: ArrayLike | native_pd.Index | Series | None = None,
        unit: str | lib.NoDefault = _CONSTRUCTOR_DEFAULTS["unit"],
        freq: Frequency | lib.NoDefault = _CONSTRUCTOR_DEFAULTS["freq"],
        dtype: Dtype | None = _CONSTRUCTOR_DEFAULTS["dtype"],
        copy: bool = _CONSTRUCTOR_DEFAULTS["copy"],
        name: Hashable | None = _CONSTRUCTOR_DEFAULTS["name"],
        query_compiler: SnowflakeQueryCompiler = None,
    ) -> TimedeltaIndex:
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
        if query_compiler:
            # Raise error if underlying type is not a Timedelta type.
            current_dtype = query_compiler.index_dtypes[0]
            if not is_timedelta64_dtype(current_dtype):
                raise ValueError(
                    f"TimedeltaIndex can only be created from a query compiler with TimedeltaType, found {current_dtype}"
                )
        kwargs = {
            "unit": unit,
            "freq": freq,
            "dtype": dtype,
            "copy": copy,
            "name": name,
        }
        tdi = object.__new__(cls)
        tdi._query_compiler = TimedeltaIndex._init_query_compiler(
            data, _CONSTRUCTOR_DEFAULTS, query_compiler, **kwargs
        )
        # `_parent` keeps track of any Series or DataFrame that this Index is a part of.
        tdi._parent = None
        return tdi

    def __init__(
        self,
        data: ArrayLike | native_pd.Index | Series | None = None,
        unit: str | lib.NoDefault = _CONSTRUCTOR_DEFAULTS["unit"],
        freq: Frequency | lib.NoDefault = _CONSTRUCTOR_DEFAULTS["freq"],
        dtype: Dtype | None = _CONSTRUCTOR_DEFAULTS["dtype"],
        copy: bool = _CONSTRUCTOR_DEFAULTS["copy"],
        name: Hashable | None = _CONSTRUCTOR_DEFAULTS["name"],
        query_compiler: SnowflakeQueryCompiler = None,
    ) -> None:
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
        # TimedeltaIndex is already initialized in __new__ method. We keep this method
        # only for docstring generation.

    @property
    def days(self) -> Index:
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
        return Index(
            query_compiler=self._query_compiler.timedelta_property(
                "days", include_index=True
            )
        )

    @property
    def seconds(self) -> Index:
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
        return Index(
            query_compiler=self._query_compiler.timedelta_property(
                "seconds", include_index=True
            )
        )

    @property
    def microseconds(self) -> Index:
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
        return Index(
            query_compiler=self._query_compiler.timedelta_property(
                "microseconds", include_index=True
            )
        )

    @property
    def nanoseconds(self) -> Index:
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
        return Index(
            query_compiler=self._query_compiler.timedelta_property(
                "nanoseconds", include_index=True
            )
        )

    @timedelta_index_not_implemented()
    @property
    def components(self) -> DataFrame:
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

    @timedelta_index_not_implemented()
    @property
    def inferred_freq(self) -> str | None:
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

    def round(self, freq: Frequency) -> TimedeltaIndex:
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
        return TimedeltaIndex(
            query_compiler=self._query_compiler.dt_round(freq, include_index=True)
        )

    def floor(self, freq: Frequency) -> TimedeltaIndex:
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
        return TimedeltaIndex(
            query_compiler=self._query_compiler.dt_floor(freq, include_index=True)
        )

    def ceil(self, freq: Frequency) -> TimedeltaIndex:
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
        return TimedeltaIndex(
            query_compiler=self._query_compiler.dt_ceil(freq, include_index=True)
        )

    @timedelta_index_not_implemented()
    def to_pytimedelta(self) -> np.ndarray:
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

    @timedelta_index_not_implemented()
    def mean(
        self, *, skipna: bool = True, axis: AxisInt | None = 0
    ) -> native_pd.Timestamp:
        """
        Return the mean value of the Array.

        Parameters
        ----------
        skipna : bool, default True
            Whether to ignore any NaT elements.
        axis : int, optional, default 0

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
        """

    @timedelta_index_not_implemented()
    def as_unit(self, unit: str) -> TimedeltaIndex:
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

    def total_seconds(self) -> Index:
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
        return Index(
            query_compiler=self._query_compiler.dt_total_seconds(include_index=True)
        )
