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

import numpy as np
import pandas as native_pd
from pandas._libs import lib
from pandas._typing import ArrayLike, Dtype, Frequency, Hashable, TimeAmbiguous

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.extensions.index import Index

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
        data: ArrayLike | SnowflakeQueryCompiler | None = None,
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
    ) -> None:
        """
        Immutable ndarray-like of datetime64 data.

        Parameters
        ----------
        data : array-like (1-dimensional) or snowflake query compiler
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

        Examples
        --------
        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00", "2/1/2020 11:00:00+00:00"], tz="America/Los_Angeles")
        >>> idx
        DatetimeIndex(['2020-01-01 02:00:00-08:00', '2020-02-01 03:00:00-08:00'], dtype='datetime64[ns, America/Los_Angeles]', freq=None)
        """
        if isinstance(data, SnowflakeQueryCompiler):
            # Raise error if underlying type is not a TimestampType.
            dtype = data.index_dtypes[0]
            if not dtype == np.dtype("datetime64[ns]"):
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
        self._init_index(data, _CONSTRUCTOR_DEFAULTS, **kwargs)
