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

"""
Module houses ``DatetimeIndex`` class, that is distributed version of
``pandas.DatetimeIndex``.
"""

from __future__ import annotations

from datetime import timedelta, tzinfo

import modin.pandas as pd
import numpy as np
import pandas as native_pd
from pandas._libs import lib
from pandas._typing import (
    ArrayLike,
    AxisInt,
    Dtype,
    Frequency,
    Hashable,
    TimeAmbiguous,
    TimeNonexistent,
)

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.extensions.index import Index
from snowflake.snowpark.modin.plugin.utils.error_message import (
    datetime_index_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    doc_replace_dataframe_with_link,
)

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


@_inherit_docstrings(
    native_pd.DatetimeIndex, modify_doc=doc_replace_dataframe_with_link
)
class DatetimeIndex(Index):

    # Equivalent index type in native pandas
    _NATIVE_INDEX_TYPE = native_pd.DatetimeIndex

    def __new__(
        cls,
        data: ArrayLike | native_pd.Index | pd.Series | None = None,
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
    ) -> DatetimeIndex:
        if query_compiler:
            # Raise error if underlying type is not a TimestampType.
            if not query_compiler.is_datetime64_any_dtype(idx=0, is_index=True):
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
        index = object.__new__(cls)
        query_compiler = DatetimeIndex._init_query_compiler(
            data, _CONSTRUCTOR_DEFAULTS, query_compiler, **kwargs
        )
        # Convert to datetime64 if not already.
        if not query_compiler.is_datetime64_any_dtype(idx=0, is_index=True):
            query_compiler = query_compiler.series_to_datetime(include_index=True)
        index._query_compiler = query_compiler
        # `_parent` keeps track of any Series or DataFrame that this Index is a part of.
        index._parent = None
        return index

    def __init__(
        self,
        data: ArrayLike | native_pd.Index | pd.Series | None = None,
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
        # DatetimeIndex is already initialized in __new__ method. We keep this method
        # only for docstring generation.
        pass  # pragma: no cover

    def _dt_property(self, property_name: str) -> Index:
        if property_name in (
            "date",
            "time",
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
        return self._dt_property("year")

    @property
    def month(self) -> Index:
        return self._dt_property("month")

    @property
    def day(self) -> Index:
        return self._dt_property("day")

    @property
    def hour(self) -> Index:
        return self._dt_property("hour")

    @property
    def minute(self) -> Index:
        return self._dt_property("minute")

    @property
    def second(self) -> Index:
        return self._dt_property("second")

    @property
    def microsecond(self) -> Index:
        return self._dt_property("microsecond")

    @property
    def nanosecond(self) -> Index:
        return self._dt_property("nanosecond")

    @property
    def date(self) -> Index:
        return self._dt_property("date")

    @property
    def dayofweek(self) -> Index:
        return self._dt_property("dayofweek")

    day_of_week = dayofweek
    weekday = dayofweek

    @property
    def dayofyear(self) -> Index:
        return self._dt_property("dayofyear")

    day_of_year = dayofyear

    @property
    def quarter(self) -> Index:
        return self._dt_property("quarter")

    @property
    def is_month_start(self) -> Index:
        return self._dt_property("is_month_start")

    @property
    def is_month_end(self) -> Index:
        return self._dt_property("is_month_end")

    @property
    def is_quarter_start(self) -> Index:
        return self._dt_property("is_quarter_start")

    @property
    def is_quarter_end(self) -> Index:
        return self._dt_property("is_quarter_end")

    @property
    def is_year_start(self) -> Index:
        return self._dt_property("is_year_start")

    @property
    def is_year_end(self) -> Index:
        return self._dt_property("is_year_end")

    @property
    def is_leap_year(self) -> Index:
        return self._dt_property("is_leap_year")

    @property
    def time(self) -> Index:
        return self._dt_property("time")

    @datetime_index_not_implemented()
    @property
    def timetz(self) -> Index:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    @property
    def tz(self) -> tzinfo | None:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    @property
    def freq(self) -> str | None:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    @property
    def freqstr(self) -> str | None:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    @property
    def inferred_freq(self) -> str | None:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    def indexer_at_time(self, time, asof: bool = False) -> np.ndarray[np.intp]:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    def indexer_between_time(
        self, start_time, end_time, include_start: bool = True, include_end: bool = True
    ) -> np.ndarray[np.intp]:
        pass  # pragma: no cover

    def normalize(self) -> DatetimeIndex:
        return DatetimeIndex(
            query_compiler=self._query_compiler.dt_normalize(include_index=True)
        )

    @datetime_index_not_implemented()
    def strftime(self, date_format: str) -> np.ndarray[np.object_]:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    def snap(self, freq: Frequency = "S") -> DatetimeIndex:
        pass  # pragma: no cover

    def tz_convert(self, tz) -> DatetimeIndex:
        # TODO (SNOW-1660843): Support tz in pd.date_range and unskip the doctests.
        return DatetimeIndex(
            query_compiler=self._query_compiler.dt_tz_convert(
                tz,
                include_index=True,
            )
        )

    def tz_localize(
        self,
        tz,
        ambiguous: TimeAmbiguous = "raise",
        nonexistent: TimeNonexistent = "raise",
    ) -> DatetimeIndex:
        # TODO (SNOW-1660843): Support tz in pd.date_range and unskip the doctests.
        return DatetimeIndex(
            query_compiler=self._query_compiler.dt_tz_localize(
                tz,
                ambiguous,
                nonexistent,
                include_index=True,
            )
        )

    def round(
        self, freq: Frequency, ambiguous: str = "raise", nonexistent: str = "raise"
    ) -> DatetimeIndex:
        return DatetimeIndex(
            query_compiler=self._query_compiler.dt_round(
                freq, ambiguous, nonexistent, include_index=True
            )
        )

    def floor(
        self, freq: Frequency, ambiguous: str = "raise", nonexistent: str = "raise"
    ) -> DatetimeIndex:
        return DatetimeIndex(
            query_compiler=self._query_compiler.dt_floor(
                freq, ambiguous, nonexistent, include_index=True
            )
        )

    def ceil(
        self, freq: Frequency, ambiguous: str = "raise", nonexistent: str = "raise"
    ) -> DatetimeIndex:
        return DatetimeIndex(
            query_compiler=self._query_compiler.dt_ceil(
                freq, ambiguous, nonexistent, include_index=True
            )
        )

    def month_name(self, locale: str = None) -> Index:
        return Index(
            query_compiler=self._query_compiler.dt_month_name(
                locale=locale, include_index=True
            )
        )

    def day_name(self, locale: str = None) -> Index:
        return Index(
            query_compiler=self._query_compiler.dt_day_name(
                locale=locale, include_index=True
            )
        )

    @datetime_index_not_implemented()
    def as_unit(self, unit: str) -> DatetimeIndex:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    def to_period(self, freq=None) -> Index:
        pass  # pragma: no cover

    @datetime_index_not_implemented()
    def to_pydatetime(self) -> np.ndarray:
        pass  # pragma: no cover

    def mean(
        self, *, skipna: bool = True, axis: AxisInt | None = 0
    ) -> native_pd.Timestamp:
        # Need to convert timestamp to int value (nanoseconds) before aggregating.
        # TODO: SNOW-1625233 When `tz` is supported, add a `tz` parameter to `to_datetime` for correct timezone result.
        if axis not in [None, 0]:
            raise ValueError(
                f"axis={axis} is not supported, this parameter is ignored. 0 is the only valid axis."
            )
        return pd.to_datetime(
            self.to_series().astype("int64").agg("mean", axis=0, skipna=skipna)
        )

    def std(
        self,
        axis: AxisInt | None = None,
        ddof: int = 1,
        skipna: bool = True,
        **kwargs,
    ) -> timedelta:
        if axis not in [None, 0]:
            raise ValueError(
                f"axis={axis} is not supported, this parameter is ignored. 0 is the only valid axis."
            )
        if ddof != 1:
            raise NotImplementedError(
                "`ddof` parameter is not yet supported for `std`."
            )
        # Snowflake cannot directly perform `std` on a timestamp; therefore, convert the timestamp to an integer.
        # By default, the integer version of a timestamp is in nanoseconds. Directly performing computations with
        # nanoseconds can lead to results with integer size much larger than the original integer size. Therefore,
        # convert the nanoseconds to seconds and then compute the standard deviation.
        # The timestamp is converted to seconds instead of the float version of nanoseconds since that can lead to
        # floating point precision issues
        return pd.to_timedelta(
            (self.to_series().astype(int) // 1_000_000_000).agg(
                "std", axis=0, ddof=ddof, skipna=skipna, **kwargs
            )
            * 1_000_000_000
        )
