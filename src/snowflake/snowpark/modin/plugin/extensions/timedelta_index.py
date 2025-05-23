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
Module houses ``TimedeltaIndex`` class, that is distributed version of
``pandas.TimedeltaIndex``.
"""

from __future__ import annotations

import numpy as np
import pandas as native_pd
from modin.pandas import DataFrame, Series
from pandas._libs import lib
from pandas._typing import ArrayLike, AxisInt, Dtype, Frequency, Hashable
from pandas.core.dtypes.common import is_timedelta64_dtype

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.extensions.index import Index
from snowflake.snowpark.modin.plugin.utils.error_message import (
    timedelta_index_not_implemented,
)
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    doc_replace_dataframe_with_link,
)

_CONSTRUCTOR_DEFAULTS = {
    "unit": lib.no_default,
    "freq": lib.no_default,
    "dtype": None,
    "copy": False,
    "name": None,
}


@_inherit_docstrings(
    native_pd.TimedeltaIndex, modify_doc=doc_replace_dataframe_with_link
)
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
        # TimedeltaIndex is already initialized in __new__ method. We keep this method
        # only for docstring generation.
        pass  # pragma: no cover

    @property
    def days(self) -> Index:
        return Index(
            query_compiler=self._query_compiler.timedelta_property(
                "days", include_index=True
            )
        )

    @property
    def seconds(self) -> Index:
        return Index(
            query_compiler=self._query_compiler.timedelta_property(
                "seconds", include_index=True
            )
        )

    @property
    def microseconds(self) -> Index:
        return Index(
            query_compiler=self._query_compiler.timedelta_property(
                "microseconds", include_index=True
            )
        )

    @property
    def nanoseconds(self) -> Index:
        return Index(
            query_compiler=self._query_compiler.timedelta_property(
                "nanoseconds", include_index=True
            )
        )

    @timedelta_index_not_implemented()
    @property
    def components(self) -> DataFrame:
        pass  # pragma: no cover

    @timedelta_index_not_implemented()
    @property
    def inferred_freq(self) -> str | None:
        pass  # pragma: no cover

    def round(self, freq: Frequency) -> TimedeltaIndex:
        return TimedeltaIndex(
            query_compiler=self._query_compiler.dt_round(freq, include_index=True)
        )

    def floor(self, freq: Frequency) -> TimedeltaIndex:
        return TimedeltaIndex(
            query_compiler=self._query_compiler.dt_floor(freq, include_index=True)
        )

    def ceil(self, freq: Frequency) -> TimedeltaIndex:
        return TimedeltaIndex(
            query_compiler=self._query_compiler.dt_ceil(freq, include_index=True)
        )

    @timedelta_index_not_implemented()
    def to_pytimedelta(self) -> np.ndarray:
        pass  # pragma: no cover

    def mean(
        self, *, skipna: bool = True, axis: AxisInt | None = 0
    ) -> native_pd.Timedelta:
        if axis:
            # Native pandas raises IndexError: tuple index out of range
            # We raise a different more user-friendly error message.
            raise ValueError(
                f"axis should be 0 for TimedeltaIndex.mean, found '{axis}'"
            )
        pandas_dataframe_result = (
            # reset_index(drop=False) copies the index column of
            # self._query_compiler into a new data column. Use `drop=False`
            # so that we don't have to use SQL row_number() to generate a new
            # index column.
            self._query_compiler.reset_index(drop=False)
            # Aggregate the data column.
            .agg("mean", axis=0, args=(), kwargs={"skipna": skipna})
            # convert the query compiler to a pandas dataframe with
            # dimensions 1x1 (note that the frame has a single row even
            # if `self` is empty.)
            .to_pandas()
        )
        assert pandas_dataframe_result.shape == (
            1,
            1,
        ), "Internal error: aggregation result is not 1x1."
        # Return the only element in the frame.
        return pandas_dataframe_result.iloc[0, 0]

    @timedelta_index_not_implemented()
    def as_unit(self, unit: str) -> TimedeltaIndex:
        pass  # pragma: no cover

    def total_seconds(self) -> Index:
        return Index(
            query_compiler=self._query_compiler.dt_total_seconds(include_index=True)
        )
