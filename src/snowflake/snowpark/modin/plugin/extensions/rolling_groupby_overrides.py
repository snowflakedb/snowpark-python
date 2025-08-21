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

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the pandas project, under the BSD 3-Clause License


"""Implement ResamplerGroupby public API."""
from typing import Any, Union

import modin.pandas as pd
import pandas.core.groupby


from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    series_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    doc_replace_dataframe_with_link,
)
from .series_overrides import register_series_accessor


@_inherit_docstrings(
    pandas.core.groupby.DataFrameGroupBy.resample,
    modify_doc=doc_replace_dataframe_with_link,
)
class RollingGroupby(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        by,
        window,
        min_periods: int | None = None,
        center: bool = False,
        win_type: str | None = None,
        on: str | None = None,
        axis: Union[int, str] = 0,
        closed: str | None = None,
        method: str = "single",
    ) -> None:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._dataframe = dataframe
        self._query_compiler = dataframe._query_compiler
        self.by = by
        self.rolling_kwargs = {
            "window": window,
            "min_periods": min_periods,
            "center": center,
            "win_type": win_type,
            "on": on,
            "axis": axis,
            "closed": closed,
            "method": method,
        }
        self.groupby_kwargs = {
            "by": by,
        }

    def _method_not_implemented(self, method: str):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        raise ErrorMessage.not_implemented(
            f"Method {method} is not implemented for GroupbyResampler!"
        )

    def _series_not_implemented(self):
        """
        When the caller object is Series (ndim == 1), it is not valid to call aggregation
        method with numeric_only = True.

        Raises:
            NotImplementedError if the above condition is encountered.
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        if self._dataframe.ndim == 1:
            raise ErrorMessage.not_implemented(
                "Series GroupbyResampler is not yet implemented."
            )
        func = series_not_implemented()(self.__class__)
        register_series_accessor(self.__class__.__name__)(func)
        return func

    def max(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_max", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "max",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def mean(
        self,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_mean", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "mean",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def median(
        self,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_median", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "median",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def min(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_min", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "min",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def sum(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_sum", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "sum",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def count(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_min", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "count",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def sem(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_min", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "sem",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def var(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_min", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "var",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def std(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("resample_min", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                self.rolling_kwargs,
                "std",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )
