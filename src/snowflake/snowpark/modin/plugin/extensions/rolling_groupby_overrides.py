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


"""Implement RollingGroupby public API."""
from typing import Any, Union

import modin.pandas as pd


from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    series_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from .series_overrides import register_series_accessor
from pandas._libs import lib
from pandas._typing import AnyArrayLike


class RollingGroupby(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        by,
        window,
        min_periods: Union[int, None] = None,
        center: bool = False,
        win_type: Union[str, None] = None,
        on: Union[str, None] = None,
        axis: Union[int, str] = 0,
        closed: Union[str, None] = None,
        method: str = "single",
        dropna: bool = True,
    ) -> None:
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
            "dropna": dropna,
        }
        self._series_not_implemented()

    def _method_not_implemented(self, method: str):  # pragma: no cover
        ErrorMessage.not_implemented(
            f"Method {method} is not implemented for RollingGroupby!"
        )

    def _series_not_implemented(self):
        """
        Raises NotImplementedError if Groupby.rolling is called with a Series.
        """
        if self._dataframe.ndim == 1:
            ErrorMessage.not_implemented(
                "Snowpark pandas does not yet support the method GroupBy.rolling for Series"
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
        WarningMessage.warning_if_engine_args_is_set("rolling_max", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="max",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def mean(
        self,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("rolling_mean", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="mean",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def median(
        self,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("rolling_median", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="median",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def min(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("rolling_min", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="min",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def sum(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("rolling_sum", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="sum",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def count(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("rolling_count", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="count",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def sem(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("rolling_sem", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="sem",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def var(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("rolling_var", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="var",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def std(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        self._series_not_implemented()
        WarningMessage.warning_if_engine_args_is_set("rolling_std", args, kwargs)

        agg_kwargs = dict(numeric_only=numeric_only, min_count=min_count)
        is_series = not self._dataframe._is_dataframe

        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.groupby_rolling(
                rolling_kwargs=self.rolling_kwargs,
                rolling_method="std",
                groupby_kwargs=self.groupby_kwargs,
                is_series=is_series,
                agg_args=tuple(),
                agg_kwargs=agg_kwargs,
            )
        )

    def quantile(
        self,
        q: Union[float, AnyArrayLike] = 0.5,
        **kwargs: Any,
    ):
        self._method_not_implemented("quantile")

    def prod(
        self,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ):
        self._method_not_implemented("prod")
