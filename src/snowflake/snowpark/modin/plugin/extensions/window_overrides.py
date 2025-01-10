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

"""Implement Window and Rolling public API."""
from typing import Any, Literal, Optional, Union

import numpy as np  # noqa: F401
import pandas.core.window.rolling

from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    doc_replace_dataframe_with_link,
)


# TODO SNOW-1041934: Add support for more window aggregations
class Window(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        window: Any = None,
        min_periods: int = None,
        center: bool = False,
        win_type: str = None,
        on: str = None,
        axis: Union[int, str] = 0,
        closed: str = None,
        step: int = None,
        method: str = "single",
    ) -> None:
        # TODO: SNOW-1063357: Modin upgrade - modin.pandas.window.Window
        self._dataframe = dataframe
        self._query_compiler = dataframe._query_compiler
        self.window_kwargs = {  # pragma: no cover
            "window": window,
            "min_periods": min_periods,
            "center": center,
            "win_type": win_type,
            "on": on,
            "axis": axis,
            "closed": closed,
            "step": step,
            "method": method,
        }
        self.axis = axis

    def mean(self, *args, **kwargs):
        # TODO: SNOW-1063357: Modin upgrade - modin.pandas.window.Window
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.window_mean(
                self.axis, self.window_kwargs, *args, **kwargs
            )
        )

    def sum(self, *args, **kwargs):
        # TODO: SNOW-1063357: Modin upgrade - modin.pandas.window.Window
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.window_sum(
                self.axis, self.window_kwargs, *args, **kwargs
            )
        )

    def var(self, ddof=1, *args, **kwargs):
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.window_var(
                self.axis, self.window_kwargs, ddof, *args, **kwargs
            )
        )

    def std(self, ddof=1, *args, **kwargs):
        # TODO: SNOW-1063357: Modin upgrade - modin.pandas.window.Window
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.window_std(
                self.axis, self.window_kwargs, ddof, *args, **kwargs
            )
        )


@_inherit_docstrings(
    pandas.core.window.rolling.Rolling,
    excluded=[pandas.core.window.rolling.Rolling.__init__],
    modify_doc=doc_replace_dataframe_with_link,
)
# TODO SNOW-1041934: Add support for more window aggregations
class Rolling(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        window: Any,
        min_periods: Optional[int] = None,
        center: bool = False,
        win_type: Optional[str] = None,
        on: Optional[str] = None,
        axis: Union[int, str] = 0,
        closed: Optional[str] = None,
        step: Optional[int] = None,
        method: str = "single",
    ) -> None:
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        # Raise ValueError when invalid parameter values/combinations
        if (isinstance(window, int) and window < 0) or window is None:
            raise ValueError("window must be an integer 0 or greater")
        if not isinstance(center, bool):
            raise ValueError("center must be a boolean")
        if min_periods is not None and not isinstance(min_periods, int):
            raise ValueError("min_periods must be an integer")
        if isinstance(min_periods, int) and min_periods < 0:
            raise ValueError("min_periods must be >= 0")
        if (
            isinstance(min_periods, int)
            and isinstance(window, int)
            and min_periods > window
        ):
            raise ValueError(f"min_periods {min_periods} must be <= window {window}")

        self._dataframe = dataframe
        self._query_compiler = dataframe._query_compiler
        self.rolling_kwargs = {
            "window": window,
            "min_periods": min_periods,
            "center": center,
            "win_type": win_type,
            "on": on,
            "axis": axis,
            "closed": closed,
            "step": step,
            "method": method,
        }
        self.axis = axis
        if method != "single":
            WarningMessage.ignored_argument(
                operation="Rolling",
                argument="method",
                message="Snowpark pandas API executes on Snowflake. Ignoring engine related arguments to select a different execution engine.",
            )

    def _call_qc_method(self, method_name, *args, **kwargs):
        """
        Call a query compiler method for the specified rolling aggregation.

        Parameters
        ----------
        method_name : str
            Name of the aggregation.
        *args : tuple
            Positional arguments to pass to the query compiler method.
        **kwargs : dict
            Keyword arguments to pass to the query compiler method.

        Returns
        -------
        BaseQueryCompiler
            QueryCompiler holding the result of the aggregation.
        """
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        qc_method = getattr(self._query_compiler, f"rolling_{method_name}")
        return qc_method(self.axis, self.rolling_kwargs, *args, **kwargs)

    def _aggregate(self, method_name, *args, **kwargs):
        """
        Run the specified rolling aggregation.

        Parameters
        ----------
        method_name : str
            Name of the aggregation.
        *args : tuple
            Positional arguments to pass to the aggregation.
        **kwargs : dict
            Keyword arguments to pass to the aggregation.

        Returns
        -------
        DataFrame or Series
            Result of the aggregation.
        """
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        qc_result = self._call_qc_method(method_name, *args, **kwargs)
        return self._dataframe.__constructor__(query_compiler=qc_result)

    def count(self, numeric_only: bool = False):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(method_name="count", numeric_only=numeric_only)

    def sum(
        self,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="sum",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    def mean(
        self,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="mean",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    def median(
        self,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="median",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            **kwargs,
        )

    def var(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="var",
            ddof=ddof,
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    def std(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="std",
            ddof=ddof,
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    def min(
        self,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="min",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    def max(
        self,
        numeric_only: bool = False,
        *args: Any,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="max",
            numeric_only=numeric_only,
            engine=engine,
            engine_kwargs=engine_kwargs,
            *args,
            **kwargs,
        )

    def corr(
        self,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="corr",
            other=other,
            pairwise=pairwise,
            ddof=ddof,
            numeric_only=numeric_only,
            **kwargs,
        )

    def cov(
        self,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="cov",
            other=other,
            pairwise=pairwise,
            ddof=ddof,
            numeric_only=numeric_only,
            **kwargs,
        )

    def skew(
        self,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(method_name="skew", numeric_only=numeric_only, **kwargs)

    def kurt(
        self,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(method_name="kurt", numeric_only=numeric_only, **kwargs)

    def apply(
        self,
        func: Any,
        raw: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="apply",
            func=func,
            raw=raw,
            engine=engine,
            engine_kwargs=engine_kwargs,
            args=args,
            kwargs=kwargs,
        )

    def aggregate(
        self,
        func: Union[str, list, dict],
        *args: Any,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(method_name="aggregate", func=func, *args, **kwargs)

    agg = aggregate

    def quantile(
        self,
        quantile: float,
        interpolation: str = "linear",
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="quantile",
            quantile=quantile,
            interpolation=interpolation,
            numeric_only=numeric_only,
            **kwargs,
        )

    def sem(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="sem", ddof=ddof, numeric_only=numeric_only, *args, **kwargs
        )

    def rank(
        self,
        method: str = "average",
        ascending: bool = True,
        pct: bool = False,
        numeric_only: bool = False,
        **kwargs,
    ):
        # TODO: SNOW-1063358: Modin upgrade - modin.pandas.window.Rolling
        return self._aggregate(
            method_name="rank",
            method=method,
            ascending=ascending,
            pct=pct,
            numeric_only=numeric_only,
            **kwargs,
        )


@_inherit_docstrings(
    pandas.core.window.expanding.Expanding,
    excluded=[pandas.core.window.expanding.Expanding.__init__],
    modify_doc=doc_replace_dataframe_with_link,
)
class Expanding(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        min_periods: int = 1,
        axis: Union[int, str] = 0,
        method: str = "single",
    ) -> None:
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        if min_periods is not None and not isinstance(min_periods, int):
            raise ValueError("min_periods must be an integer")
        if isinstance(min_periods, int) and min_periods < 0:
            raise ValueError("min_periods must be >= 0")

        self._dataframe = dataframe
        self._query_compiler = dataframe._query_compiler
        self.expanding_kwargs = {
            "min_periods": min_periods,
            "axis": axis,
            "method": method,
        }
        self.axis = axis
        if method != "single":
            WarningMessage.ignored_argument(
                operation="Expanding",
                argument="method",
                message="Snowpark pandas API executes on Snowflake. Ignoring engine related arguments to select a different execution engine.",
            )

    def count(
        self,
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_count(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                numeric_only=numeric_only,
            )
        )

    def sum(
        self,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_sum(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                numeric_only=numeric_only,
                engine=engine,
                engine_kwargs=engine_kwargs,
            )
        )

    def mean(
        self,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_mean(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                numeric_only=numeric_only,
                engine=engine,
                engine_kwargs=engine_kwargs,
            )
        )

    def median(
        self,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_median(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                numeric_only=numeric_only,
                engine=engine,
                engine_kwargs=engine_kwargs,
            )
        )

    def var(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_var(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                ddof=ddof,
                numeric_only=numeric_only,
                engine=engine,
                engine_kwargs=engine_kwargs,
            )
        )

    def std(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_std(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                ddof=ddof,
                numeric_only=numeric_only,
                engine=engine,
                engine_kwargs=engine_kwargs,
            )
        )

    def min(
        self,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_min(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                numeric_only=numeric_only,
                engine=engine,
                engine_kwargs=engine_kwargs,
            )
        )

    def max(
        self,
        numeric_only: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_max(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                numeric_only=numeric_only,
                engine=engine,
                engine_kwargs=engine_kwargs,
            )
        )

    def corr(
        self,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_corr(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                other=other,
                pairwise=pairwise,
                ddof=ddof,
                numeric_only=numeric_only,
            )
        )

    def cov(
        self,
        other: Optional[SnowparkDataFrame] = None,
        pairwise: Optional[bool] = None,
        ddof: int = 1,
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_cov(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                other=other,
                pairwise=pairwise,
                ddof=ddof,
                numeric_only=numeric_only,
            )
        )

    def skew(
        self,
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_skew(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                numeric_only=numeric_only,
            )
        )

    def kurt(
        self,
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_kurt(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                numeric_only=numeric_only,
            )
        )

    def apply(
        self,
        func: Any,
        raw: bool = False,
        engine: Optional[Literal["cython", "numba"]] = None,
        engine_kwargs: Optional[dict[str, bool]] = None,
        args: Optional[tuple] = None,
        kwargs: Optional[dict] = None,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_apply(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                func=func,
                raw=raw,
                engine=engine,
                engine_kwargs=engine_kwargs,
                args=args,
                kwargs=kwargs,
            )
        )

    def aggregate(
        self,
        func: Any,
        *args,
        **kwargs,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_aggregate(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                func=func,
                *args,
                **kwargs,
            )
        )

    def quantile(
        self,
        quantile: float,
        interpolation: str = "linear",
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_quantile(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                quantile=quantile,
                interpolation=interpolation,
                numeric_only=numeric_only,
            )
        )

    def sem(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_sem(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                ddof=ddof,
                numeric_only=numeric_only,
            )
        )

    def rank(
        self,
        method: str = "average",
        ascending: bool = True,
        pct: bool = False,
        numeric_only: bool = False,
    ):
        # TODO: SNOW-1063366: Modin upgrade - modin.pandas.window.Expanding
        return self._dataframe.__constructor__(
            query_compiler=self._query_compiler.expanding_rank(
                fold_axis=self.axis,
                expanding_kwargs=self.expanding_kwargs,
                method=method,
                ascending=ascending,
                pct=pct,
                numeric_only=numeric_only,
            )
        )
