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
from typing import Any, Callable, Optional, Union

import modin.pandas as pd
import pandas
import pandas.core.groupby
from pandas._libs import lib
from pandas._libs.lib import no_default
from pandas._typing import AggFuncType, T, AnyArrayLike

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
class ResamplerGroupby(metaclass=TelemetryMeta):
    def __init__(
        self,
        dataframe,
        by,
        rule,
        include_groups=True,
        axis=0,
        closed=None,
        label=None,
        convention="start",
        kind=None,
        on=None,
        level=None,
        origin="start_day",
        offset=None,
        group_keys=no_default,
    ) -> None:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._dataframe = dataframe
        self._query_compiler = dataframe._query_compiler
        self.by = by
        self.resample_kwargs = {
            "rule": rule,
            "axis": axis,
            "include_groups": include_groups,
            "closed": closed,
            "label": label,
            "convention": convention,
            "kind": kind,
            "on": on,
            "level": level,
            "origin": origin,
            "offset": offset,
            "group_keys": group_keys,
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

    def _get_groups(self):
        """
        Compute the resampled groups.

        Returns
        -------
        PandasGroupby
            Groups as specified by resampling arguments.
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("_get_groups")

    def __getitem__(self, key):  # pragma: no cover
        """
        Get ``Resampler`` based on `key` columns of original dataframe.

        Parameters
        ----------
        key : str or list
            String or list of selections.

        Returns
        -------
        modin.pandas.BasePandasDataset
            New ``Resampler`` based on `key` columns subset
            of the original dataframe.
        """

        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("__getitem__")

    ###########################################################################
    # Indexing, iteration
    ###########################################################################

    @property
    def groups(self):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("groups")
        # This property is currently not supported, and NotImplementedError will be
        # thrown before reach here. This is kept here because property function requires
        # a return value.
        return self._query_compiler.default_to_pandas(
            lambda df: pandas.DataFrame.groupby(by=self.by)
            .resample(df, **self.resample_kwargs)
            .groups
        )

    @property
    def indices(self):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("indices")

    def get_group(self, name, obj=None):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("get_group")

    ###########################################################################
    # Function application
    ###########################################################################

    def apply(
        self, func: Optional[AggFuncType] = None, *args: Any, **kwargs: Any
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("apply")

    def aggregate(
        self, func: Optional[AggFuncType] = None, *args: Any, **kwargs: Any
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("aggregate")

    agg = aggregate

    def transform(
        self,
        arg: Union[Callable[..., T], tuple[Callable[..., T], str]],
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("transform")

    def pipe(
        self,
        func: Union[Callable[..., T], tuple[Callable[..., T], str]],
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("pipe")

    ###########################################################################
    # Computations / descriptive stats
    ###########################################################################

    def count(self):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("count")

    def nunique(self, *args: Any, **kwargs: Any):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("nunique")

    def first(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        skipna: bool = True,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("first")

    def last(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        skipna: bool = True,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("last")

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
            query_compiler=self._query_compiler.groupby_resample(
                self.resample_kwargs,
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
            query_compiler=self._query_compiler.groupby_resample(
                self.resample_kwargs,
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
            query_compiler=self._query_compiler.groupby_resample(
                self.resample_kwargs,
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
            query_compiler=self._query_compiler.groupby_resample(
                self.resample_kwargs,
                "min",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def ohlc(self, *args: Any, **kwargs: Any):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("ohlc")

    def prod(
        self,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("prod")

    def size(self):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("size")

    def sem(
        self,
        ddof: int = 1,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("sem")

    def std(
        self,
        ddof: int = 1,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("std")

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
            query_compiler=self._query_compiler.groupby_resample(
                self.resample_kwargs,
                "sum",
                self.groupby_kwargs,
                is_series,
                tuple(),
                agg_kwargs,
            )
        )

    def var(
        self,
        ddof: int = 1,
        numeric_only: Union[bool, lib.NoDefault] = lib.no_default,
        *args: Any,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("var")

    def quantile(
        self,
        q: Union[float, AnyArrayLike] = 0.5,
        **kwargs: Any,
    ):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._method_not_implemented("quantile")
