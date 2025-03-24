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
import collections
from typing import Any, Callable, Hashable, Optional, Union

import modin.pandas as pd
import numpy as np
import pandas
import pandas.core.resample
from pandas._libs import lib
from pandas._libs.lib import no_default
from pandas._typing import AggFuncType, T

from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    doc_replace_dataframe_with_link,
)


@_inherit_docstrings(
    pandas.core.resample.Resampler, modify_doc=doc_replace_dataframe_with_link
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
        # self.__groups = self._get_groups()

    def _method_not_implemented(self, method: str):  # pragma: no cover
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        raise ErrorMessage.not_implemented(
            f"Method {method} is not implemented for GroupbyResampler!"
        )

    def _validate_numeric_only_for_aggregate_methods(self, numeric_only):
        """
        When the caller object is Series (ndim == 1), it is not valid to call aggregation
        method with numeric_only = True.

        Raises:
            NotImplementedError if the above condition is encountered.
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        if self._dataframe.ndim == 1:
            if numeric_only and numeric_only is not lib.no_default:
                raise ErrorMessage.not_implemented(
                    "Series GroupbyResampler does not implement numeric_only."
                )

    def _get_groups(self):
        """
        Compute the resampled groups.

        Returns
        -------
        PandasGroupby
            Groups as specified by resampling arguments.
        """
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        df = self._dataframe if self.axis == 0 else self._dataframe.T
        groups = df.groupby(
            pandas.Grouper(
                key=self.resample_kwargs["on"],
                freq=self.resample_kwargs["rule"],
                closed=self.resample_kwargs["closed"],
                label=self.resample_kwargs["label"],
                convention=self.resample_kwargs["convention"],
                level=self.resample_kwargs["level"],
                origin=self.resample_kwargs["origin"],
                offset=self.resample_kwargs["offset"],
            ),
            group_keys=self.resample_kwargs["group_keys"],
        )
        return groups

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

        def _get_new_resampler(key):
            subset = self._dataframe[key]
            resampler = type(self)(subset, **self.resample_kwargs)
            return resampler

        from modin.pandas import Series

        if isinstance(key, (list, tuple, Series, pandas.Index, np.ndarray)):
            if len(self._dataframe.columns.intersection(key)) != len(set(key)):
                missed_keys = list(set(key).difference(self._dataframe.columns))
                raise KeyError(f"Columns not found: {str(sorted(missed_keys))[1:-1]}")
            return _get_new_resampler(list(key))

        if key not in self._dataframe:
            raise KeyError(f"Column not found: {key}")

        return _get_new_resampler(key)

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
            lambda df: pandas.DataFrame.resample(df, **self.resample_kwargs).groups
        )

    @property
    def indices(self) -> collections.defaultdict[Hashable, list]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        return self._query_compiler.groupby_resample(
            self.resample_kwargs,
            "indices",
            tuple(),
            dict(),
            False,
        )

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
        self._method_not_implemented("aggregate")

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
    # Upsampling
    ###########################################################################
    def sum(
        self,
        numeric_only: bool = False,
        min_count: int = 0,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
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

    def mean(
        self,
        numeric_only: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, pd.Series]:
        # TODO: SNOW-1063368: Modin upgrade - modin.pandas.resample.Resample
        self._validate_numeric_only_for_aggregate_methods(numeric_only)
        WarningMessage.warning_if_engine_args_is_set("resample_sum", args, kwargs)

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
