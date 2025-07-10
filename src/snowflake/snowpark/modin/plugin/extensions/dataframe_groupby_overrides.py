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

"""Implement GroupBy public API as pandas does."""

from collections.abc import Hashable
import functools
from functools import cached_property
from typing import Any, Callable, Literal, Optional, Sequence, Union

import modin.pandas as pd
from modin.pandas.groupby import SeriesGroupBy, DataFrameGroupBy
import numpy as np  # noqa: F401
import numpy.typing as npt
import pandas
import pandas.core.groupby
from modin.pandas import Series
from pandas._libs.lib import NoDefault, no_default
from pandas._typing import (
    AggFuncType,
    Axis,
    FillnaOptions,
    IndexLabel,
    Level,
    TimedeltaConvertibleTypes,
    TimestampConvertibleTypes,
)
from pandas.core.dtypes.common import is_dict_like, is_list_like, is_numeric_dtype
from pandas.io.formats.printing import PrettyDict
from pandas.util._validators import validate_bool_kwarg

from snowflake.snowpark.modin.plugin._internal.apply_utils import (
    create_groupby_transform_func,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)

# the following import is used in doctests
from snowflake.snowpark.modin.plugin.extensions.utils import (
    extract_validate_and_try_convert_named_aggs_from_kwargs,
    raise_if_native_pandas_objects,
    validate_and_try_convert_agg_func_arg_func_to_str,
)
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    MODIN_UNNAMED_SERIES_LABEL,
    hashable,
    validate_int_kwarg,
)

from modin.pandas.api.extensions import (
    register_dataframe_groupby_accessor,
)

register_df_groupby_override = functools.partial(
    register_dataframe_groupby_accessor, backend="Snowflake"
)


@register_df_groupby_override("__init__")
def __init__(
    self,
    df,
    by,
    axis,
    level,
    as_index,
    sort,
    group_keys,
    idx_name,
    drop,
    # TODO MODIN_IS_AT_LEAST_0_34_0
    # backend_pinned did not exist in 0.33.x; after removing support for 0.33.x we should
    # remove the default value of backend_pinned and make the argument mandatory
    backend_pinned: bool = False,
    **kwargs,
) -> None:
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    self._axis = axis
    self._idx_name = idx_name
    self._df = df
    self._df._query_compiler.validate_groupby(by, axis, level)
    self._query_compiler = self._df._query_compiler
    self._columns = self._query_compiler.columns
    self._by = by
    self._drop = drop
    # When providing a list of columns of length one to DataFrame.groupby(),
    # the keys that are returned by iterating over the resulting DataFrameGroupBy
    # object will now be tuples of length one
    self._return_tuple_when_iterating = kwargs.pop("return_tuple_when_iterating", False)
    self._backend_pinned = backend_pinned
    self._level = level
    self._kwargs = {
        "level": level,
        "sort": sort,
        "as_index": as_index,
        "group_keys": group_keys,
    }
    self._kwargs.update(kwargs)
    if "apply_op" not in self._kwargs:
        # Can be "apply", "transform", "filter" or "aggregate"
        self._kwargs.update({"apply_op": "apply"})


@register_df_groupby_override("_override")
def _override(self, **kwargs):
    """
    Override groupby parameters.

    Parameters
    ----------
    **kwargs : dict
        Parameters to override.

    Returns
    -------
    DataFrameGroupBy
        A groupby object with new parameters.
    """
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    new_kw = dict(
        df=self._df,
        by=self._by,
        axis=self._axis,
        idx_name=self._idx_name,
        drop=self._drop,
        **self._kwargs,
    )
    new_kw.update(kwargs)
    return type(self)(**new_kw)


@register_df_groupby_override("ngroups")
@property
def ngroups(self):
    return self._query_compiler.groupby_ngroups(
        by=self._by,
        axis=self._axis,
        groupby_kwargs=self._kwargs,
    )


###########################################################################
# Indexing, iteration
###########################################################################


@register_df_groupby_override("groups")
@cached_property
def groups(self) -> PrettyDict[Hashable, "pd.Index"]:
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    return self._query_compiler.groupby_groups(
        self._by,
        self._axis,
        groupby_kwargs={
            # groupby.groups always treat as_index as True. this seems to be
            # intended behavior: https://github.com/pandas-dev/pandas/issues/56965
            k: True if k == "as_index" else v
            for k, v in self._kwargs.items()
        },
    )


@register_df_groupby_override("indices")
@property
def indices(self) -> dict[Hashable, npt.NDArray[np.intp]]:
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    return self._query_compiler.groupby_indices(
        self._by,
        self._axis,
        groupby_kwargs={
            # groupby.indices always treat as_index as True. this seems to be
            # intended behavior: https://github.com/pandas-dev/pandas/issues/56965
            k: True if k == "as_index" else v
            for k, v in self._kwargs.items()
        },
    )


###########################################################################
# Function application
###########################################################################


@register_df_groupby_override("apply")
def apply(self, func, *args, include_groups=True, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    # TODO: SNOW-1244717: Explore whether window function are performant and can be used
    #       whenever `func` is an aggregation function.
    if not callable(func):
        raise NotImplementedError("No support for non-callable `func`")
    dataframe_result = pd.DataFrame(
        query_compiler=self._query_compiler.groupby_apply(
            self._by,
            agg_func=func,
            axis=self._axis,
            groupby_kwargs=self._kwargs,
            agg_args=args,
            agg_kwargs=kwargs,
            series_groupby=False,
            include_groups=include_groups,
        )
    )
    if dataframe_result.columns.equals(pandas.Index([MODIN_UNNAMED_SERIES_LABEL])):
        return dataframe_result.squeeze(axis=1)
    return dataframe_result


@register_df_groupby_override("aggregate")
def aggregate(
    self,
    func: Optional[AggFuncType] = None,
    *args: Any,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
    **kwargs: Any,
):
    WarningMessage.warning_if_engine_args_is_set(
        "groupby_aggregate", engine, engine_kwargs
    )
    if self._axis != 0 and (is_dict_like(func) or is_list_like(func)):
        # This is the same as pandas for func that is a list or dict
        ErrorMessage.not_implemented(
            "axis other than 0 is not supported"
        )  # pragma: no cover
    if func is None:
        # When func is None, we assume that the aggregation functions have been passed in via named aggregations,
        # which can be of the form named_agg=('col_name', 'agg_func') or named_agg=pd.NamedAgg('col_name', 'agg_func').
        # We need to parse out the following three things:
        # 1. The new label to apply to the result of the aggregation.
        # 2. The column to apply the aggregation over.
        # 3. The aggregation to apply.
        # This function checks that:
        # 1. The kwargs contain named aggregations.
        # 2. The kwargs do not contain anything besides named aggregations. (for pandas compatibility - see function for more details.)
        # If both of these things are true, it then extracts the named aggregations from the kwargs, and returns a dictionary that contains
        # a mapping from the column pandas labels to apply the aggregation over (2 above) to a tuple containing the aggregation to apply
        # and the new label to assign it (1 and 3 above). Take for example, the following call:
        # df.groupby(...).agg(new_col1=('A', 'min'), new_col2=('B', 'max'), new_col3=('A', 'max'))
        # After this function returns, func will look like this:
        # {
        #   "A": [AggFuncWithLabel(func="min", pandas_label="new_col1"), AggFuncWithLabel(func="max", pandas_label="new_col3")],
        #   "B": AggFuncWithLabel(func="max", pandas_label="new_col2")
        # }
        # This remapping causes an issue with ordering though - the dictionary above will be processed in the following order:
        # 1. apply "min" to "A" and name it "new_col1"
        # 2. apply "max" to "A" and name it "new_col3"
        # 3. apply "max" to "B" and name it "new_col2"
        # In other words - the order is slightly shifted so that named aggregations on the same column are contiguous in the ordering
        # although the ordering of the kwargs is used to determine the ordering of named aggregations on the same columns. Since
        # the reordering for groupby agg is a reordering of columns, its relatively cheap to do after the aggregation is over,
        # rather than attempting to preserve the order of the named aggregations internally.
        func = extract_validate_and_try_convert_named_aggs_from_kwargs(
            obj=self,
            allow_duplication=True,
            axis=self._axis,
            **kwargs,
        )
    else:
        func = validate_and_try_convert_agg_func_arg_func_to_str(
            agg_func=func,
            obj=self,
            allow_duplication=True,
            axis=self._axis,
        )

    if isinstance(func, str):
        # Using "getattr" here masks possible AttributeError which we throw
        # in __getattr__, so we should call __getattr__ directly instead.
        agg_func = self.__getattr__(func)
        if callable(agg_func):
            return agg_func(*args, **kwargs)

    # when the aggregation function passed in is list like always return a Dataframe regardless
    # it is SeriesGroupBy or DataFrameGroupBy
    is_result_dataframe = (self.ndim == 2) or is_list_like(func)
    result = self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        numeric_only=False,
        agg_func=func,
        agg_args=args,
        agg_kwargs=kwargs,
        how="axis_wise",
        is_result_dataframe=is_result_dataframe,
    )

    return result


register_df_groupby_override("agg")(aggregate)


@register_df_groupby_override("transform")
def transform(
    self,
    func: Union[str, Callable],
    *args: Any,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
    **kwargs: Any,
) -> "pd.DataFrame":
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    WarningMessage.warning_if_engine_args_is_set(
        "groupby_transform", engine, engine_kwargs
    )

    # The resultant DataFrame from `transform` has an index that always matches the original
    # DataFrame's index.
    # Create a new groupby object so that we can tune parameters to ensure that `apply`
    # returns a DataFrame with the required index (same as original DataFrame).
    #
    # Unlike `transform`, groupby parameters affect the result of `apply`.
    # - `group_keys` controls whether the grouped column(s) are included in the index.
    #   `group_keys` needs to be False to ensure that the resultant DataFrame has the
    #   original DataFrame's index.
    #
    # - `dropna` controls whether the NA values should be included as a group/be present
    #    in the group keys. `transform` always includes the NA values, therefore `dropna`
    #    needs to be False to ensure that all NA values are included.
    #
    # - `sort` controls whether the group keys are sorted.
    #
    # - `as_index` controls whether the groupby object has group labels as the index.
    by = self._by
    level = self._level
    groupby_obj = self._df.groupby(
        by=by,  # either by or levels can be specified at a time
        level=level,
        as_index=self._as_index,
        group_keys=False,
        dropna=False,
        sort=self._sort,
    )
    groupby_obj._kwargs["apply_op"] = "transform"

    # Apply the transform function to each group.
    res = groupby_obj.apply(
        create_groupby_transform_func(func, by, level, *args, **kwargs)
    )

    dropna = self._kwargs.get("dropna", True)
    if dropna is True:
        # - To avoid dropping any NA values, `dropna` is set to False in both the groupby
        #   object created above and the groupby object created in `create_groupby_transform_func`.
        #
        # - If dropna is set to True in the groupby object, the output from this code (so far)
        #   and the expected native pandas result differs.
        #
        # - In the Snowpark pandas code, all rows grouped under NA keys calculate the result with
        #   the given `func`, thus resulting in non-NA values.
        #
        # - In the native pandas version, all rows grouped under NA keys take up
        #   "NaN" values in all columns.
        #
        # Therefore, we need to convert the rows grouped under NA keys to have NaN values in
        # all columns.
        na_col_data = self._df[by].isna()
        condition = (
            na_col_data.any(axis=1)
            if isinstance(na_col_data, pd.DataFrame)
            else na_col_data
        )
        res.loc[condition, :] = np.nan

    return res


@register_df_groupby_override("pipe")
def pipe(self, func, *args, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="pipe", class_="GroupBy")


@register_df_groupby_override("filter")
def filter(self, func, dropna=True, *args, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="filter", class_="GroupBy")


###########################################################################
# Computations / descriptive stats
###########################################################################


@register_df_groupby_override("bfill")
def bfill(self, limit=None):
    is_series_groupby = self.ndim == 1

    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    query_compiler = self._query_compiler.groupby_fillna(
        self._by,
        self._axis,
        self._kwargs,
        value=None,
        method="bfill",
        fill_axis=None,
        inplace=False,
        limit=limit,
        downcast=None,
    )
    return (
        pd.Series(query_compiler=query_compiler)
        if is_series_groupby
        else pd.DataFrame(query_compiler=query_compiler)
    )


@register_df_groupby_override("corr")
def corr(self, method="pearson", min_periods=1, numeric_only=False):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="corr", class_="GroupBy")


@register_df_groupby_override("corrwith")
@property
def corrwith(self):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="corrwith", class_="GroupBy")


@register_df_groupby_override("count")
def count(self):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    result = self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        numeric_only=False,
        agg_func="count",
    )
    return result


@register_df_groupby_override("cov")
def cov(self, min_periods=None, ddof=1, numeric_only=False):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="cov", class_="GroupBy")


@register_df_groupby_override("cumcount")
def cumcount(self, ascending: bool = True):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    query_compiler = self._query_compiler.groupby_cumcount(
        self._by, self._axis, self._kwargs, ascending
    )
    return pd.Series(query_compiler=query_compiler)


@register_df_groupby_override("cummax")
def cummax(self, axis: Axis = 0, numeric_only: bool = False, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    qc = self._query_compiler.groupby_cummax(
        self._by, self._axis, numeric_only, self._kwargs
    )
    return (
        pd.Series(query_compiler=qc)
        if self.ndim == 1
        else pd.DataFrame(query_compiler=qc)
    )


@register_df_groupby_override("cummin")
def cummin(self, axis: Axis = 0, numeric_only: bool = False, **kwargs):
    qc = self._query_compiler.groupby_cummin(
        self._by, self._axis, numeric_only, self._kwargs
    )
    return (
        pd.Series(query_compiler=qc)
        if self.ndim == 1
        else pd.DataFrame(query_compiler=qc)
    )


@register_df_groupby_override("cumprod")
def cumprod(self, axis=0, *args, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="cumprod", class_="GroupBy")


@register_df_groupby_override("cumsum")
def cumsum(self, axis: Axis = 0, *args, **kwargs):
    qc = self._query_compiler.groupby_cumsum(self._by, self._axis, self._kwargs)
    return (
        pd.Series(query_compiler=qc)
        if self.ndim == 1
        else pd.DataFrame(query_compiler=qc)
    )


@register_df_groupby_override("describe")
def describe(self, percentiles=None, include=None, exclude=None):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="describe", class_="GroupBy")


@register_df_groupby_override("diff")
def diff(self, periods=1, axis=no_default):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="diff", class_="GroupBy")


@register_df_groupby_override("ffill")
def ffill(self, limit=None):
    is_series_groupby = self.ndim == 1

    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    query_compiler = self._query_compiler.groupby_fillna(
        self._by,
        self._axis,
        self._kwargs,
        value=None,
        method="ffill",
        fill_axis=None,
        inplace=False,
        limit=limit,
        downcast=None,
    )
    return (
        pd.Series(query_compiler=query_compiler)
        if is_series_groupby
        else pd.DataFrame(query_compiler=query_compiler)
    )


@register_df_groupby_override("fillna")
def fillna(
    self,
    value: Any = None,
    method: Optional[FillnaOptions] = None,
    axis: Optional[Axis] = None,
    inplace: Optional[bool] = False,
    limit: Optional[int] = None,
    downcast: Optional[dict] = None,
):
    is_series_groupby = self.ndim == 1

    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    query_compiler = self._query_compiler.groupby_fillna(
        self._by,
        self._axis,
        self._kwargs,
        value,
        method,
        axis,
        inplace,
        limit,
        downcast,
    )
    return (
        pd.Series(query_compiler=query_compiler)
        if is_series_groupby
        else pd.DataFrame(query_compiler=query_compiler)
    )


@register_df_groupby_override("head")
def head(self, n=5):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    # Ensure that n is an integer value.
    if not isinstance(n, int):
        raise TypeError("n must be an integer value.")

    # Only the groupby parameter "dropna" affects the output of head. None of the other groupby
    # parameters: as_index, sort, and group_keys, affect head.
    # Values needed for the helper functions.
    agg_kwargs = {
        "n": n,
        "level": self._level,
        "dropna": self._kwargs.get("dropna", True),
    }

    result = self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        agg_func="head",
        agg_kwargs=agg_kwargs,
    )
    return pd.DataFrame(result)


@register_df_groupby_override("idxmax")
def idxmax(
    self, axis: Axis = no_default, skipna: bool = True, numeric_only: bool = False
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    axis_number = self._df._get_axis_number(axis)
    if axis_number == 1:
        # Performing idxmax is deprecated and will be removed in a future pandas version.
        raise NotImplementedError(
            "DataFrameGroupBy.idxmax with axis=1 is deprecated and will be removed in a "
            "future version. Operate on the un-grouped DataFrame instead."
        )
    else:
        # When performing idxmax/idxmin on axis=0, it can be done column-wise.
        result = self._wrap_aggregation(
            qc_method=type(self._query_compiler).groupby_agg,
            numeric_only=numeric_only,
            how="axis_wise",
            agg_func="idxmax",
            # axis is also specified here since the axis used with idxmax/idxmin is different from
            # the groupby axis.
            agg_kwargs=dict(skipna=skipna, axis=0),
        )
    return result


@register_df_groupby_override("idxmin")
def idxmin(
    self, axis: Axis = no_default, skipna: bool = True, numeric_only: bool = False
) -> Series:
    axis_number = self._df._get_axis_number(axis)
    if axis_number == 1:
        # Performing idxmin is deprecated and will be removed in a future pandas version.
        raise NotImplementedError(
            "DataFrameGroupBy.idxmin with axis=1 is deprecated and will be removed in a "
            "future version. Operate on the un-grouped DataFrame instead."
        )
    else:
        # When performing idxmax/idxmin on axis=0, it can be done column-wise.
        result = self._wrap_aggregation(
            qc_method=type(self._query_compiler).groupby_agg,
            numeric_only=numeric_only,
            how="axis_wise",
            agg_func="idxmin",
            # axis is also specified here since the axis used with idxmax/idxmin is different from
            # the groupby axis.
            agg_kwargs=dict(skipna=skipna, axis=0),
        )
    return result


@register_df_groupby_override("max")
def max(
    self,
    numeric_only: bool = False,
    min_count: int = -1,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    WarningMessage.warning_if_engine_args_is_set("groupby_max", engine, engine_kwargs)
    validate_int_kwarg(min_count, "min_count", float_allowed=False)
    return self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        numeric_only=numeric_only,
        agg_func="max",
        agg_kwargs=dict(min_count=min_count, numeric_only=numeric_only),
    )


@register_df_groupby_override("mean")
def mean(
    self,
    numeric_only: bool = False,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    WarningMessage.warning_if_engine_args_is_set("groupby_mean", engine, engine_kwargs)
    return self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        numeric_only=numeric_only,
        agg_func="mean",
        agg_kwargs=dict(numeric_only=numeric_only),
    )


@register_df_groupby_override("median")
def median(self, numeric_only: bool = False):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    return self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        numeric_only=numeric_only,
        agg_func="median",
        agg_kwargs=dict(numeric_only=numeric_only),
    )


@register_df_groupby_override("min")
def min(
    self,
    numeric_only: bool = False,
    min_count: int = -1,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
):
    WarningMessage.warning_if_engine_args_is_set("groupby_min", engine, engine_kwargs)
    validate_int_kwarg(min_count, "min_count", float_allowed=False)
    return self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        numeric_only=numeric_only,
        agg_func="min",
        agg_kwargs=dict(min_count=min_count, numeric_only=numeric_only),
    )


@register_df_groupby_override("ngroup")
def ngroup(self, ascending=True):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="ngroup", class_="GroupBy")


@register_df_groupby_override("nth")
def nth(self, n, dropna=None):
    ErrorMessage.method_not_implemented_error(name="nth", class_="GroupBy")


@register_df_groupby_override("nunique")
def nunique(self, dropna=True):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    return self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_nunique,
        agg_func="nunique",
        agg_kwargs=dict(dropna=dropna),
    )


@register_df_groupby_override("ohlc")
def ohlc(self):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="ohlc", class_="GroupBy")


@register_df_groupby_override("pct_change")
def pct_change(
    self,
    periods=1,
    fill_method=no_default,
    limit=no_default,
    freq=no_default,
    axis=no_default,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    if fill_method not in (no_default, None) or limit is not no_default:
        WarningMessage.single_warning(
            "The 'fill_method' keyword being not None and the 'limit' keyword in "
            + f"{type(self).__name__}.pct_change are deprecated and will be removed "
            + "in a future version. Either fill in any non-leading NA values prior "
            + "to calling pct_change or specify 'fill_method=None' to not fill NA "
            + "values.",
        )
    if fill_method is no_default:
        WarningMessage.single_warning(
            f"The default fill_method='ffill' in {type(self).__name__}.pct_change is "
            + "deprecated and will be removed in a future version. Either fill in any "
            + "non-leading NA values prior to calling pct_change or specify 'fill_method=None' "
            + "to not fill NA values.",
        )
        fill_method = "ffill"

    if limit is no_default:
        limit = None

    if freq is no_default:
        freq = None

    if axis is not no_default:
        axis = self._df._get_axis_number(axis)
    else:
        axis = 0

    if not isinstance(periods, int):
        raise TypeError(f"Periods must be integer, but {periods} is {type(periods)}.")

    return self._wrap_aggregation(
        type(self._query_compiler).groupby_pct_change,
        agg_kwargs=dict(
            periods=periods,
            fill_method=fill_method,
            limit=limit,
            freq=freq,
            axis=axis,
        ),
    )


@register_df_groupby_override("prod")
def prod(self, numeric_only=False, min_count=0):
    ErrorMessage.method_not_implemented_error(name="prod", class_="GroupBy")


@register_df_groupby_override("quantile")
def quantile(self, q=0.5, interpolation="linear", numeric_only=False):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    return self._wrap_aggregation(
        type(self._query_compiler).groupby_agg,
        numeric_only=numeric_only,
        agg_func="quantile",
        agg_kwargs=dict(q=q, interpolation=interpolation),
    )


@register_df_groupby_override("rank")
def rank(
    self,
    method: str = "average",
    ascending: bool = True,
    na_option: str = "keep",
    pct: bool = False,
    *args,
    **kwargs,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    query_compiler = self._query_compiler.groupby_rank(
        by=self._by,
        axis=self._axis,
        method=method,
        na_option=na_option,
        ascending=ascending,
        pct=pct,
        groupby_kwargs=self._kwargs,
        agg_args=args,
        agg_kwargs=kwargs,
    )
    if self.ndim == 1:
        result = pd.Series(query_compiler=query_compiler)
    else:
        result = pd.DataFrame(query_compiler=query_compiler)
    return result


@register_df_groupby_override("resample")
def resample(
    self,
    rule,
    axis: int = 0,
    closed: str = None,
    label: str = None,
    convention: str = "start",
    kind: str = None,
    on: Level = None,
    level: Level = None,
    origin: [str, TimestampConvertibleTypes] = "start_day",
    offset: TimedeltaConvertibleTypes = None,
    group_keys=no_default,
    *args,
    include_groups: bool = True,
    **kwargs,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    from snowflake.snowpark.modin.plugin.extensions.resampler_groupby_overrides import (
        ResamplerGroupby,
    )

    return ResamplerGroupby(
        dataframe=self._df,
        by=self._by,
        rule=rule,
        include_groups=include_groups,
        axis=axis,
        closed=closed,
        label=label,
        convention=convention,
        kind=kind,
        on=on,
        level=level,
        origin=origin,
        offset=offset,
        group_keys=group_keys,
    )


@register_df_groupby_override("rolling")
def rolling(self, *args, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="rolling", class_="GroupBy")


@register_df_groupby_override("sample")
def sample(self, n=None, frac=None, replace=False, weights=None, random_state=None):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="sample", class_="GroupBy")


@register_df_groupby_override("sem")
def sem(self, ddof=1, numeric_only=False):
    ErrorMessage.method_not_implemented_error(name="sem", class_="GroupBy")


@register_df_groupby_override("shift")
def shift(
    self,
    periods: Union[int, Sequence[int]] = 1,
    freq: int = None,
    axis: Axis = 0,
    fill_value: Any = None,
    suffix: Optional[str] = None,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    if isinstance(periods, Sequence):
        ErrorMessage.not_implemented(
            "Snowpark pandas GroupBy.shift does not yet support `periods` that are sequences. Only int `periods` are supported."
        )
    if suffix is not None:
        ErrorMessage.not_implemented(
            "Snowpark pandas GroupBy.shift does not yet support the `suffix` parameter"
        )
    if not isinstance(periods, int):
        raise TypeError(f"Periods must be integer, but {periods} is {type(periods)}.")
    qc = self._query_compiler.groupby_shift(
        self._by, self._axis, self._level, periods, freq, fill_value, self.ndim == 1
    )
    return (
        pd.Series(query_compiler=qc)
        if self.ndim == 1
        else pd.DataFrame(query_compiler=qc)
    )


@register_df_groupby_override("size")
def size(self):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    result = self._wrap_aggregation(
        type(self._query_compiler).groupby_size,
        numeric_only=False,
    )
    if not isinstance(result, Series):
        result = result.squeeze(axis=1)
    if not self._kwargs.get("as_index") and not isinstance(result, Series):
        result = (
            result.rename(columns={MODIN_UNNAMED_SERIES_LABEL: "index"})
            if MODIN_UNNAMED_SERIES_LABEL in result.columns
            else result
        )
    elif isinstance(self._df, Series):
        result.name = self._df.name
    return result


@register_df_groupby_override("skew")
def skew(self, axis=no_default, skipna=True, numeric_only=False, **kwargs):
    ErrorMessage.method_not_implemented_error(name="skew", class_="GroupBy")


@register_df_groupby_override("std")
def std(
    self,
    ddof: int = 1,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
    numeric_only: bool = False,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    WarningMessage.warning_if_engine_args_is_set("groupby_std", engine, engine_kwargs)
    return self._wrap_aggregation(
        qc_method=SnowflakeQueryCompiler.groupby_agg,
        numeric_only=numeric_only,
        agg_func="std",
        agg_kwargs=dict(ddof=ddof, numeric_only=numeric_only),
    )


@register_df_groupby_override("sum")
def sum(
    self,
    numeric_only: bool = False,
    min_count: int = 0,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    WarningMessage.warning_if_engine_args_is_set("groupby_sum", engine, engine_kwargs)
    validate_int_kwarg(min_count, "min_count", float_allowed=False)
    return self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        numeric_only=numeric_only,
        agg_func="sum",
        agg_kwargs=dict(min_count=min_count, numeric_only=numeric_only),
    )


@register_df_groupby_override("var")
def var(
    self,
    ddof: int = 1,
    engine: Optional[Literal["cython", "numba"]] = None,
    engine_kwargs: Optional[dict[str, bool]] = None,
    numeric_only: bool = False,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    WarningMessage.warning_if_engine_args_is_set("groupby_var", engine, engine_kwargs)

    return self._wrap_aggregation(
        qc_method=SnowflakeQueryCompiler.groupby_agg,
        numeric_only=numeric_only,
        agg_func="var",
        agg_kwargs=dict(ddof=ddof, numeric_only=numeric_only),
    )


@register_df_groupby_override("tail")
def tail(self, n=5):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    # Ensure that n is an integer value.
    if not isinstance(n, int):
        raise TypeError("n must be an integer value.")

    # Only the groupby parameter "dropna" affects the output of tail. None of the other groupby
    # parameters: as_index, sort, and group_keys, affect tail.
    # Values needed for the helper functions.
    agg_kwargs = {
        "n": n,
        "level": self._level,
        "dropna": self._kwargs.get("dropna", True),
    }

    result = self._wrap_aggregation(
        qc_method=type(self._query_compiler).groupby_agg,
        agg_func="tail",
        agg_kwargs=agg_kwargs,
    )
    return pd.DataFrame(result)


@register_df_groupby_override("take")
def take(self, indices, axis=no_default, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="take", class_="GroupBy")


@register_df_groupby_override("ewm")
def ewm(self, *args, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="ewm", class_="GroupBy")


@register_df_groupby_override("value_counts")
def value_counts(
    self,
    subset: Optional[list[str]] = None,
    normalize: bool = False,
    sort: bool = True,
    ascending: bool = False,
    dropna: bool = True,
):
    query_compiler = self._query_compiler.groupby_value_counts(
        by=self._by,
        axis=self._axis,
        groupby_kwargs=self._kwargs,
        subset=subset,
        normalize=normalize,
        sort=sort,
        ascending=ascending,
        dropna=dropna,
    )
    if self._as_index:
        return pd.Series(
            query_compiler=query_compiler,
            name="proportion" if normalize else "count",
        )
    return pd.DataFrame(query_compiler=query_compiler)


###########################################################################
# Plotting and visualization
###########################################################################


@register_df_groupby_override("boxplot")
def boxplot(
    self,
    grouped,
    subplots=True,
    column=None,
    fontsize=None,
    rot=0,
    grid=True,
    ax=None,
    figsize=None,
    layout=None,
    sharex=False,
    sharey=True,
    backend=None,
    **kwargs,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="boxplot", class_="GroupBy")


@register_df_groupby_override("hist")
def hist(
    self,
    column=None,
    by=None,
    grid=True,
    xlabelsize=None,
    xrot=None,
    ylabelsize=None,
    yrot=None,
    ax=None,
    sharex=False,
    sharey=False,
    figsize=None,
    layout=None,
    bins=10,
    backend=None,
    legend=False,
    **kwargs,
):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="hist", class_="GroupBy")


@register_df_groupby_override("plot")
@property
def plot(self):  # pragma: no cover
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="plot", class_="GroupBy")


@register_df_groupby_override("__bytes__")
def __bytes__(self):
    """
    Convert DataFrameGroupBy object into a python2-style byte string.

    Returns
    -------
    bytearray
        Byte array representation of `self`.

    Notes
    -----
    Deprecated and removed in pandas and will be likely removed in Modin.
    """
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="__bytes__", class_="GroupBy")


@register_df_groupby_override("dtypes")
@property
def dtypes(self):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="dtypes", class_="GroupBy")


@register_df_groupby_override("_internal_by")
@cached_property
def _internal_by(self):
    """
    Get only those components of 'by' that are column labels of the source frame.

    Returns
    -------
    tuple of labels
    """
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    by_list = self._by if is_list_like(self._by) else [self._by]

    internal_by = []
    for by in by_list:
        by = by.key if isinstance(by, pandas.Grouper) else by
        if hashable(by) and by in self._columns:
            internal_by.append(by)
    return tuple(internal_by)


@register_df_groupby_override("__getitem__")
def __getitem__(self, key):
    """
    Implement indexing operation on a DataFrameGroupBy object.

    Parameters
    ----------
    key : list or str
        Names of columns to use as subset of original object.

    Returns
    -------
    DataFrameGroupBy or SeriesGroupBy
        Result of indexing operation.

    Raises
    ------
    NotImplementedError
        Column lookups on GroupBy with arbitrary Series in by is not yet supported.
    """
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    if self._axis == 1:
        raise ValueError("Cannot subset columns when using axis=1")

    # These parameters are common for building the resulted Series or DataFrame groupby object
    kwargs = {
        **self._kwargs.copy(),
        "by": self._by,
        "axis": self._axis,
        "idx_name": self._idx_name,
    }
    # The rules of type deduction for the resulted object is the following:
    #   1. If `key` is a list-like, then the resulted object is a DataFrameGroupBy
    #   2. Otherwise, the resulted object is SeriesGroupBy
    #   3. Result type does not depend on the `by` origin
    # Examples:
    #   - drop: any, as_index: any, __getitem__(key: list_like) -> DataFrameGroupBy
    #   - drop: any, as_index: False, __getitem__(key: list_like) -> DataFrameGroupBy
    #   - drop: any, as_index: False, __getitem__(key: label) -> SeriesGroupBy
    #   - drop: any, as_index: True, __getitem__(key: label) -> SeriesGroupBy
    if is_list_like(key):
        make_dataframe = True
    else:
        make_dataframe = False
        key = [key]

    column_index = self._df.columns
    # validate that all keys are labels belong to the data column of the df
    for label in key:
        if not (label in column_index):
            raise KeyError(f"Columns not found: '{label}'")

    # internal_by records all label in by that belongs to the data columns
    internal_by = frozenset(self._internal_by)
    if len(internal_by.intersection(key)) != 0:
        message = (
            "Data column selection with overlap of 'by' columns is not yet supported, "
            "please duplicate the overlapped by columns and rename it to a different name"
        )
        ErrorMessage.not_implemented(message=message)

    # select the union of the internal bys and select keys. Here we find all integer
    # positions for all the selected columns, and then call iloc to select all columns.
    # This is because loc currently doesn't support select with multiindex, once iloc and
    # dataframe getitem is supported, this can be replaced with df[list(internal_by) + list(key)]
    # TODO (SNOW-896342): update self._df.iloc[:, ilocs_list] to use df[list(internal_by) + list(key)]
    #           once dataframe getitem is supported.
    _, by_ilocs = column_index._get_indexer_strict(list(internal_by), "columns")
    _, key_ilocs = column_index._get_indexer_strict(list(key), "columns")
    ilocs_list = list(by_ilocs) + list(key_ilocs)

    if len(key_ilocs) > 1:
        make_dataframe = True

    if make_dataframe:
        return DataFrameGroupBy(
            self._df.iloc[:, ilocs_list],
            drop=self._drop,
            backend_pinned=self._backend_pinned,
            **kwargs,
        )
    else:
        return SeriesGroupBy(
            self._df.iloc[:, ilocs_list],
            drop=self._drop,
            backend_pinned=self._backend_pinned,
            **kwargs,
        )


@register_df_groupby_override("__len__")
def __len__(self):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="__len__", class_="GroupBy")


# expanding and rolling are unique cases and need to likely be handled
# separately. They do not appear to be commonly used.
@register_df_groupby_override("expanding")
def expanding(self, *args, **kwargs):
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    ErrorMessage.method_not_implemented_error(name="expanding", class_="GroupBy")


@register_df_groupby_override("_iter")
@property
def _iter(self):
    """
    Construct a tuple of (group_id, DataFrame) tuples to allow iteration over groups.

    Returns
    -------
    generator
        Generator expression of GroupBy object broken down into tuples for iteration.
    """
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    indices = self.indices
    group_ids = indices.keys()

    assert self._axis == 0, (
        "GroupBy does not yet support axis=1. "
        "A NotImplementedError should have already been raised."
    )

    return (
        (
            (k,) if self._return_tuple_when_iterating else k,
            pd.DataFrame(
                query_compiler=self._query_compiler.getitem_row_array(indices[k])
            ),
        )
        for k in (sorted(group_ids) if self._sort else group_ids)
    )


@register_df_groupby_override("_wrap_aggregation")
def _wrap_aggregation(
    self,
    qc_method: Callable,
    numeric_only: bool = False,
    agg_args: list[Any] = None,
    agg_kwargs: dict[str, Any] = None,
    is_result_dataframe: Optional[bool] = None,
    **kwargs: Any,
):
    """
    Perform common metadata transformations and apply groupby functions.

    Parameters
    ----------
    qc_method : callable
        The query compiler method to call.
    numeric_only : bool, default: False
        Specifies whether to aggregate non numeric columns:
            - True: include only numeric columns (including categories that holds a numeric dtype)
            - False: include all columns
    agg_args : list-like, optional
        Positional arguments to pass to the aggregation function.
    agg_kwargs : dict-like, optional
        Keyword arguments to pass to the aggregation function.
    is_result_dataframe: bool optional
        whether the result of aggregation is a dataframe or series. If None, is_result_dataframe will be
        False for SeriesGroupBy, and True for DataFrameGroupBy.
    **kwargs : dict
        Keyword arguments to pass to the specified query compiler's method.

    Returns
    -------
    DataFrame or Series
        Returns the same type as `self._df`.
    """
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    try:
        numeric_only = validate_bool_kwarg(
            numeric_only, "numeric_only", none_allowed=True
        )
    except ValueError:
        # SNOW-1429199: Snowpark users expect to be able to pass in the column to aggregate
        # on in the aggregation method, e.g. df.groupby("COL0").sum("COL1"), but the pandas
        # API's only accept the numeric_only argument, so users get an error complaining that
        # the numeric_only kwarg expects a bool argument, but a string was passed in. Instead
        # of that error, we can throw this error instead that will make it more clear to users
        # what went wrong.
        raise ValueError(
            f"GroupBy aggregations like 'sum' take a 'numeric_only' argument that needs to be a bool, but a {type(numeric_only).__name__} value was passed in."
        )

    agg_args = tuple() if agg_args is None else agg_args
    agg_kwargs = dict() if agg_kwargs is None else agg_kwargs

    is_series_groupby = self.ndim == 1
    if is_series_groupby:
        # when ndim is 1, it is SeriesGroupBy. SeriesGroupBy does not implement numeric_only
        # parameter even if it accepts the parameter, and the aggregation is handled the
        # same as numeric_only is False.
        if numeric_only and not is_numeric_dtype(self._query_compiler.dtypes[0]):
            # pandas throws an NotImplementedError when the numeric_only is True, but the
            # series dtype is not numeric
            ErrorMessage.not_implemented(
                "SeriesGroupBy does not implement numeric_only"
            )
        numeric_only = False

    if is_result_dataframe is None:
        # If the GroupBy object is a SeriesGroupBy, we generally return a Series
        # after an aggregation - unless `as_index` is False, in which case we
        # return a DataFrame with N columns, where the first N-1 columns are
        # the grouping columns (by), and the Nth column is the aggregation
        # result.
        is_result_dataframe = not is_series_groupby or not self._as_index
    result_type = pd.DataFrame if is_result_dataframe else pd.Series
    result = result_type(
        query_compiler=qc_method(
            self._query_compiler,
            by=self._by,
            axis=self._axis,
            groupby_kwargs=self._kwargs,
            agg_args=agg_args,
            agg_kwargs=agg_kwargs,
            numeric_only=numeric_only,
            is_series_groupby=is_series_groupby,
            **kwargs,
        )
    )
    return result


@register_df_groupby_override("_check_index_name")
def _check_index_name(self, result):
    """
    Check the result of groupby aggregation on the need of resetting index name.

    Parameters
    ----------
    result : DataFrame
        Group by aggregation result.

    Returns
    -------
    DataFrame
    """
    # TODO: SNOW-1063349: Modin upgrade - modin.pandas.groupby.DataFrameGroupBy functions
    if self._by is not None:
        # pandas does not name the index for this case
        result._query_compiler.set_index_name(None)
    return result


# NOT AN OVERRIDE
def validate_groupby_args(
    by: Any,
    level: Optional[IndexLabel],
    observed: Union[bool, NoDefault],
) -> None:
    """
    Common validation and checks for the groupby arguments that are used by both SeriesGroupBy
    and DataFrameGroupBy.

    Raises:
        TypeError if native pandas series is used as by item, or if both level and by are None
    Warns:
        If observed is True, this parameter is ignored because CategoryDType is not supported with Snowpark pandas API
    """
    # TODO: SNOW-1063350: Modin upgrade - modin.pandas.groupby.SeriesGroupBy functions
    # check if pandas.Series is used as by item, no native pandas series or dataframe
    # object is allowed.
    raise_if_native_pandas_objects(by)
    if not isinstance(by, Series) and is_list_like(by):
        for o in by:
            raise_if_native_pandas_objects(o)

    if level is None and by is None:
        raise TypeError("You have to supply one of 'by' and 'level'")

    if observed is not no_default and observed:
        WarningMessage.ignored_argument(
            operation="groupby",
            argument="observed",
            message="CategoricalDType is not yet supported with Snowpark pandas API, the observed parameter is ignored.",
        )
