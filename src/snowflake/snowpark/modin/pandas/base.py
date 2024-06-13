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

"""Implement DataFrame/Series public API as pandas does."""
from __future__ import annotations

import pickle as pkl
import re
import warnings
from collections.abc import Hashable, Mapping, Sequence
from typing import Any, Callable, Literal, get_args

import numpy as np
import numpy.typing as npt
import pandas
import pandas.core.generic
import pandas.core.resample
import pandas.core.window.rolling
from pandas._libs import lib
from pandas._libs.lib import NoDefault, is_bool, no_default
from pandas._typing import (
    AggFuncType,
    AnyArrayLike,
    Axes,
    Axis,
    CompressionOptions,
    DtypeBackend,
    FillnaOptions,
    IgnoreRaise,
    IndexKeyFunc,
    IndexLabel,
    Level,
    NaPosition,
    RandomState,
    Scalar,
    StorageOptions,
    TimedeltaConvertibleTypes,
    TimestampConvertibleTypes,
)
from pandas.compat import numpy as numpy_compat
from pandas.core.common import apply_if_callable, count_not_none, pipe
from pandas.core.dtypes.common import (
    is_dict_like,
    is_dtype_equal,
    is_list_like,
    is_numeric_dtype,
    is_object_dtype,
    pandas_dtype,
)
from pandas.core.dtypes.inference import is_integer
from pandas.errors import SpecificationError
from pandas.util._validators import (
    validate_ascending,
    validate_bool_kwarg,
    validate_percentile,
)

from snowflake.snowpark.modin import pandas as pd
from snowflake.snowpark.modin.pandas.utils import (
    ensure_index,
    extract_validate_and_try_convert_named_aggs_from_kwargs,
    get_as_shape_compatible_dataframe_or_series,
    is_scalar,
    raise_if_native_pandas_objects,
    validate_and_try_convert_agg_func_arg_func_to_str,
)
from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    base_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    try_cast_to_pandas,
    validate_int_kwarg,
)

# Similar to pandas, sentinel value to use as kwarg in place of None when None has
# special meaning and needs to be distinguished from a user explicitly passing None.
sentinel = object()

# Do not look up certain attributes in columns or index, as they're used for some
# special purposes, like serving remote context
_ATTRS_NO_LOOKUP = {
    "____id_pack__",
    "__name__",
    "_cache",
    "_ipython_canary_method_should_not_exist_",
    "_ipython_display_",
    "_repr_html_",
    "_repr_javascript_",
    "_repr_jpeg_",
    "_repr_json_",
    "_repr_latex_",
    "_repr_markdown_",
    "_repr_mimebundle_",
    "_repr_pdf_",
    "_repr_png_",
    "_repr_svg_",
    "__array_struct__",
    "__array_interface__",
    "_typ",
}

_DEFAULT_BEHAVIOUR = {
    "__init__",
    "__class__",
    "_get_index",
    "_set_index",
    "_pandas_class",
    "_get_axis_number",
    "empty",
    "index",
    "columns",
    "name",
    "dtypes",
    "dtype",
    "groupby",
    "_get_name",
    "_set_name",
    "_default_to_pandas",
    "_query_compiler",
    "_to_pandas",
    "_repartition",
    "_build_repr_df",
    "_reduce_dimension",
    "__repr__",
    "__len__",
    "__constructor__",
    "_create_or_update_from_compiler",
    "_update_inplace",
    # for persistance support;
    # see DataFrame methods docstrings for more
    "_inflate_light",
    "_inflate_full",
    "__reduce__",
    "__reduce_ex__",
    "_init",
} | _ATTRS_NO_LOOKUP


@_inherit_docstrings(
    pandas.DataFrame,
    apilink=["pandas.DataFrame", "pandas.Series"],
    excluded=[
        pandas.DataFrame.between_time,
        pandas.Series.between_time,
        pandas.DataFrame.flags,
        pandas.Series.flags,
        pandas.DataFrame.kurt,
        pandas.Series.kurt,
        pandas.DataFrame.kurtosis,
        pandas.Series.kurtosis,
        pandas.DataFrame.rank,
        pandas.Series.rank,
        pandas.DataFrame.to_csv,
        pandas.Series.to_csv,
        pandas.DataFrame.sum,
    ],
)
class BasePandasDataset(metaclass=TelemetryMeta):
    """
    Implement most of the common code that exists in DataFrame/Series.

    Since both objects share the same underlying representation, and the algorithms
    are the same, we use this object to define the general behavior of those objects
    and then use those objects to define the output type.

    TelemetryMeta is a metaclass that automatically add telemetry decorators to classes/instance methods.
    See TelemetryMeta for details. Note: Its subclasses will inherit this metaclass.
    """

    # pandas class that we pretend to be; usually it has the same name as our class
    # but lives in "pandas" namespace.
    _pandas_class = pandas.core.generic.NDFrame

    @pandas.util.cache_readonly
    def _is_dataframe(self) -> bool:
        """
        Tell whether this is a dataframe.

        Ideally, other methods of BasePandasDataset shouldn't care whether this
        is a dataframe or a series, but sometimes we need to know. This method
        is better than hasattr(self, "columns"), which for series will call
        self.__getattr__("columns"), which requires materializing the index.

        Returns
        -------
        bool : Whether this is a dataframe.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return issubclass(self._pandas_class, pandas.DataFrame)

    def _add_sibling(self, sibling):
        """
        Add a DataFrame or Series object to the list of siblings.

        Siblings are objects that share the same query compiler. This function is called
        when a shallow copy is made.

        Parameters
        ----------
        sibling : BasePandasDataset
            Dataset to add to siblings list.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        sibling._siblings = self._siblings + [self]
        self._siblings += [sibling]
        for sib in self._siblings:
            sib._siblings += [sibling]

    def _update_inplace(self, new_query_compiler):
        """
        Update the current DataFrame inplace.

        Parameters
        ----------
        new_query_compiler : query_compiler
            The new QueryCompiler to use to manage the data.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        old_query_compiler = self._query_compiler
        self._query_compiler = new_query_compiler
        for sib in self._siblings:
            sib._query_compiler = new_query_compiler
        old_query_compiler.free()

    def _validate_other(
        self,
        other,
        axis,
        dtype_check=False,
        compare_index=False,
    ):
        """
        Help to check validity of other in inter-df operations.

        Parameters
        ----------
        other : modin.pandas.BasePandasDataset
            Another dataset to validate against `self`.
        axis : {None, 0, 1}
            Specifies axis along which to do validation. When `1` or `None`
            is specified, validation is done along `index`, if `0` is specified
            validation is done along `columns` of `other` frame.
        dtype_check : bool, default: False
            Validates that both frames have compatible dtypes.
        compare_index : bool, default: False
            Compare Index if True.

        Returns
        -------
        modin.pandas.BasePandasDataset
            Other frame if it is determined to be valid.

        Raises
        ------
        ValueError
            If `other` is `Series` and its length is different from
            length of `self` `axis`.
        TypeError
            If any validation checks fail.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if isinstance(other, BasePandasDataset):
            return other._query_compiler
        if not is_list_like(other):
            # We skip dtype checking if the other is a scalar. Note that pandas
            # is_scalar can be misleading as it is False for almost all objects,
            # even when those objects should be treated as scalars. See e.g.
            # https://github.com/modin-project/modin/issues/5236. Therefore, we
            # detect scalars by checking that `other` is neither a list-like nor
            # another BasePandasDataset.
            return other
        axis = self._get_axis_number(axis) if axis is not None else 1
        result = other
        if axis == 0:
            if len(other) != len(self._query_compiler.index):
                raise ValueError(
                    f"Unable to coerce to Series, length must be {len(self._query_compiler.index)}: "
                    + f"given {len(other)}"
                )
        else:
            if len(other) != len(self._query_compiler.columns):
                raise ValueError(
                    f"Unable to coerce to Series, length must be {len(self._query_compiler.columns)}: "
                    + f"given {len(other)}"
                )
        if hasattr(other, "dtype"):
            other_dtypes = [other.dtype] * len(other)
        elif is_dict_like(other):
            other_dtypes = [
                type(other[label])
                for label in self._query_compiler.get_axis(axis)
                # The binary operation is applied for intersection of axis labels
                # and dictionary keys. So filtering out extra keys.
                if label in other
            ]
        else:
            other_dtypes = [type(x) for x in other]
        if compare_index:
            if not self.index.equals(other.index):
                raise TypeError("Cannot perform operation with non-equal index")
        # Do dtype checking.
        if dtype_check:
            self_dtypes = self._get_dtypes()
            if is_dict_like(other):
                # The binary operation is applied for the intersection of axis labels
                # and dictionary keys. So filtering `self_dtypes` to match the `other`
                # dictionary.
                self_dtypes = [
                    dtype
                    for label, dtype in zip(
                        self._query_compiler.get_axis(axis), self._get_dtypes()
                    )
                    if label in other
                ]

            # TODO(https://github.com/modin-project/modin/issues/5239):
            # this spuriously rejects other that is a list including some
            # custom type that can be added to self's elements.
            if not all(
                (is_numeric_dtype(self_dtype) and is_numeric_dtype(other_dtype))
                or (is_object_dtype(self_dtype) and is_object_dtype(other_dtype))
                # Check if dtype is timedelta ("m") or datetime ("M")
                or (
                    lib.is_np_dtype(self_dtype, "mM")
                    and lib.is_np_dtype(other_dtype, "mM")
                )
                or is_dtype_equal(self_dtype, other_dtype)
                for self_dtype, other_dtype in zip(self_dtypes, other_dtypes)
            ):
                raise TypeError("Cannot do operation with improper dtypes")
        return result

    def _validate_function(self, func, on_invalid=None):
        """
        Check the validity of the function which is intended to be applied to the frame.

        Parameters
        ----------
        func : object
        on_invalid : callable(str, cls), optional
            Function to call in case invalid `func` is met, `on_invalid` takes an error
            message and an exception type as arguments. If not specified raise an
            appropriate exception.
            **Note:** This parameter is a hack to concord with pandas error types.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset

        def error_raiser(msg, exception=Exception):
            raise exception(msg)

        if on_invalid is None:
            on_invalid = error_raiser

        if isinstance(func, dict):
            [self._validate_function(fn, on_invalid) for fn in func.values()]
            return
            # We also could validate this, but it may be quite expensive for lazy-frames
            # if not all(idx in self.axes[axis] for idx in func.keys()):
            #     error_raiser("Invalid dict keys", KeyError)

        if not is_list_like(func):
            func = [func]

        for fn in func:
            if isinstance(fn, str):
                if not (hasattr(self, fn) or hasattr(np, fn)):
                    on_invalid(
                        f"{fn} is not valid function for {type(self)} object.",
                        AttributeError,
                    )
            elif not callable(fn):
                on_invalid(
                    f"One of the passed functions has an invalid type: {type(fn)}: {fn}, "
                    + "only callable or string is acceptable.",
                    TypeError,
                )

    def _binary_op(
        self,
        op: str,
        other: BasePandasDataset,
        axis: Axis,
        level: Level | None = None,
        fill_value: float | None = None,
        **kwargs: Any,
    ):
        """
        Do binary operation between two datasets.

        Parameters
        ----------
        op : str
            Name of binary operation.
        other : modin.pandas.BasePandasDataset
            Second operand of binary operation.
        axis: Whether to compare by the index (0 or ‘index’) or columns. (1 or ‘columns’).
        level: Broadcast across a level, matching Index values on the passed MultiIndex level.
        fill_value: Fill existing missing (NaN) values, and any new element needed for
            successful DataFrame alignment, with this value before computation.
            If data in both corresponding DataFrame locations is missing the result will be missing.
            only arithmetic binary operation has this parameter (e.g., add() has, but eq() doesn't have).

        kwargs can contain the following parameters passed in at the frontend:
            func: Only used for `combine` method. Function that takes two series as inputs and
                return a Series or a scalar. Used to merge the two dataframes column by columns.

        Returns
        -------
        modin.pandas.BasePandasDataset
            Result of binary operation.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        raise_if_native_pandas_objects(other)
        axis = self._get_axis_number(axis)
        squeeze_self = isinstance(self, pd.Series)

        # pandas itself will ignore the axis argument when using Series.<op>.
        # Per default, it is set to axis=0. However, for the case of a Series interacting with
        # a DataFrame the behavior is axis=1. Manually check here for this case and adjust the axis.

        is_lhs_series_and_rhs_dataframe = (
            True
            if isinstance(self, pd.Series) and isinstance(other, pd.DataFrame)
            else False
        )

        new_query_compiler = self._query_compiler.binary_op(
            op=op,
            other=other,
            axis=1 if is_lhs_series_and_rhs_dataframe else axis,
            level=level,
            fill_value=fill_value,
            squeeze_self=squeeze_self,
            **kwargs,
        )

        from snowflake.snowpark.modin.pandas.dataframe import DataFrame

        # Modin Bug: https://github.com/modin-project/modin/issues/7236
        # For a Series interacting with a DataFrame, always return a DataFrame
        return (
            DataFrame(query_compiler=new_query_compiler)
            if is_lhs_series_and_rhs_dataframe
            else self._create_or_update_from_compiler(new_query_compiler)
        )

    def _default_to_pandas(self, op, *args, **kwargs):
        """
        Convert dataset to pandas type and call a pandas function on it.

        Parameters
        ----------
        op : str
            Name of pandas function.
        *args : list
            Additional positional arguments to be passed to `op`.
        **kwargs : dict
            Additional keywords arguments to be passed to `op`.

        Returns
        -------
        object
            Result of operation.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        args = try_cast_to_pandas(args)
        kwargs = try_cast_to_pandas(kwargs)
        pandas_obj = self._to_pandas()
        if callable(op):
            result = op(pandas_obj, *args, **kwargs)
        elif isinstance(op, str):
            # The inner `getattr` is ensuring that we are treating this object (whether
            # it is a DataFrame, Series, etc.) as a pandas object. The outer `getattr`
            # will get the operation (`op`) from the pandas version of the class and run
            # it on the object after we have converted it to pandas.
            attr = getattr(self._pandas_class, op)
            if isinstance(attr, property):
                result = getattr(pandas_obj, op)
            else:
                result = attr(pandas_obj, *args, **kwargs)
        else:
            ErrorMessage.internal_error(
                failure_condition=True,
                extra_log=f"{op} is an unsupported operation",
            )
        # SparseDataFrames cannot be serialized by arrow and cause problems for Modin.
        # For now we will use pandas.
        if isinstance(result, type(self)) and not isinstance(
            result, (pandas.SparseDataFrame, pandas.SparseSeries)
        ):
            return self._create_or_update_from_compiler(
                result, inplace=kwargs.get("inplace", False)
            )
        elif isinstance(result, pandas.DataFrame):
            from snowflake.snowpark.modin.pandas import DataFrame

            return DataFrame(result)
        elif isinstance(result, pandas.Series):
            from snowflake.snowpark.modin.pandas import Series

            return Series(result)
        # inplace
        elif result is None:
            return self._create_or_update_from_compiler(
                getattr(pd, type(pandas_obj).__name__)(pandas_obj)._query_compiler,
                inplace=True,
            )
        else:
            try:
                if (
                    isinstance(result, (list, tuple))
                    and len(result) == 2
                    and isinstance(result[0], pandas.DataFrame)
                ):
                    # Some operations split the DataFrame into two (e.g. align). We need to wrap
                    # both of the returned results
                    if isinstance(result[1], pandas.DataFrame):
                        second = self.__constructor__(result[1])
                    else:
                        second = result[1]
                    return self.__constructor__(result[0]), second
                else:
                    return result
            except TypeError:
                return result

    @classmethod
    def _get_axis_number(cls, axis):
        """
        Convert axis name or number to axis index.

        Parameters
        ----------
        axis : int, str or pandas._libs.lib.NoDefault
            Axis name ('index' or 'columns') or number to be converted to axis index.

        Returns
        -------
        int
            0 or 1 - axis index in the array of axes stored in the dataframe.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if axis is no_default:
            axis = None

        return cls._pandas_class._get_axis_number(axis) if axis is not None else 0

    @pandas.util.cache_readonly
    def __constructor__(self):
        """
        Construct DataFrame or Series object depending on self type.

        Returns
        -------
        modin.pandas.BasePandasDataset
            Constructed object.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return type(self)

    def abs(self):  # noqa: RT01, D200
        """
        Return a `BasePandasDataset` with absolute numeric value of each element.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.__constructor__(query_compiler=self._query_compiler.unary_op("abs"))

    def _to_series_list(self, index: pd.Index) -> list[pd.Series]:
        """
        Convert index to a list of series
        Args:
            index: can be single or multi index

        Returns:
            the list of series
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if isinstance(index, pd.MultiIndex):
            return [
                pd.Series(index.get_level_values(level))
                for level in range(index.nlevels)
            ]
        elif isinstance(index, pd.Index):
            return [pd.Series(index)]

    def _set_index(self, new_index: Axes) -> None:
        """
        Set the index for this DataFrame.

        Parameters
        ----------
        new_index : pandas.Index
            The new index to set this.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        self._update_inplace(
            new_query_compiler=self._query_compiler.set_index(
                [
                    s._query_compiler
                    for s in self._to_series_list(ensure_index(new_index))
                ]
            )
        )

    def set_axis(
        self,
        labels: IndexLabel,
        *,
        axis: Axis = 0,
        copy: bool | NoDefault = no_default,
    ):
        """
        Assign desired index to given axis.
        """
        # Behavior based on copy:
        # -----------------------------------
        # - In native pandas, copy determines whether to create a copy of the data (not DataFrame).
        # - We cannot emulate the native pandas' copy behavior in Snowpark since a copy of only data
        #   cannot be created -- you can only copy the whole object (DataFrame/Series).
        #
        # Snowpark behavior:
        # ------------------
        # - copy is kept for compatibility with native pandas but is ignored. The user is warned that copy is unused.
        # Warn user that copy does not do anything.
        if copy is not no_default:
            WarningMessage.single_warning(
                message=f"{type(self).__name__}.set_axis 'copy' keyword is unused and is ignored."
            )
        if labels is None:
            raise TypeError("None is not a valid value for the parameter 'labels'.")

        # Determine whether to update self or a copy and perform update.
        obj = self.copy()
        setattr(obj, axis, labels)
        return obj

    def _get_index(self):
        """
        Get the index for this DataFrame.

        Returns
        -------
        pandas.Index
            The union of all indexes across the partitions.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._query_compiler.index

    index = property(_get_index, _set_index)

    def add(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Return addition of `BasePandasDataset` and `other`, element-wise (binary operator `add`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "add", other, axis=axis, level=level, fill_value=fill_value
        )

    def aggregate(
        self, func: AggFuncType = None, axis: Axis | None = 0, *args: Any, **kwargs: Any
    ):
        """
        Aggregate using one or more operations over the specified axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        from snowflake.snowpark.modin.pandas import Series

        origin_axis = axis
        axis = self._get_axis_number(axis)

        if axis == 1 and isinstance(self, Series):
            raise ValueError(f"No axis named {origin_axis} for object type Series")

        if len(self._query_compiler.columns) == 0:
            # native pandas raise error with message "no result", here we raise a more readable error.
            raise ValueError("No column to aggregate on.")

        # If we are using named kwargs, then we do not clear the kwargs (need them in the QC for processing
        # order, as well as formatting error messages.)
        uses_named_kwargs = False
        # If aggregate is called on a Series, named aggregations can be passed in via a dictionary
        # to func.
        if func is None or (is_dict_like(func) and not self._is_dataframe):
            if axis == 1:
                raise ValueError(
                    "`func` must not be `None` when `axis=1`. Named aggregations are not supported with `axis=1`."
                )
            if func is not None:
                # If named aggregations are passed in via a dictionary to func, then we
                # ignore the kwargs.
                if any(is_dict_like(value) for value in func.values()):
                    # We can only get to this codepath if self is a Series, and func is a dictionary.
                    # In this case, if any of the values of func are themselves dictionaries, we must raise
                    # a Specification Error, as that is what pandas does.
                    raise SpecificationError("nested renamer is not supported")
                kwargs = func
            func = extract_validate_and_try_convert_named_aggs_from_kwargs(
                self, allow_duplication=False, axis=axis, **kwargs
            )
            uses_named_kwargs = True
        else:
            func = validate_and_try_convert_agg_func_arg_func_to_str(
                agg_func=func,
                obj=self,
                allow_duplication=False,
                axis=axis,
            )

        # This is to stay consistent with pandas result format, when the func is single
        # aggregation function in format of callable or str, reduce the result dimension to
        # convert dataframe to series, or convert series to scalar.
        # Note: When named aggregations are used, the result is not reduced, even if there
        # is only a single function.
        # needs_reduce_dimension cannot be True if we are using named aggregations, since
        # the values for func in that case are either NamedTuples (AggFuncWithLabels) or
        # lists of NamedTuples, both of which are list like.
        need_reduce_dimension = (
            (callable(func) or isinstance(func, str))
            # A Series should be returned when a single scalar string/function aggregation function, or a
            # dict of scalar string/functions is specified. In all other cases (including if the function
            # is a 1-element list), the result is a DataFrame.
            #
            # The examples below have axis=1, but the same logic is applied for axis=0.
            # >>> df = pd.DataFrame({"a": [0, 1], "b": [2, 3]})
            #
            # single aggregation: return Series
            # >>> df.agg("max", axis=1)
            # 0    2
            # 1    3
            # dtype: int64
            #
            # list of aggregations: return DF
            # >>> df.agg(["max"], axis=1)
            #    max
            # 0    2
            # 1    3
            #
            # dict where all aggregations are strings: return Series
            # >>> df.agg({1: "max", 0: "min"}, axis=1)
            # 1    3
            # 0    0
            # dtype: int64
            #
            # dict where one element is a list: return DF
            # >>> df.agg({1: "max", 0: ["min"]}, axis=1)
            #    max  min
            # 1  3.0  NaN
            # 0  NaN  0.0
            or (
                is_dict_like(func)
                and all(not is_list_like(value) for value in func.values())
            )
        )

        # If func is a dict, pandas will not respect kwargs for each aggregation function, and
        # we should drop them before passing the to the query compiler.
        #
        # >>> native_pd.DataFrame({"a": [0, 1], "b": [np.nan, 0]}).agg("max", skipna=False, axis=1)
        # 0    NaN
        # 1    1.0
        # dtype: float64
        # >>> native_pd.DataFrame({"a": [0, 1], "b": [np.nan, 0]}).agg(["max"], skipna=False, axis=1)
        #    max
        # 0  0.0
        # 1  1.0
        # >>> pd.DataFrame([[np.nan], [0]]).aggregate("count", skipna=True, axis=0)
        # 0    1
        # dtype: int8
        # >>> pd.DataFrame([[np.nan], [0]]).count(skipna=True, axis=0)
        # TypeError: got an unexpected keyword argument 'skipna'
        if is_dict_like(func) and not uses_named_kwargs:
            kwargs.clear()

        result = self.__constructor__(
            query_compiler=self._query_compiler.agg(
                func=func,
                axis=axis,
                args=args,
                kwargs=kwargs,
            )
        )

        if need_reduce_dimension:
            if self._is_dataframe:
                result = Series(query_compiler=result._query_compiler)

            if isinstance(result, Series):
                # When func is just "quantile" with a scalar q, result has quantile value as name
                q = kwargs.get("q", 0.5)
                if func == "quantile" and is_scalar(q):
                    result.name = q
                else:
                    result.name = None

            # handle case for single scalar (same as result._reduce_dimension())
            if isinstance(self, Series):
                return result.to_pandas().squeeze()

        return result

    agg = aggregate

    def _string_function(self, func, *args, **kwargs):
        """
        Execute a function identified by its string name.

        Parameters
        ----------
        func : str
            Function name to call on `self`.
        *args : list
            Positional arguments to pass to func.
        **kwargs : dict
            Keyword arguments to pass to func.

        Returns
        -------
        object
            Function result.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        assert isinstance(func, str)
        f = getattr(self, func, None)
        if f is not None:
            if callable(f):
                return f(*args, **kwargs)
            assert len(args) == 0
            assert len([kwarg for kwarg in kwargs if kwarg != "axis"]) == 0
            return f
        f = getattr(np, func, None)
        if f is not None:
            return self._default_to_pandas("agg", func, *args, **kwargs)
        raise ValueError(f"{func} is an unknown string function")

    def _get_dtypes(self):
        """
        Get dtypes as list.

        Returns
        -------
        list
            Either a one-element list that contains `dtype` if object denotes a Series
            or a list that contains `dtypes` if object denotes a DataFrame.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if hasattr(self, "dtype"):
            return [self.dtype]
        else:
            return list(self.dtypes)

    @base_not_implemented()
    def align(
        self,
        other,
        join="outer",
        axis=None,
        level=None,
        copy=None,
        fill_value=None,
        method=lib.no_default,
        limit=lib.no_default,
        fill_axis=lib.no_default,
        broadcast_axis=lib.no_default,
    ):  # noqa: PR01, RT01, D200
        """
        Align two objects on their axes with the specified join method.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "align",
            other,
            join=join,
            axis=axis,
            level=level,
            copy=copy,
            fill_value=fill_value,
            method=method,
            limit=limit,
            fill_axis=fill_axis,
            broadcast_axis=broadcast_axis,
        )

    def all(self, axis=0, bool_only=None, skipna=True, **kwargs):
        """
        Return whether all elements are True, potentially over an axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        validate_bool_kwarg(skipna, "skipna", none_allowed=False)
        if axis is not None:
            axis = self._get_axis_number(axis)
            if bool_only and axis == 0:
                if hasattr(self, "dtype"):
                    ErrorMessage.not_implemented(
                        "{}.{} does not implement numeric_only.".format(
                            type(self).__name__, "all"
                        )
                    )  # pragma: no cover
                data_for_compute = self[self.columns[self.dtypes == np.bool_]]
                return data_for_compute.all(
                    axis=axis, bool_only=False, skipna=skipna, **kwargs
                )
            return self._reduce_dimension(
                self._query_compiler.all(
                    axis=axis, bool_only=bool_only, skipna=skipna, **kwargs
                )
            )
        else:
            if bool_only:
                raise ValueError(f"Axis must be 0 or 1 (got {axis})")
            # Reduce to a scalar if axis is None.
            result = self._reduce_dimension(
                # FIXME: Judging by pandas docs `**kwargs` serves only compatibility
                # purpose and does not affect the result, we shouldn't pass them to the query compiler.
                self._query_compiler.all(
                    axis=0,
                    bool_only=bool_only,
                    skipna=skipna,
                    **kwargs,
                )
            )
            if isinstance(result, BasePandasDataset):
                return result.all(
                    axis=axis, bool_only=bool_only, skipna=skipna, **kwargs
                )
            return result

    def any(self, axis=0, bool_only=None, skipna=True, **kwargs):
        """
        Return whether any element is True, potentially over an axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        validate_bool_kwarg(skipna, "skipna", none_allowed=False)
        if axis is not None:
            axis = self._get_axis_number(axis)
            if bool_only and axis == 0:
                if hasattr(self, "dtype"):
                    ErrorMessage.not_implemented(
                        "{}.{} does not implement numeric_only.".format(
                            type(self).__name__, "all"
                        )
                    )  # pragma: no cover
                data_for_compute = self[self.columns[self.dtypes == np.bool_]]
                return data_for_compute.any(
                    axis=axis, bool_only=False, skipna=skipna, **kwargs
                )
            return self._reduce_dimension(
                self._query_compiler.any(
                    axis=axis, bool_only=bool_only, skipna=skipna, **kwargs
                )
            )
        else:
            if bool_only:
                raise ValueError(f"Axis must be 0 or 1 (got {axis})")
            # Reduce to a scalar if axis is None.
            result = self._reduce_dimension(
                self._query_compiler.any(
                    axis=0,
                    bool_only=bool_only,
                    skipna=skipna,
                    **kwargs,
                )
            )
            if isinstance(result, BasePandasDataset):
                return result.any(
                    axis=axis, bool_only=bool_only, skipna=skipna, **kwargs
                )
            return result

    def apply(
        self,
        func,
        axis,
        broadcast,
        raw,
        reduce,
        result_type,
        convert_dtype,
        args,
        **kwds,
    ):  # noqa: PR01, RT01, D200
        """
        Apply a function along an axis of the `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset

        def error_raiser(msg, exception):
            """Convert passed exception to the same type as pandas do and raise it."""
            # HACK: to concord with pandas error types by replacing all of the
            # TypeErrors to the AssertionErrors
            exception = exception if exception is not TypeError else AssertionError
            raise exception(msg)

        self._validate_function(func, on_invalid=error_raiser)
        axis = self._get_axis_number(axis)
        # TODO SNOW-864025: Support str in series.apply and df.apply
        if isinstance(func, str):
            # if axis != 1 function can be bounded to the Series, which doesn't
            # support axis parameter
            if axis == 1:
                kwds["axis"] = axis
            result = self._string_function(func, *args, **kwds)
            if isinstance(result, BasePandasDataset):
                return result._query_compiler
            return result
        # TODO SNOW-856682: Support dict in series.apply and df.apply
        elif isinstance(func, dict):
            if len(self.columns) != len(set(self.columns)):
                WarningMessage.mismatch_with_pandas(
                    operation="apply",
                    message="Duplicate column names not supported with apply().",
                )  # pragma: no cover
        query_compiler = self._query_compiler.apply(
            func,
            axis,
            args=args,
            raw=raw,
            result_type=result_type,
            **kwds,
        )
        return query_compiler

    @base_not_implemented()
    def asfreq(
        self, freq, method=None, how=None, normalize=False, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Convert time series to specified frequency.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "asfreq",
            freq,
            method=method,
            how=how,
            normalize=normalize,
            fill_value=fill_value,
        )

    @base_not_implemented()
    def asof(self, where, subset=None):  # noqa: PR01, RT01, D200
        """
        Return the last row(s) without any NaNs before `where`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        scalar = not is_list_like(where)
        if isinstance(where, pandas.Index):
            # Prevent accidental mutation of original:
            where = where.copy()
        else:
            if scalar:
                where = [where]
            where = pandas.Index(where)

        if subset is None:
            data = self
        else:
            # Only relevant for DataFrames:
            data = self[subset]
        no_na_index = data.dropna().index
        new_index = pandas.Index([no_na_index.asof(i) for i in where])
        result = self.reindex(new_index)
        result.index = where

        if scalar:
            # Need to return a Series:
            result = result.squeeze()
        return result

    def astype(
        self,
        dtype: str | type | pd.Series | dict[str, type],
        copy: bool = True,
        errors: Literal["raise", "ignore"] = "raise",
    ) -> pd.DataFrame | pd.Series:
        """
        Cast a Modin object to a specified dtype `dtype`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # dtype can be a series, a dict, or a scalar. If it's series or scalar,
        # convert it to a dict before passing it to the query compiler.
        raise_if_native_pandas_objects(dtype)
        from snowflake.snowpark.modin.pandas import Series

        if isinstance(dtype, Series):
            dtype = dtype.to_pandas()
            if not dtype.index.is_unique:
                raise ValueError(
                    "The new Series of types must have a unique index, i.e. "
                    + "it must be one-to-one mapping from column names to "
                    + " their new dtypes."
                )
            dtype = dtype.to_dict()
        # If we got a series or dict originally, dtype is a dict now. Its keys
        # must be column names.
        if isinstance(dtype, dict):
            # Avoid materializing columns. The query compiler will handle errors where
            # dtype dict includes keys that are not in columns.
            col_dtypes = dtype
            for col_name in col_dtypes:
                if col_name not in self._query_compiler.columns:
                    raise KeyError(
                        "Only a column name can be used for the key in a dtype mappings argument. "
                        f"'{col_name}' not found in columns."
                    )
        else:
            # Assume that the dtype is a scalar.
            col_dtypes = {column: dtype for column in self._query_compiler.columns}

        # ensure values are pandas dtypes
        col_dtypes = {k: pandas_dtype(v) for k, v in col_dtypes.items()}
        new_query_compiler = self._query_compiler.astype(col_dtypes, errors=errors)
        return self._create_or_update_from_compiler(new_query_compiler, not copy)

    @base_not_implemented()
    @property
    def at(self, axis=None):  # noqa: PR01, RT01, D200
        """
        Get a single value for a row/column label pair.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        from .indexing import _LocIndexer

        return _LocIndexer(self)

    @base_not_implemented()
    def at_time(self, time, asof=False, axis=None):  # noqa: PR01, RT01, D200
        """
        Select values at particular time of day (e.g., 9:30AM).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        idx = self.index if axis == 0 else self.columns
        indexer = pandas.Series(index=idx).at_time(time, asof=asof).index
        return self.loc[indexer] if axis == 0 else self.loc[:, indexer]

    @base_not_implemented()
    @_inherit_docstrings(
        pandas.DataFrame.between_time, apilink="pandas.DataFrame.between_time"
    )
    def between_time(
        self: BasePandasDataset,
        start_time,
        end_time,
        inclusive: str | None = None,
        axis=None,
    ):  # noqa: PR01, RT01, D200
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        idx = self.index if axis == 0 else self.columns
        indexer = (
            pandas.Series(index=idx)
            .between_time(
                start_time,
                end_time,
                inclusive=inclusive,
            )
            .index
        )
        return self.loc[indexer] if axis == 0 else self.loc[:, indexer]

    @base_not_implemented()
    def bfill(
        self, axis=None, inplace=False, limit=None, downcast=None
    ):  # noqa: PR01, RT01, D200
        """
        Synonym for `DataFrame.fillna` with ``method='bfill'``.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.fillna(
            method="bfill", axis=axis, limit=limit, downcast=downcast, inplace=inplace
        )

    backfill = bfill

    @base_not_implemented()
    def bool(self):  # noqa: RT01, D200
        """
        Return the bool of a single element `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        shape = self.shape
        if shape != (1,) and shape != (1, 1):
            raise ValueError(
                """The PandasObject does not have exactly
                                1 element. Return the bool of a single
                                element PandasObject. The truth value is
                                ambiguous. Use a.empty, a.item(), a.any()
                                or a.all()."""
            )
        else:
            return self._to_pandas().bool()

    @base_not_implemented()
    def clip(
        self, lower=None, upper=None, axis=None, inplace=False, *args, **kwargs
    ):  # noqa: PR01, RT01, D200
        """
        Trim values at input threshold(s).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # validate inputs
        if axis is not None:
            axis = self._get_axis_number(axis)
        self._validate_dtypes(numeric_only=True)
        inplace = validate_bool_kwarg(inplace, "inplace")
        axis = numpy_compat.function.validate_clip_with_axis(axis, args, kwargs)
        # any np.nan bounds are treated as None
        if lower is not None and np.any(np.isnan(lower)):
            lower = None
        if upper is not None and np.any(np.isnan(upper)):
            upper = None
        if is_list_like(lower) or is_list_like(upper):
            if axis is None:
                raise ValueError("Must specify axis = 0 or 1")
            lower = self._validate_other(lower, axis)
            upper = self._validate_other(upper, axis)
        # FIXME: Judging by pandas docs `*args` and `**kwargs` serves only compatibility
        # purpose and does not affect the result, we shouldn't pass them to the query compiler.
        new_query_compiler = self._query_compiler.clip(
            lower=lower, upper=upper, axis=axis, inplace=inplace, *args, **kwargs
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    @base_not_implemented()
    def combine(self, other, func, fill_value=None, **kwargs):  # noqa: PR01, RT01, D200
        """
        Perform combination of `BasePandasDataset`-s according to `func`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "combine", other, axis=0, func=func, fill_value=fill_value, **kwargs
        )

    @base_not_implemented()
    def combine_first(self, other):  # noqa: PR01, RT01, D200
        """
        Update null elements with value in the same location in `other`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("combine_first", other, axis=0)

    def copy(self, deep: bool = True):
        """
        Make a copy of the object's metadata.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if deep:
            return self.__constructor__(query_compiler=self._query_compiler.copy())
        new_obj = self.__constructor__(query_compiler=self._query_compiler)
        self._add_sibling(new_obj)
        return new_obj

    def count(
        self,
        axis: Axis | None = 0,
        numeric_only: bool = False,
    ):
        """
        Count non-NA cells for `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._agg_helper(
            func="count",
            axis=axis,
            numeric_only=numeric_only,
        )

    def cummax(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return cumulative maximum over a `BasePandasDataset` axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        if axis == 1:
            self._validate_dtypes(numeric_only=True)
        return self.__constructor__(
            # FIXME: Judging by pandas docs `*args` and `**kwargs` serves only compatibility
            # purpose and does not affect the result, we shouldn't pass them to the query compiler.
            query_compiler=self._query_compiler.cummax(
                fold_axis=axis, axis=axis, skipna=skipna, **kwargs
            )
        )

    def cummin(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return cumulative minimum over a `BasePandasDataset` axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        if axis == 1:
            self._validate_dtypes(numeric_only=True)
        return self.__constructor__(
            # FIXME: Judging by pandas docs `*args` and `**kwargs` serves only compatibility
            # purpose and does not affect the result, we shouldn't pass them to the query compiler.
            query_compiler=self._query_compiler.cummin(
                fold_axis=axis, axis=axis, skipna=skipna, **kwargs
            )
        )

    def cumprod(
        self, axis=None, skipna=True, *args, **kwargs
    ):  # noqa: PR01, RT01, D200
        """
        Return cumulative product over a `BasePandasDataset` axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        self._validate_dtypes(numeric_only=True)
        return self.__constructor__(
            # FIXME: Judging by pandas docs `**kwargs` serves only compatibility
            # purpose and does not affect the result, we shouldn't pass them to the query compiler.
            query_compiler=self._query_compiler.cumprod(
                fold_axis=axis, axis=axis, skipna=skipna, **kwargs
            )
        )

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return cumulative sum over a `BasePandasDataset` axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        self._validate_dtypes(numeric_only=True)
        return self.__constructor__(
            # FIXME: Judging by pandas docs `*args` and `**kwargs` serves only compatibility
            # purpose and does not affect the result, we shouldn't pass them to the query compiler.
            query_compiler=self._query_compiler.cumsum(
                fold_axis=axis, axis=axis, skipna=skipna, **kwargs
            )
        )

    def describe(
        self,
        percentiles: ListLike | None = None,
        include: ListLike | Literal["all"] | None = None,
        exclude: ListLike | None = None,
    ) -> BasePandasDataset:
        """
        Generate descriptive statistics.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # Upstream modin uses pandas.core.methods.describe._refine_percentiles for this,
        # which is not available in pandas 1.5.X
        if percentiles is not None:
            # explicit conversion of `percentiles` to list
            percentiles = list(percentiles)

            # get them all to be in [0, 1]
            validate_percentile(percentiles)

            # median should always be included
            if 0.5 not in percentiles:
                percentiles.append(0.5)
            percentiles = np.asarray(percentiles)
        else:
            percentiles = np.array([0.25, 0.5, 0.75])

        data = self
        if self._is_dataframe:
            # Upstream modin lacks this check because it defaults to pandas for describing empty dataframes
            if len(self.columns) == 0:
                raise ValueError("Cannot describe a DataFrame without columns")

            # include/exclude are ignored for Series
            if (include is None) and (exclude is None):
                # when some numerics are found, keep only numerics
                default_include: list[npt.DTypeLike] = [np.number]
                default_include.append("datetime")
                data = self.select_dtypes(include=default_include)
                if len(data.columns) == 0:
                    data = self
            elif include == "all":
                if exclude is not None:
                    raise ValueError("exclude must be None when include is 'all'")
                data = self
            else:
                data = self.select_dtypes(
                    include=include,
                    exclude=exclude,
                )
        # Upstream modin uses data.empty, but that incurs an extra row count query
        if self._is_dataframe and len(data.columns) == 0:
            # Match pandas error from concatenating empty list of series descriptions.
            raise ValueError("No objects to concatenate")

        return self.__constructor__(
            query_compiler=data._query_compiler.describe(percentiles=percentiles)
        )

    def diff(self, periods: int = 1, axis: Axis = 0):
        """
        First discrete difference of element.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # We must only accept integer (or float values that are whole numbers)
        # for periods.
        int_periods = validate_int_kwarg(periods, "periods", float_allowed=True)
        axis = self._get_axis_number(axis)
        return self.__constructor__(
            query_compiler=self._query_compiler.diff(axis=axis, periods=int_periods)
        )

    def drop(
        self,
        labels: IndexLabel = None,
        axis: Axis = 0,
        index: IndexLabel = None,
        columns: IndexLabel = None,
        level: Level = None,
        inplace: bool = False,
        errors: IgnoreRaise = "raise",
    ) -> BasePandasDataset | None:
        """
        Drop specified labels from `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        inplace = validate_bool_kwarg(inplace, "inplace")
        if labels is not None:
            if index is not None or columns is not None:
                raise ValueError("Cannot specify both 'labels' and 'index'/'columns'")
            axes = {self._get_axis_number(axis): labels}
        elif index is not None or columns is not None:
            axes = {0: index, 1: columns}
        else:
            raise ValueError(
                "Need to specify at least one of 'labels', 'index' or 'columns'"
            )

        for axis, labels in axes.items():
            if labels is not None:
                if level is not None and not self._query_compiler.has_multiindex(
                    axis=axis
                ):
                    # Same error as native pandas.
                    raise AssertionError("axis must be a MultiIndex")
                # According to pandas documentation, a tuple will be used as a single
                # label and not treated as a list-like.
                if not is_list_like(labels) or isinstance(labels, tuple):
                    axes[axis] = [labels]

        new_query_compiler = self._query_compiler.drop(
            index=axes.get(0), columns=axes.get(1), level=level, errors=errors
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    def _dropna(
        self,
        axis: Axis = 0,
        how: str | NoDefault = no_default,
        thresh: int | NoDefault = no_default,
        subset: IndexLabel = None,
        inplace: bool = False,
    ):
        inplace = validate_bool_kwarg(inplace, "inplace")

        if is_list_like(axis):
            raise TypeError("supplying multiple axes to axis is no longer supported.")

        axis = self._get_axis_number(axis)

        if (how is not no_default) and (thresh is not no_default):
            raise TypeError(
                "You cannot set both the how and thresh arguments at the same time."
            )

        if how is no_default:
            how = "any"
        if how not in ["any", "all"]:
            raise ValueError("invalid how option: %s" % how)
        if subset is not None:
            if axis == 1:
                indices = self.index.get_indexer_for(subset)
                check = indices == -1
                if check.any():
                    raise KeyError(list(np.compress(check, subset)))
            else:
                indices = self.columns.get_indexer_for(subset)
                check = indices == -1
                if check.any():
                    raise KeyError(list(np.compress(check, subset)))

        new_query_compiler = self._query_compiler.dropna(
            axis=axis,
            how=how,
            thresh=thresh,
            subset=subset,
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    @base_not_implemented()
    def droplevel(self, level, axis=0):  # noqa: PR01, RT01, D200
        """
        Return `BasePandasDataset` with requested index / column level(s) removed.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        new_axis = self.axes[axis].droplevel(level)
        result = self.copy()
        if axis == 0:
            result.index = new_axis
        else:
            result.columns = new_axis
        return result

    def drop_duplicates(
        self, keep="first", inplace=False, **kwargs
    ):  # noqa: PR01, RT01, D200
        """
        Return `BasePandasDataset` with duplicate rows removed.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        inplace = validate_bool_kwarg(inplace, "inplace")
        ignore_index = kwargs.get("ignore_index", False)
        subset = kwargs.get("subset", None)
        if subset is not None:
            if is_list_like(subset):
                if not isinstance(subset, list):
                    subset = list(subset)
            else:
                subset = [subset]
            df = self[subset]
        else:
            df = self
        duplicated = df.duplicated(keep=keep)
        result = self[~duplicated]
        if ignore_index:
            result.index = pandas.RangeIndex(stop=len(result))
        if inplace:
            self._update_inplace(result._query_compiler)
        else:
            return result

    @base_not_implemented()
    def map(self, func, na_action: str | None = None, **kwargs):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if not callable(func):
            raise ValueError(f"'{type(func)}' object is not callable")
        return self.__constructor__(
            query_compiler=self._query_compiler.map(func, na_action=na_action, **kwargs)
        )

    def mask(
        self,
        cond: BasePandasDataset | Callable | AnyArrayLike,
        other: BasePandasDataset | Callable | Scalar | None = np.nan,
        inplace: bool = False,
        axis: Axis | None = None,
        level: Level | None = None,
    ):
        """
        Replace values where the condition is True.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # TODO: https://snowflakecomputing.atlassian.net/browse/SNOW-985670
        # will move pre-processing to QC layer.
        inplace = validate_bool_kwarg(inplace, "inplace")
        if cond is None:
            raise ValueError("Array conditional must be same shape as self")

        cond = apply_if_callable(cond, self)

        if isinstance(cond, Callable):
            raise NotImplementedError("Do not support callable for 'cond' parameter.")

        from snowflake.snowpark.modin.pandas import Series

        if isinstance(cond, Series):
            cond._query_compiler._shape_hint = "column"
        if isinstance(self, Series):
            self._query_compiler._shape_hint = "column"
        if isinstance(other, Series):
            other._query_compiler._shape_hint = "column"

        if not isinstance(cond, BasePandasDataset):
            cond = get_as_shape_compatible_dataframe_or_series(cond, self)
            cond._query_compiler._shape_hint = "array"

        if other is not None:
            other = apply_if_callable(other, self)

            if isinstance(other, np.ndarray):
                other = get_as_shape_compatible_dataframe_or_series(
                    other,
                    self,
                    shape_mismatch_message="other must be the same shape as self when an ndarray",
                )
                other._query_compiler._shape_hint = "array"

            if isinstance(other, BasePandasDataset):
                other = other._query_compiler

        query_compiler = self._query_compiler.mask(
            cond._query_compiler,
            other,
            axis,
            level,
        )

        return self._create_or_update_from_compiler(query_compiler, inplace)

    def where(
        self,
        cond: BasePandasDataset | Callable | AnyArrayLike,
        other: BasePandasDataset | Callable | Scalar | None = np.nan,
        inplace: bool = False,
        axis: Axis | None = None,
        level: Level | None = None,
    ):
        """
        Replace values where the condition is False.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # TODO: SNOW-985670: Refactor `where` and `mask`
        # will move pre-processing to QC layer.
        inplace = validate_bool_kwarg(inplace, "inplace")
        if cond is None:
            raise ValueError("Array conditional must be same shape as self")

        cond = apply_if_callable(cond, self)

        if isinstance(cond, Callable):
            raise NotImplementedError("Do not support callable for 'cond' parameter.")

        from snowflake.snowpark.modin.pandas import Series

        if isinstance(cond, Series):
            cond._query_compiler._shape_hint = "column"
        if isinstance(self, Series):
            self._query_compiler._shape_hint = "column"
        if isinstance(other, Series):
            other._query_compiler._shape_hint = "column"

        if not isinstance(cond, BasePandasDataset):
            cond = get_as_shape_compatible_dataframe_or_series(cond, self)
            cond._query_compiler._shape_hint = "array"

        if other is not None:
            other = apply_if_callable(other, self)

            if isinstance(other, np.ndarray):
                other = get_as_shape_compatible_dataframe_or_series(
                    other,
                    self,
                    shape_mismatch_message="other must be the same shape as self when an ndarray",
                )
                other._query_compiler._shape_hint = "array"

            if isinstance(other, BasePandasDataset):
                other = other._query_compiler

        query_compiler = self._query_compiler.where(
            cond._query_compiler,
            other,
            axis,
            level,
        )

        return self._create_or_update_from_compiler(query_compiler, inplace)

    def eq(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get equality of `BasePandasDataset` and `other`, element-wise (binary operator `eq`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("eq", other, axis=axis, level=level, dtypes=np.bool_)

    @base_not_implemented()
    def explode(self, column, ignore_index: bool = False):  # noqa: PR01, RT01, D200
        """
        Transform each element of a list-like to a row.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        exploded = self.__constructor__(
            query_compiler=self._query_compiler.explode(column)
        )
        if ignore_index:
            exploded = exploded.reset_index(drop=True)
        return exploded

    @base_not_implemented()
    def ewm(
        self,
        com: float | None = None,
        span: float | None = None,
        halflife: float | TimedeltaConvertibleTypes | None = None,
        alpha: float | None = None,
        min_periods: int | None = 0,
        adjust: bool = True,
        ignore_na: bool = False,
        axis: Axis = 0,
        times: str | np.ndarray | BasePandasDataset | None = None,
        method: str = "single",
    ) -> pandas.core.window.ewm.ExponentialMovingWindow:  # noqa: PR01, RT01, D200
        """
        Provide exponentially weighted (EW) calculations.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "ewm",
            com=com,
            span=span,
            halflife=halflife,
            alpha=alpha,
            min_periods=min_periods,
            adjust=adjust,
            ignore_na=ignore_na,
            axis=axis,
            times=times,
            method=method,
        )

    def expanding(
        self, min_periods=1, axis=0, method="single"
    ):  # noqa: PR01, RT01, D200
        """
        Provide expanding window calculations.
        """
        from .window import Expanding

        if axis is not lib.no_default:
            axis = self._get_axis_number(axis)
            name = "expanding"
            if axis == 1:
                warnings.warn(
                    f"Support for axis=1 in {type(self).__name__}.{name} is "
                    + "deprecated and will be removed in a future version. "
                    + f"Use obj.T.{name}(...) instead",
                    FutureWarning,
                    stacklevel=1,
                )
            else:
                warnings.warn(
                    f"The 'axis' keyword in {type(self).__name__}.{name} is "
                    + "deprecated and will be removed in a future version. "
                    + "Call the method without the axis keyword instead.",
                    FutureWarning,
                    stacklevel=1,
                )
        else:
            axis = 0

        return Expanding(
            self,
            min_periods=min_periods,
            axis=axis,
            method=method,
        )

    def ffill(
        self,
        axis: Axis | None = None,
        inplace: bool = False,
        limit: int | None = None,
        downcast: dict | None = None,
    ):
        """
        Synonym for `DataFrame.fillna` with ``method='ffill'``.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.fillna(
            method="ffill", axis=axis, limit=limit, downcast=downcast, inplace=inplace
        )

    pad = ffill

    def fillna(
        self,
        self_is_series,
        value: Hashable | Mapping | pd.Series | pd.DataFrame = None,
        method: FillnaOptions | None = None,
        axis: Axis | None = None,
        inplace: bool = False,
        limit: int | None = None,
        downcast: dict | None = None,
    ):
        """
        Fill NA/NaN values using the specified method.

        Parameters
        ----------
        self_is_series : bool
            If True then self contains a Series object, if False then self contains
            a DataFrame object.
        value : scalar, dict, Series, or DataFrame, default: None
            Value to use to fill holes (e.g. 0), alternately a
            dict/Series/DataFrame of values specifying which value to use for
            each index (for a Series) or column (for a DataFrame).  Values not
            in the dict/Series/DataFrame will not be filled. This value cannot
            be a list.
        method : {'backfill', 'bfill', 'pad', 'ffill', None}, default: None
            Method to use for filling holes in reindexed Series
            pad / ffill: propagate last valid observation forward to next valid
            backfill / bfill: use next valid observation to fill gap.
        axis : {None, 0, 1}, default: None
            Axis along which to fill missing values.
        inplace : bool, default: False
            If True, fill in-place. Note: this will modify any
            other views on this object (e.g., a no-copy slice for a column in a
            DataFrame).
        limit : int, default: None
            If method is specified, this is the maximum number of consecutive
            NaN values to forward/backward fill. In other words, if there is
            a gap with more than this number of consecutive NaNs, it will only
            be partially filled. If method is not specified, this is the
            maximum number of entries along the entire axis where NaNs will be
            filled. Must be greater than 0 if not None.
        downcast : dict, default: None
            A dict of item->dtype of what to downcast if possible,
            or the string 'infer' which will try to downcast to an appropriate
            equal type (e.g. float64 to int64 if possible).

        Returns
        -------
        Series, DataFrame or None
            Object with missing values filled or None if ``inplace=True``.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        raise_if_native_pandas_objects(value)
        inplace = validate_bool_kwarg(inplace, "inplace")
        axis = self._get_axis_number(axis)
        if isinstance(value, (list, tuple)):
            raise TypeError(
                '"value" parameter must be a scalar or dict, but '
                + f'you passed a "{type(value).__name__}"'
            )
        if value is None and method is None:
            # same as pandas
            raise ValueError("Must specify a fill 'value' or 'method'.")
        if value is not None and method is not None:
            raise ValueError("Cannot specify both 'value' and 'method'.")
        if method is not None and method not in ["backfill", "bfill", "pad", "ffill"]:
            expecting = "pad (ffill) or backfill (bfill)"
            msg = "Invalid fill method. Expecting {expecting}. Got {method}".format(
                expecting=expecting, method=method
            )
            raise ValueError(msg)
        if limit is not None:
            if not isinstance(limit, int):
                raise ValueError("Limit must be an integer")
            elif limit <= 0:
                raise ValueError("Limit must be greater than 0")

        new_query_compiler = self._query_compiler.fillna(
            self_is_series=self_is_series,
            value=value,
            method=method,
            axis=axis,
            limit=limit,
            downcast=downcast,
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    @base_not_implemented()
    def filter(
        self, items=None, like=None, regex=None, axis=None
    ):  # noqa: PR01, RT01, D200
        """
        Subset the `BasePandasDataset` rows or columns according to the specified index labels.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        nkw = count_not_none(items, like, regex)
        if nkw > 1:
            raise TypeError(
                "Keyword arguments `items`, `like`, or `regex` are mutually exclusive"
            )
        if nkw == 0:
            raise TypeError("Must pass either `items`, `like`, or `regex`")
        if axis is None:
            axis = "columns"  # This is the default info axis for dataframes

        axis = self._get_axis_number(axis)
        labels = self.columns if axis else self.index

        if items is not None:
            bool_arr = labels.isin(items)
        elif like is not None:

            def f(x):
                return like in str(x)

            bool_arr = labels.map(f).tolist()
        else:

            def f(x):
                return matcher.search(str(x)) is not None

            matcher = re.compile(regex)
            bool_arr = labels.map(f).tolist()
        if not axis:
            return self[bool_arr]
        return self[self.columns[bool_arr]]

    def first(self, offset):  # noqa: PR01, RT01, D200
        """
        Select initial periods of time series data based on a date offset.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.loc[pandas.Series(index=self.index).first(offset).index]

    def first_valid_index(self) -> Scalar | tuple[Scalar]:
        """
        Return index for first non-NA value or None, if no non-NA value is found.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._query_compiler.first_valid_index()

    def floordiv(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get integer division of `BasePandasDataset` and `other`, element-wise (binary operator `floordiv`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "floordiv", other, axis=axis, level=level, fill_value=fill_value
        )

    def ge(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get greater than or equal comparison of `BasePandasDataset` and `other`, element-wise (binary operator `ge`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("ge", other, axis=axis, level=level, dtypes=np.bool_)

    def get(self, key, default=None):  # noqa: PR01, RT01, D200
        """
        Get item from object for given key.
        """
        try:
            return self.__getitem__(key)
        except (KeyError, ValueError, IndexError):
            return default

    def gt(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get greater than comparison of `BasePandasDataset` and `other`, element-wise (binary operator `gt`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("gt", other, axis=axis, level=level, dtypes=np.bool_)

    def head(self, n: int = 5):
        """
        Return the first `n` rows.
        """
        return self.iloc[:n]

    @base_not_implemented()
    @property
    def iat(self, axis=None):  # noqa: PR01, RT01, D200
        """
        Get a single value for a row/column pair by integer position.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        from .indexing import _iLocIndexer

        return _iLocIndexer(self)

    def idxmax(self, axis=0, skipna=True, numeric_only=False):  # noqa: PR01, RT01, D200
        """
        Return index of first occurrence of maximum over requested axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        dtypes = self._get_dtypes()
        if (
            axis == 1
            and not numeric_only
            and any(not is_numeric_dtype(d) for d in dtypes)
            and len(set(dtypes)) > 1
        ):
            # For numeric_only=False, if we have any non-numeric dtype, e.g.
            # a string type, we need every other column to be of the same type.
            # We can't compare two objects of different non-numeric types, e.g.
            # a string and a timestamp.
            # If we have only numeric data, we can compare columns even if they
            # different types, e.g. we can compare an int column to a float
            # column.
            raise TypeError("'>' not supported for these dtypes")
        axis = self._get_axis_number(axis)
        return self._reduce_dimension(
            self._query_compiler.idxmax(
                axis=axis, skipna=skipna, numeric_only=numeric_only
            )
        )

    def idxmin(self, axis=0, skipna=True, numeric_only=False):  # noqa: PR01, RT01, D200
        """
        Return index of first occurrence of minimum over requested axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        dtypes = self._get_dtypes()
        if (
            axis == 1
            and not numeric_only
            and any(not is_numeric_dtype(d) for d in dtypes)
            and len(set(dtypes)) > 1
        ):
            # For numeric_only=False, if we have any non-numeric dtype, e.g.
            # a string type, we need every other column to be of the same type.
            # We can't compare two objects of different non-numeric types, e.g.
            # a string and a timestamp.
            # If we have only numeric data, we can compare columns even if they
            # different types, e.g. we can compare an int column to a float
            # column.
            raise TypeError("'<' not supported for these dtypes")
        axis = self._get_axis_number(axis)
        return self._reduce_dimension(
            self._query_compiler.idxmin(
                axis=axis, skipna=skipna, numeric_only=numeric_only
            )
        )

    @base_not_implemented()
    def infer_objects(
        self, copy: bool | None = None
    ) -> BasePandasDataset:  # pragma: no cover # noqa: RT01, D200
        """
        Attempt to infer better dtypes for object columns.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        new_query_compiler = self._query_compiler.infer_objects()
        return self._create_or_update_from_compiler(
            new_query_compiler, inplace=False if copy is None else not copy
        )

    def convert_dtypes(
        self,
        infer_objects: bool = True,
        convert_string: bool = True,
        convert_integer: bool = True,
        convert_boolean: bool = True,
        convert_floating: bool = True,
        dtype_backend: DtypeBackend = "numpy_nullable",
    ):  # noqa: PR01, RT01, D200
        """
        Convert columns to best possible dtypes using dtypes supporting ``pd.NA``.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.__constructor__(
            query_compiler=self._query_compiler.convert_dtypes(
                infer_objects=infer_objects,
                convert_string=convert_string,
                convert_integer=convert_integer,
                convert_boolean=convert_boolean,
                convert_floating=convert_floating,
                dtype_backend=dtype_backend,
            )
        )

    def isin(
        self, values: BasePandasDataset | ListLike | dict[Hashable, ListLike]
    ) -> BasePandasDataset:  # noqa: PR01, RT01, D200
        """
        Whether elements in `BasePandasDataset` are contained in `values`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset

        # Pass as query compiler if values is BasePandasDataset.
        if isinstance(values, BasePandasDataset):
            values = values._query_compiler

        # Convert non-dict values to List if values is neither List[Any] nor np.ndarray. SnowflakeQueryCompiler
        # expects for the non-lazy case, where values is not a BasePandasDataset, the data to be materialized
        # as list or numpy array. Because numpy may perform implicit type conversions, use here list to be more general.
        elif not isinstance(values, dict) and (
            not isinstance(values, list) or not isinstance(values, np.ndarray)
        ):
            values = list(values)

        return self.__constructor__(
            query_compiler=self._query_compiler.isin(values=values)
        )

    def isna(self):  # noqa: RT01, D200
        """
        Detect missing values.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.__constructor__(query_compiler=self._query_compiler.isna())

    isnull = isna

    @property
    def iloc(self):
        """
        Purely integer-location based indexing for selection by position.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # TODO: SNOW-930028 enable all skipped doctests
        from .indexing import _iLocIndexer

        return _iLocIndexer(self)

    @base_not_implemented()
    def kurt(self, axis=no_default, skipna=True, numeric_only=False, **kwargs):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        validate_bool_kwarg(skipna, "skipna", none_allowed=False)
        axis = self._get_axis_number(axis)
        if numeric_only is not None and not numeric_only:
            self._validate_dtypes(numeric_only=True)

        data = (
            self._get_numeric_data(axis)
            if numeric_only is None or numeric_only
            else self
        )

        return self._reduce_dimension(
            data._query_compiler.kurt(
                axis=axis,
                skipna=skipna,
                numeric_only=numeric_only,
                **kwargs,
            )
        )

    kurtosis = kurt

    def last(self, offset):  # noqa: PR01, RT01, D200
        """
        Select final periods of time series data based on a date offset.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.loc[pandas.Series(index=self.index).last(offset).index]

    def last_valid_index(self) -> Scalar | tuple[Scalar]:
        """
        Return index for last non-NA value or None, if no non-NA value is found.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._query_compiler.last_valid_index()

    def le(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get less than or equal comparison of `BasePandasDataset` and `other`, element-wise (binary operator `le`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("le", other, axis=axis, level=level, dtypes=np.bool_)

    def lt(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get less than comparison of `BasePandasDataset` and `other`, element-wise (binary operator `lt`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("lt", other, axis=axis, level=level, dtypes=np.bool_)

    @property
    def loc(self):
        """
        Get a group of rows and columns by label(s) or a boolean array.
        """
        # TODO: SNOW-935444 fix doctest where index key has name
        # TODO: SNOW-933782 fix multiindex transpose bug, e.g., Name: (cobra, mark ii) => Name: ('cobra', 'mark ii')
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        from .indexing import _LocIndexer

        return _LocIndexer(self)

    def _agg_helper(
        self,
        func: str,
        skipna: bool = True,
        axis: int | None | NoDefault = no_default,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        if not self._is_dataframe and numeric_only and not is_numeric_dtype(self.dtype):
            # Series aggregations on non-numeric data do not support numeric_only:
            # https://github.com/pandas-dev/pandas/blob/cece8c6579854f6b39b143e22c11cac56502c4fd/pandas/core/series.py#L6358
            raise TypeError(
                f"Series.{func} does not allow numeric_only=True with non-numeric dtypes."
            )
        axis = self._get_axis_number(axis)
        numeric_only = validate_bool_kwarg(
            numeric_only, "numeric_only", none_allowed=True
        )
        skipna = validate_bool_kwarg(skipna, "skipna", none_allowed=False)
        agg_kwargs: dict[str, Any] = {
            "numeric_only": numeric_only,
            "skipna": skipna,
        }
        agg_kwargs.update(kwargs)
        return self.aggregate(func=func, axis=axis, **agg_kwargs)

    def max(
        self,
        axis: Axis | None = 0,
        skipna: bool = True,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        """
        Return the maximum of the values over the requested axis.
        """
        return self._agg_helper(
            func="max",
            axis=axis,
            skipna=skipna,
            numeric_only=numeric_only,
            **kwargs,
        )

    def _stat_operation(
        self,
        op_name: str,
        axis: int | str,
        skipna: bool,
        numeric_only: bool = False,
        **kwargs,
    ):
        """
        Do common statistic reduce operations under frame.

        Parameters
        ----------
        op_name : str
            Name of method to apply.
        axis : int or str
            Axis to apply method on.
        skipna : bool
            Exclude NA/null values when computing the result.
        numeric_only : bool
            Include only float, int, boolean columns.
        **kwargs : dict
            Additional keyword arguments to pass to `op_name`.

        Returns
        -------
        scalar or Series
            `scalar` - self is Series
            `Series` -  self is DataFrame
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        validate_bool_kwarg(skipna, "skipna", none_allowed=False)
        if not numeric_only:
            self._validate_dtypes(numeric_only=True)

        data = self._get_numeric_data(axis) if numeric_only else self
        result_qc = getattr(data._query_compiler, op_name)(
            axis=axis,
            skipna=skipna,
            numeric_only=numeric_only,
            **kwargs,
        )
        result_qc = self._reduce_dimension(result_qc)
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # This pattern is seen throughout this file so we should try to correct it
        # when we have a more general way of resetting the name to None
        from snowflake.snowpark.modin.pandas import Series

        if isinstance(result_qc, Series):
            result_qc.name = None
        return result_qc

    def memory_usage(self, index=True, deep=False):  # noqa: PR01, RT01, D200
        """
        Return the memory usage of the `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._reduce_dimension(
            self._query_compiler.memory_usage(index=index, deep=deep)
        )

    def min(
        self,
        axis: Axis | None | NoDefault = no_default,
        skipna: bool = True,
        numeric_only: bool = False,
        **kwargs,
    ):
        """
        Return the minimum of the values over the requested axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._agg_helper(
            func="min",
            axis=axis,
            skipna=skipna,
            numeric_only=numeric_only,
            **kwargs,
        )

    def mod(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get modulo of `BasePandasDataset` and `other`, element-wise (binary operator `mod`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "mod", other, axis=axis, level=level, fill_value=fill_value
        )

    @base_not_implemented()
    def mode(self, axis=0, numeric_only=False, dropna=True):  # noqa: PR01, RT01, D200
        """
        Get the mode(s) of each element along the selected axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        return self.__constructor__(
            query_compiler=self._query_compiler.mode(
                axis=axis, numeric_only=numeric_only, dropna=dropna
            )
        )

    def mul(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get multiplication of `BasePandasDataset` and `other`, element-wise (binary operator `mul`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "mul", other, axis=axis, level=level, fill_value=fill_value
        )

    multiply = mul

    def ne(self, other, axis="columns", level=None):  # noqa: PR01, RT01, D200
        """
        Get Not equal comparison of `BasePandasDataset` and `other`, element-wise (binary operator `ne`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("ne", other, axis=axis, level=level, dtypes=np.bool_)

    def notna(self):  # noqa: RT01, D200
        """
        Detect existing (non-missing) values.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.__constructor__(query_compiler=self._query_compiler.notna())

    notnull = notna

    def nunique(self, axis=0, dropna=True):  # noqa: PR01, RT01, D200
        """
        Return number of unique elements in the `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        from snowflake.snowpark.modin.pandas import Series

        axis = self._get_axis_number(axis)
        result = self._reduce_dimension(
            self._query_compiler.nunique(axis=axis, dropna=dropna)
        )
        if isinstance(result, Series):
            result.name = None
        return result

    def pct_change(
        self, periods=1, fill_method=no_default, limit=no_default, freq=None, **kwargs
    ):  # noqa: PR01, RT01, D200
        """
        Percentage change between the current and a prior element.
        """
        if fill_method not in (lib.no_default, None) or limit is not lib.no_default:
            warnings.warn(
                "The 'fill_method' keyword being not None and the 'limit' keyword in "
                + f"{type(self).__name__}.pct_change are deprecated and will be removed "
                + "in a future version. Either fill in any non-leading NA values prior "
                + "to calling pct_change or specify 'fill_method=None' to not fill NA "
                + "values.",
                FutureWarning,
                stacklevel=1,
            )
        if fill_method is lib.no_default:
            warnings.warn(
                f"The default fill_method='pad' in {type(self).__name__}.pct_change is "
                + "deprecated and will be removed in a future version. Either fill in any "
                + "non-leading NA values prior to calling pct_change or specify 'fill_method=None' "
                + "to not fill NA values.",
                FutureWarning,
                stacklevel=1,
            )
            fill_method = "pad"

        if limit is lib.no_default:
            limit = None

        if "axis" in kwargs:
            kwargs["axis"] = self._get_axis_number(kwargs["axis"])

        # Attempting to match pandas error behavior here
        if not isinstance(periods, int):
            raise TypeError(f"periods must be an int. got {type(periods)} instead")

        # Attempting to match pandas error behavior here
        for dtype in self._get_dtypes():
            if not is_numeric_dtype(dtype):
                raise TypeError(
                    f"cannot perform pct_change on non-numeric column with dtype {dtype}"
                )

        return self.__constructor__(
            query_compiler=self._query_compiler.pct_change(
                periods=periods,
                fill_method=fill_method,
                limit=limit,
                freq=freq,
                **kwargs,
            )
        )

    @base_not_implemented()
    def pipe(self, func, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Apply chainable functions that expect `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return pipe(self, func, *args, **kwargs)

    @base_not_implemented()
    def pop(self, item):  # noqa: PR01, RT01, D200
        """
        Return item and drop from frame. Raise KeyError if not found.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        result = self[item]
        del self[item]
        return result

    def pow(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get exponential power of `BasePandasDataset` and `other`, element-wise (binary operator `pow`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "pow", other, axis=axis, level=level, fill_value=fill_value
        )

    def quantile(
        self,
        q: Scalar | ListLike = 0.5,
        axis: Axis = 0,
        numeric_only: bool = False,
        interpolation: Literal[
            "linear", "lower", "higher", "midpoint", "nearest"
        ] = "linear",
        method: Literal["single", "table"] = "single",
    ) -> float | BasePandasDataset:
        """
        Return values at the given quantile over requested axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)

        # TODO
        # - SNOW-1008361: support axis=1
        # - SNOW-1008367: support when q is Snowpandas DF/Series (need to require QC interface to accept QC q values)
        # - SNOW-1003587: support datetime/timedelta columns

        if (
            axis == 1
            or interpolation not in ["linear", "nearest"]
            or method != "single"
        ):
            ErrorMessage.not_implemented(
                f"quantile function with parameters axis={axis}, interpolation={interpolation}, method={method} not supported"
            )

        if not numeric_only:
            # If not numeric_only and columns, then check all columns are either
            # numeric, timestamp, or timedelta
            # Check if dtype is numeric, timedelta ("m"), or datetime ("M")
            if not axis and not all(
                is_numeric_dtype(t) or lib.is_np_dtype(t, "mM")
                for t in self._get_dtypes()
            ):
                raise TypeError("can't multiply sequence by non-int of type 'float'")
            # If over rows, then make sure that all dtypes are equal for not
            # numeric_only
            elif axis:
                for i in range(1, len(self._get_dtypes())):
                    pre_dtype = self._get_dtypes()[i - 1]
                    curr_dtype = self._get_dtypes()[i]
                    if not is_dtype_equal(pre_dtype, curr_dtype):
                        raise TypeError(
                            "Cannot compare type '{}' with type '{}'".format(
                                pre_dtype, curr_dtype
                            )
                        )
        else:
            # Normally pandas returns this near the end of the quantile, but we
            # can't afford the overhead of running the entire operation before
            # we error.
            if not any(is_numeric_dtype(t) for t in self._get_dtypes()):
                raise ValueError("need at least one array to concatenate")

        # check that all qs are between 0 and 1
        validate_percentile(q)
        axis = self._get_axis_number(axis)
        query_compiler = self._query_compiler.quantiles_along_axis0(
            q=q if is_list_like(q) else [q],
            numeric_only=numeric_only,
            interpolation=interpolation,
            method=method,
        )
        if is_list_like(q):
            return self.__constructor__(query_compiler=query_compiler)
        else:
            # result is either a scalar or Series
            result = self._reduce_dimension(query_compiler.transpose_single_row())
            if isinstance(result, BasePandasDataset):
                result.name = q
            return result

    @_inherit_docstrings(pandas.DataFrame.rank, apilink="pandas.DataFrame.rank")
    def rank(
        self,
        axis=0,
        method: str = "average",
        numeric_only: bool = False,
        na_option: str = "keep",
        ascending: bool = True,
        pct: bool = False,
    ):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        return self.__constructor__(
            query_compiler=self._query_compiler.rank(
                axis=axis,
                method=method,
                numeric_only=numeric_only,
                na_option=na_option,
                ascending=ascending,
                pct=pct,
            )
        )

    def _copy_index_metadata(self, source, destination):  # noqa: PR01, RT01, D200
        """
        Copy Index metadata from `source` to `destination` inplace.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if hasattr(source, "name") and hasattr(destination, "name"):
            destination.name = source.name
        if hasattr(source, "names") and hasattr(destination, "names"):
            destination.names = source.names
        return destination

    def _ensure_index(self, index_like, axis=0):  # noqa: PR01, RT01, D200
        """
        Ensure that we have an index from some index-like object.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if (
            self._query_compiler.has_multiindex(axis=axis)
            and not isinstance(index_like, pandas.Index)
            and is_list_like(index_like)
            and len(index_like) > 0
            and isinstance(index_like[0], tuple)
        ):
            try:
                return pandas.MultiIndex.from_tuples(index_like)
            except TypeError:
                # not all tuples
                pass
        return ensure_index(index_like)

    @base_not_implemented()
    def reindex(
        self,
        index=None,
        columns=None,
        copy=True,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Conform `BasePandasDataset` to new index with optional filling logic.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset

        new_query_compiler = None
        if index is not None:
            if not isinstance(index, pandas.Index) or not index.equals(self.index):
                new_query_compiler = self._query_compiler.reindex(
                    axis=0, labels=index, **kwargs
                )
        if new_query_compiler is None:
            new_query_compiler = self._query_compiler
        final_query_compiler = None
        if columns is not None:
            if not isinstance(index, pandas.Index) or not columns.equals(self.columns):
                final_query_compiler = new_query_compiler.reindex(
                    axis=1, labels=columns, **kwargs
                )
        if final_query_compiler is None:
            final_query_compiler = new_query_compiler
        return self._create_or_update_from_compiler(
            final_query_compiler, inplace=False if copy is None else not copy
        )

    @base_not_implemented()
    def reindex_like(
        self, other, method=None, copy=True, limit=None, tolerance=None
    ):  # noqa: PR01, RT01, D200
        """
        Return an object with matching indices as `other` object.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "reindex_like",
            other,
            method=method,
            copy=copy,
            limit=limit,
            tolerance=tolerance,
        )

    def rename_axis(
        self,
        mapper=lib.no_default,
        *,
        index=lib.no_default,
        columns=lib.no_default,
        axis=0,
        copy=None,
        inplace=False,
    ):  # noqa: PR01, RT01, D200
        """
        Set the name of the axis for the index or columns.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axes = {"index": index, "columns": columns}

        if copy is None:
            copy = True

        if axis is not None:
            axis = self._get_axis_number(axis)
        else:
            axis = 0

        inplace = validate_bool_kwarg(inplace, "inplace")

        if mapper is not lib.no_default and mapper is not None:
            # Use v0.23 behavior if a scalar or list
            non_mapper = is_scalar(mapper) or (
                is_list_like(mapper) and not is_dict_like(mapper)
            )
            if non_mapper:
                return self._set_axis_name(mapper, axis=axis, inplace=inplace)
            else:
                raise ValueError("Use `.rename` to alter labels with a mapper.")
        else:
            # Use new behavior.  Means that index and/or columns is specified
            result = self if inplace else self.copy(deep=copy)

            for axis in range(self.ndim):
                v = axes.get(pandas.DataFrame._get_axis_name(axis))
                if v is lib.no_default:
                    continue
                non_mapper = is_scalar(v) or (is_list_like(v) and not is_dict_like(v))
                if non_mapper:
                    newnames = v
                else:

                    def _get_rename_function(mapper):
                        if isinstance(mapper, (dict, BasePandasDataset)):

                            def f(x):
                                if x in mapper:
                                    return mapper[x]
                                else:
                                    return x

                        else:
                            f = mapper

                        return f

                    f = _get_rename_function(v)
                    curnames = self.index.names if axis == 0 else self.columns.names
                    newnames = [f(name) for name in curnames]
                result._set_axis_name(newnames, axis=axis, inplace=True)
            if not inplace:
                return result

    @base_not_implemented()
    def reorder_levels(self, order, axis=0):  # noqa: PR01, RT01, D200
        """
        Rearrange index levels using input order.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        new_labels = self.axes[axis].reorder_levels(order)
        return self.set_axis(new_labels, axis=axis)

    def resample(
        self,
        rule,
        axis: Axis = lib.no_default,
        closed: str | None = None,
        label: str | None = None,
        convention: str = "start",
        kind: str | None = None,
        on: Level = None,
        level: Level = None,
        origin: str | TimestampConvertibleTypes = "start_day",
        offset: TimedeltaConvertibleTypes | None = None,
        group_keys=no_default,
    ):  # noqa: PR01, RT01, D200
        """
        Resample time-series data.
        """
        from .resample import Resampler

        if axis is not lib.no_default:  # pragma: no cover
            axis = self._get_axis_number(axis)
            if axis == 1:
                warnings.warn(
                    "DataFrame.resample with axis=1 is deprecated. Do "
                    + "`frame.T.resample(...)` without axis instead.",
                    FutureWarning,
                    stacklevel=1,
                )
            else:
                warnings.warn(
                    f"The 'axis' keyword in {type(self).__name__}.resample is "
                    + "deprecated and will be removed in a future version.",
                    FutureWarning,
                    stacklevel=1,
                )
        else:
            axis = 0

        return Resampler(
            dataframe=self,
            rule=rule,
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

    def reset_index(
        self,
        level: IndexLabel = None,
        drop: bool = False,
        inplace: bool = False,
        col_level: Hashable = 0,
        col_fill: Hashable = "",
        allow_duplicates=no_default,
        names: Hashable | Sequence[Hashable] = None,
    ):
        """
        Reset the index, or a level of it.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        inplace = validate_bool_kwarg(inplace, "inplace")
        if allow_duplicates is no_default:
            allow_duplicates = False
        new_query_compiler = self._query_compiler.reset_index(
            drop=drop,
            level=level,
            col_level=col_level,
            col_fill=col_fill,
            allow_duplicates=allow_duplicates,
            names=names,
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    def radd(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Return addition of `BasePandasDataset` and `other`, element-wise (binary operator `radd`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "radd", other, axis=axis, level=level, fill_value=fill_value
        )

    def rfloordiv(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get integer division of `BasePandasDataset` and `other`, element-wise (binary operator `rfloordiv`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "rfloordiv", other, axis=axis, level=level, fill_value=fill_value
        )

    def rmod(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get modulo of `BasePandasDataset` and `other`, element-wise (binary operator `rmod`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "rmod", other, axis=axis, level=level, fill_value=fill_value
        )

    def rmul(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get Multiplication of dataframe and other, element-wise (binary operator `rmul`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "rmul", other, axis=axis, level=level, fill_value=fill_value
        )

    def rolling(
        self,
        window,
        min_periods: int | None = None,
        center: bool = False,
        win_type: str | None = None,
        on: str | None = None,
        axis: Axis = lib.no_default,
        closed: str | None = None,
        step: int | None = None,
        method: str = "single",
    ):  # noqa: PR01, RT01, D200
        """
        Provide rolling window calculations.
        """
        if axis is not lib.no_default:
            axis = self._get_axis_number(axis)
            name = "rolling"
            if axis == 1:
                warnings.warn(
                    f"Support for axis=1 in {type(self).__name__}.{name} is "
                    + "deprecated and will be removed in a future version. "
                    + f"Use obj.T.{name}(...) instead",
                    FutureWarning,
                    stacklevel=1,
                )
            else:  # pragma: no cover
                warnings.warn(
                    f"The 'axis' keyword in {type(self).__name__}.{name} is "
                    + "deprecated and will be removed in a future version. "
                    + "Call the method without the axis keyword instead.",
                    FutureWarning,
                    stacklevel=1,
                )
        else:
            axis = 0

        if win_type is not None:
            from .window import Window

            return Window(
                self,
                window=window,
                min_periods=min_periods,
                center=center,
                win_type=win_type,
                on=on,
                axis=axis,
                closed=closed,
                step=step,
                method=method,
            )
        from .window import Rolling

        return Rolling(
            self,
            window=window,
            min_periods=min_periods,
            center=center,
            win_type=win_type,
            on=on,
            axis=axis,
            closed=closed,
            step=step,
            method=method,
        )

    def round(self, decimals=0, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Round a `BasePandasDataset` to a variable number of decimal places.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # FIXME: Judging by pandas docs `*args` and `**kwargs` serves only compatibility
        # purpose and does not affect the result, we shouldn't pass them to the query compiler.
        return self.__constructor__(
            query_compiler=self._query_compiler.round(decimals=decimals, **kwargs)
        )

    def rpow(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get exponential power of `BasePandasDataset` and `other`, element-wise (binary operator `rpow`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "rpow", other, axis=axis, level=level, fill_value=fill_value
        )

    def rsub(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get subtraction of `BasePandasDataset` and `other`, element-wise (binary operator `rsub`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "rsub", other, axis=axis, level=level, fill_value=fill_value
        )

    def rtruediv(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get floating division of `BasePandasDataset` and `other`, element-wise (binary operator `rtruediv`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "rtruediv", other, axis=axis, level=level, fill_value=fill_value
        )

    rdiv = rtruediv

    def sample(
        self,
        n: int | None = None,
        frac: float | None = None,
        replace: bool = False,
        weights: str | np.ndarray | None = None,
        random_state: RandomState | None = None,
        axis: Axis | None = None,
        ignore_index: bool = False,
    ):
        """
        Return a random sample of items from an axis of object.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if self._get_axis_number(axis):
            if weights is not None and isinstance(weights, str):
                raise ValueError(
                    "Strings can only be passed to weights when sampling from rows on a DataFrame"
                )
        else:
            if n is None and frac is None:
                n = 1
            elif n is not None and frac is not None:
                raise ValueError("Please enter a value for `frac` OR `n`, not both")
            else:
                if n is not None:
                    if n < 0:
                        raise ValueError(
                            "A negative number of rows requested. Please provide `n` >= 0."
                        )
                    if n % 1 != 0:
                        raise ValueError("Only integers accepted as `n` values")
                else:
                    if frac < 0:
                        raise ValueError(
                            "A negative number of rows requested. Please provide `frac` >= 0."
                        )

        query_compiler = self._query_compiler.sample(
            n, frac, replace, weights, random_state, axis, ignore_index
        )
        return self.__constructor__(query_compiler=query_compiler)

    @base_not_implemented()
    def sem(
        self,
        axis: Axis | None = None,
        skipna: bool = True,
        ddof: int = 1,
        numeric_only=False,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Return unbiased standard error of the mean over requested axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._stat_operation(
            "sem", axis, skipna, numeric_only, ddof=ddof, **kwargs
        )

    def mean(
        self,
        axis: Axis | None | NoDefault = no_default,
        skipna: bool = True,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        """
        Return the mean of the values over the requested axis.
        """
        return self._agg_helper(
            func="mean",
            axis=axis,
            skipna=skipna,
            numeric_only=numeric_only,
            **kwargs,
        )

    def median(
        self,
        axis: Axis | None | NoDefault = no_default,
        skipna: bool = True,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        """
        Return the mean of the values over the requested axis.
        """
        return self._agg_helper(
            func="median",
            axis=axis,
            skipna=skipna,
            numeric_only=numeric_only,
            **kwargs,
        )

    @base_not_implemented()
    def set_flags(
        self, *, copy: bool = False, allows_duplicate_labels: bool | None = None
    ):  # noqa: PR01, RT01, D200
        """
        Return a new `BasePandasDataset` with updated flags.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            pandas.DataFrame.set_flags,
            copy=copy,
            allows_duplicate_labels=allows_duplicate_labels,
        )

    @property
    def flags(self):
        return self._default_to_pandas(lambda df: df.flags)

    def shift(
        self,
        periods: int | Sequence[int] = 1,
        freq=None,
        axis: Axis = 0,
        fill_value: Hashable = no_default,
        suffix: str | None = None,
    ) -> BasePandasDataset:
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if periods == 0 and freq is None:
            # Check obvious case first, freq manipulates the index even for periods == 0 so check for it in addition.
            return self.copy()

        # pandas compatible ValueError for freq='infer'
        # TODO: Test as part of SNOW-1023324.
        if freq == "infer":  # pragma: no cover
            if not hasattr(self, "freq") and not hasattr(  # pragma: no cover
                self, "inferred_freq"  # pragma: no cover
            ):  # pragma: no cover
                raise ValueError()  # pragma: no cover

        axis = self._get_axis_number(axis)

        if fill_value == no_default:
            fill_value = None

        new_query_compiler = self._query_compiler.shift(
            periods, freq, axis, fill_value, suffix
        )
        return self._create_or_update_from_compiler(new_query_compiler, False)

    def skew(
        self,
        axis: Axis | None | NoDefault = no_default,
        skipna: bool = True,
        numeric_only=True,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        """
        Return unbiased skew over requested axis.
        """
        return self._stat_operation("skew", axis, skipna, numeric_only, **kwargs)

    def sort_index(
        self,
        axis=0,
        level=None,
        ascending=True,
        inplace=False,
        kind="quicksort",
        na_position="last",
        sort_remaining=True,
        ignore_index: bool = False,
        key: IndexKeyFunc | None = None,
    ):  # noqa: PR01, RT01, D200
        """
        Sort object by labels (along an axis).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        # pandas throws this exception. See pandas issue #39434
        if ascending is None:
            raise ValueError(
                "the `axis` parameter is not supported in the pandas implementation of argsort()"
            )
        axis = self._get_axis_number(axis)
        inplace = validate_bool_kwarg(inplace, "inplace")
        new_query_compiler = self._query_compiler.sort_index(
            axis=axis,
            level=level,
            ascending=ascending,
            inplace=inplace,
            kind=kind,
            na_position=na_position,
            sort_remaining=sort_remaining,
            ignore_index=ignore_index,
            key=key,
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    def sort_values(
        self,
        by,
        axis=0,
        ascending=True,
        inplace: bool = False,
        kind="quicksort",
        na_position="last",
        ignore_index: bool = False,
        key: IndexKeyFunc | None = None,
    ):  # noqa: PR01, RT01, D200
        """
        Sort by the values along either axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        inplace = validate_bool_kwarg(inplace, "inplace")
        ascending = validate_ascending(ascending)
        if axis == 0:
            # If any column is None raise KeyError (same a native pandas).
            if by is None or (isinstance(by, list) and None in by):
                # Same error message as native pandas.
                raise KeyError(None)
            if not isinstance(by, list):
                by = [by]

            # Convert 'ascending' to sequence if needed.
            if not isinstance(ascending, Sequence):
                ascending = [ascending] * len(by)
            if len(by) != len(ascending):
                # Same error message as native pandas.
                raise ValueError(
                    f"Length of ascending ({len(ascending)})"
                    f" != length of by ({len(by)})"
                )

            columns = self._query_compiler.columns.values.tolist()
            index_names = self._query_compiler.get_index_names()
            for by_col in by:
                col_count = columns.count(by_col)
                index_count = index_names.count(by_col)
                if col_count == 0 and index_count == 0:
                    # Same error message as native pandas.
                    raise KeyError(by_col)
                if col_count and index_count:
                    # Same error message as native pandas.
                    raise ValueError(
                        f"'{by_col}' is both an index level and a column label, which is ambiguous."
                    )
                if col_count > 1:
                    # Same error message as native pandas.
                    raise ValueError(f"The column label '{by_col}' is not unique.")

            if na_position not in get_args(NaPosition):
                # Same error message as native pandas for invalid 'na_position' value.
                raise ValueError(f"invalid na_position: {na_position}")
            result = self._query_compiler.sort_rows_by_column_values(
                by,
                ascending=ascending,
                kind=kind,
                na_position=na_position,
                ignore_index=ignore_index,
                key=key,
            )
        else:
            result = self._query_compiler.sort_columns_by_row_values(
                by,
                ascending=ascending,
                kind=kind,
                na_position=na_position,
                ignore_index=ignore_index,
                key=key,
            )
        return self._create_or_update_from_compiler(result, inplace)

    def std(
        self,
        axis: Axis | None = None,
        skipna: bool = True,
        ddof: int = 1,
        numeric_only: bool = False,
        **kwargs,
    ):
        """
        Return sample standard deviation over requested axis.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        kwargs.update({"ddof": ddof})
        return self._agg_helper(
            func="std",
            axis=axis,
            skipna=skipna,
            numeric_only=numeric_only,
            **kwargs,
        )

    def sum(
        self,
        axis: Axis | None = None,
        skipna: bool = True,
        numeric_only: bool = False,
        min_count: int = 0,
        **kwargs: Any,
    ):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        min_count = validate_int_kwarg(min_count, "min_count")
        kwargs.update({"min_count": min_count})
        return self._agg_helper(
            func="sum",
            axis=axis,
            skipna=skipna,
            numeric_only=numeric_only,
            **kwargs,
        )

    def sub(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get subtraction of `BasePandasDataset` and `other`, element-wise (binary operator `sub`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "sub", other, axis=axis, level=level, fill_value=fill_value
        )

    subtract = sub

    @base_not_implemented()
    def swapaxes(self, axis1, axis2, copy=True):  # noqa: PR01, RT01, D200
        """
        Interchange axes and swap values axes appropriately.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis1 = self._get_axis_number(axis1)
        axis2 = self._get_axis_number(axis2)
        if axis1 != axis2:
            return self.transpose()
        if copy:
            return self.copy()
        return self

    @base_not_implemented()
    def swaplevel(self, i=-2, j=-1, axis=0):  # noqa: PR01, RT01, D200
        """
        Swap levels `i` and `j` in a `MultiIndex`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        idx = self.index if axis == 0 else self.columns
        return self.set_axis(idx.swaplevel(i, j), axis=axis)

    def tail(self, n: int = 5):
        if n == 0:
            return self.iloc[0:0]
        return self.iloc[-n:]

    def take(
        self,
        indices: list | AnyArrayLike | slice,
        axis: Axis = 0,
        **kwargs,
    ):
        """
        Return the elements in the given *positional* indices along an axis.
        """
        axis = self._get_axis_number(axis)
        slice_obj = indices if axis == 0 else (slice(None), indices)
        return self.iloc[slice_obj]

    @base_not_implemented()
    def to_clipboard(
        self, excel=True, sep=None, **kwargs
    ):  # pragma: no cover  # noqa: PR01, RT01, D200
        """
        Copy object to the system clipboard.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas("to_clipboard", excel=excel, sep=sep, **kwargs)

    @base_not_implemented()
    def to_csv(
        self,
        path_or_buf=None,
        sep=",",
        na_rep="",
        float_format=None,
        columns=None,
        header=True,
        index=True,
        index_label=None,
        mode="w",
        encoding=None,
        compression="infer",
        quoting=None,
        quotechar='"',
        lineterminator=None,
        chunksize=None,
        date_format=None,
        doublequote=True,
        escapechar=None,
        decimal=".",
        errors: str = "strict",
        storage_options: StorageOptions = None,
    ):  # pragma: no cover
        from snowflake.snowpark.modin.pandas.core.execution.dispatching.factories.dispatcher import (
            FactoryDispatcher,
        )

        return FactoryDispatcher.to_csv(
            self._query_compiler,
            path_or_buf=path_or_buf,
            sep=sep,
            na_rep=na_rep,
            float_format=float_format,
            columns=columns,
            header=header,
            index=index,
            index_label=index_label,
            mode=mode,
            encoding=encoding,
            compression=compression,
            quoting=quoting,
            quotechar=quotechar,
            lineterminator=lineterminator,
            chunksize=chunksize,
            date_format=date_format,
            doublequote=doublequote,
            escapechar=escapechar,
            decimal=decimal,
            errors=errors,
            storage_options=storage_options,
        )

    @base_not_implemented()
    def to_excel(
        self,
        excel_writer,
        sheet_name="Sheet1",
        na_rep="",
        float_format=None,
        columns=None,
        header=True,
        index=True,
        index_label=None,
        startrow=0,
        startcol=0,
        engine=None,
        merge_cells=True,
        encoding=no_default,
        inf_rep="inf",
        verbose=no_default,
        freeze_panes=None,
        storage_options: StorageOptions = None,
    ):  # pragma: no cover  # noqa: PR01, RT01, D200
        """
        Write object to an Excel sheet.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "to_excel",
            excel_writer,
            sheet_name=sheet_name,
            na_rep=na_rep,
            float_format=float_format,
            columns=columns,
            header=header,
            index=index,
            index_label=index_label,
            startrow=startrow,
            startcol=startcol,
            engine=engine,
            merge_cells=merge_cells,
            inf_rep=inf_rep,
            freeze_panes=freeze_panes,
            storage_options=storage_options,
        )

    @base_not_implemented()
    def to_hdf(
        self, path_or_buf, key, format="table", **kwargs
    ):  # pragma: no cover  # noqa: PR01, RT01, D200
        """
        Write the contained data to an HDF5 file using HDFStore.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "to_hdf", path_or_buf, key, format=format, **kwargs
        )

    @base_not_implemented()
    def to_json(
        self,
        path_or_buf=None,
        orient=None,
        date_format=None,
        double_precision=10,
        force_ascii=True,
        date_unit="ms",
        default_handler=None,
        lines=False,
        compression="infer",
        index=True,
        indent=None,
        storage_options: StorageOptions = None,
    ):  # pragma: no cover  # noqa: PR01, RT01, D200
        """
        Convert the object to a JSON string.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "to_json",
            path_or_buf,
            orient=orient,
            date_format=date_format,
            double_precision=double_precision,
            force_ascii=force_ascii,
            date_unit=date_unit,
            default_handler=default_handler,
            lines=lines,
            compression=compression,
            index=index,
            indent=indent,
            storage_options=storage_options,
        )

    @base_not_implemented()
    def to_latex(
        self,
        buf=None,
        columns=None,
        col_space=None,
        header=True,
        index=True,
        na_rep="NaN",
        formatters=None,
        float_format=None,
        sparsify=None,
        index_names=True,
        bold_rows=False,
        column_format=None,
        longtable=None,
        escape=None,
        encoding=None,
        decimal=".",
        multicolumn=None,
        multicolumn_format=None,
        multirow=None,
        caption=None,
        label=None,
        position=None,
    ):  # pragma: no cover  # noqa: PR01, RT01, D200
        """
        Render object to a LaTeX tabular, longtable, or nested table.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "to_latex",
            buf=buf,
            columns=columns,
            col_space=col_space,
            header=header,
            index=index,
            na_rep=na_rep,
            formatters=formatters,
            float_format=float_format,
            sparsify=sparsify,
            index_names=index_names,
            bold_rows=bold_rows,
            column_format=column_format,
            longtable=longtable,
            escape=escape,
            encoding=encoding,
            decimal=decimal,
            multicolumn=multicolumn,
            multicolumn_format=multicolumn_format,
            multirow=multirow,
            caption=caption,
            label=label,
            position=position,
        )

    @base_not_implemented()
    def to_markdown(
        self,
        buf=None,
        mode: str = "wt",
        index: bool = True,
        storage_options: StorageOptions = None,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Print `BasePandasDataset` in Markdown-friendly format.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "to_markdown",
            buf=buf,
            mode=mode,
            index=index,
            storage_options=storage_options,
            **kwargs,
        )

    @base_not_implemented()
    def to_pickle(
        self,
        path,
        compression: CompressionOptions = "infer",
        protocol: int = pkl.HIGHEST_PROTOCOL,
        storage_options: StorageOptions = None,
    ):  # pragma: no cover  # noqa: PR01, D200
        """
        Pickle (serialize) object to file.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        from snowflake.snowpark.modin.pandas import to_pickle

        to_pickle(
            self,
            path,
            compression=compression,
            protocol=protocol,
            storage_options=storage_options,
        )

    def to_numpy(
        self,
        dtype: npt.DTypeLike | None = None,
        copy: bool = False,
        na_value: object = no_default,
        **kwargs: Any,
    ) -> np.ndarray:
        """
        Convert the `BasePandasDataset` to a NumPy array or a Modin wrapper for NumPy array.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        if copy:
            WarningMessage.ignored_argument(
                operation="to_numpy",
                argument="copy",
                message="copy is ignored in Snowflake backend",
            )
        return self._query_compiler.to_numpy(
            dtype=dtype,
            na_value=na_value,
            **kwargs,
        )

    # TODO(williamma12): When this gets implemented, have the series one call this.
    def to_period(
        self, freq=None, axis=0, copy=True
    ):  # pragma: no cover  # noqa: PR01, RT01, D200
        """
        Convert `BasePandasDataset` from DatetimeIndex to PeriodIndex.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas("to_period", freq=freq, axis=axis, copy=copy)

    @base_not_implemented()
    def to_string(
        self,
        buf=None,
        columns=None,
        col_space=None,
        header=True,
        index=True,
        na_rep="NaN",
        formatters=None,
        float_format=None,
        sparsify=None,
        index_names=True,
        justify=None,
        max_rows=None,
        min_rows=None,
        max_cols=None,
        show_dimensions=False,
        decimal=".",
        line_width=None,
        max_colwidth=None,
        encoding=None,
    ):  # noqa: PR01, RT01, D200
        """
        Render a `BasePandasDataset` to a console-friendly tabular output.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "to_string",
            buf=buf,
            columns=columns,
            col_space=col_space,
            header=header,
            index=index,
            na_rep=na_rep,
            formatters=formatters,
            float_format=float_format,
            sparsify=sparsify,
            index_names=index_names,
            justify=justify,
            max_rows=max_rows,
            max_cols=max_cols,
            show_dimensions=show_dimensions,
            decimal=decimal,
            line_width=line_width,
            max_colwidth=max_colwidth,
            encoding=encoding,
        )

    @base_not_implemented()
    def to_sql(
        self,
        name,
        con,
        schema=None,
        if_exists="fail",
        index=True,
        index_label=None,
        chunksize=None,
        dtype=None,
        method=None,
    ):  # noqa: PR01, D200
        """
        Write records stored in a `BasePandasDataset` to a SQL database.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        new_query_compiler = self._query_compiler
        # writing the index to the database by inserting it to the DF
        if index:
            if not index_label:
                index_label = "index"
            new_query_compiler = new_query_compiler.insert(0, index_label, self.index)
            # so pandas._to_sql will not write the index to the database as well
            index = False

        from modin.core.execution.dispatching.factories.dispatcher import (
            FactoryDispatcher,
        )

        FactoryDispatcher.to_sql(
            new_query_compiler,
            name=name,
            con=con,
            schema=schema,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
            method=method,
        )

    # TODO(williamma12): When this gets implemented, have the series one call this.
    @base_not_implemented()
    def to_timestamp(
        self, freq=None, how="start", axis=0, copy=True
    ):  # noqa: PR01, RT01, D200
        """
        Cast to DatetimeIndex of timestamps, at *beginning* of period.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas(
            "to_timestamp", freq=freq, how=how, axis=axis, copy=copy
        )

    @base_not_implemented()
    def to_xarray(self):  # noqa: PR01, RT01, D200
        """
        Return an xarray object from the `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas("to_xarray")

    def truediv(
        self, other, axis="columns", level=None, fill_value=None
    ):  # noqa: PR01, RT01, D200
        """
        Get floating division of `BasePandasDataset` and `other`, element-wise (binary operator `truediv`).
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op(
            "truediv", other, axis=axis, level=level, fill_value=fill_value
        )

    div = divide = truediv

    @base_not_implemented()
    def truncate(
        self, before=None, after=None, axis=None, copy=True
    ):  # noqa: PR01, RT01, D200
        """
        Truncate a `BasePandasDataset` before and after some index value.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        if (
            not self.axes[axis].is_monotonic_increasing
            and not self.axes[axis].is_monotonic_decreasing
        ):
            raise ValueError("truncate requires a sorted index")
        s = slice(*self.axes[axis].slice_locs(before, after))
        slice_obj = s if axis == 0 else (slice(None), s)
        return self.iloc[slice_obj]

    @base_not_implemented()
    def transform(self, func, axis=0, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Call ``func`` on self producing a `BasePandasDataset` with the same axis shape as self.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        kwargs["is_transform"] = True
        self._validate_function(func)
        try:
            result = self.agg(func, axis=axis, *args, **kwargs)
        except TypeError:
            raise
        except Exception as err:
            raise ValueError("Transform function failed") from err
        try:
            assert len(result) == len(self)
        except Exception:
            raise ValueError("transforms cannot produce aggregated results")
        return result

    @base_not_implemented()
    def tz_convert(self, tz, axis=0, level=None, copy=True):  # noqa: PR01, RT01, D200
        """
        Convert tz-aware axis to target time zone.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        if level is not None:
            new_labels = (
                pandas.Series(index=self.axes[axis]).tz_convert(tz, level=level).index
            )
        else:
            new_labels = self.axes[axis].tz_convert(tz)
        obj = self.copy() if copy else self
        return obj.set_axis(new_labels, axis, copy=copy)

    @base_not_implemented()
    def tz_localize(
        self, tz, axis=0, level=None, copy=True, ambiguous="raise", nonexistent="raise"
    ):  # noqa: PR01, RT01, D200
        """
        Localize tz-naive index of a `BasePandasDataset` to target time zone.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        axis = self._get_axis_number(axis)
        new_labels = (
            pandas.Series(index=self.axes[axis])
            .tz_localize(
                tz,
                axis=axis,
                level=level,
                copy=False,
                ambiguous=ambiguous,
                nonexistent=nonexistent,
            )
            .index
        )
        return self.set_axis(new_labels, axis, copy=copy)

    def var(
        self,
        axis: Axis | None = None,
        skipna: bool = True,
        ddof: int = 1,
        numeric_only: bool = False,
        **kwargs: Any,
    ):
        """
        Return unbiased variance over requested axis.
        """
        kwargs.update({"ddof": ddof})
        return self._agg_helper(
            func="var",
            axis=axis,
            skipna=skipna,
            numeric_only=numeric_only,
            **kwargs,
        )

    def __abs__(self):
        """
        Return a `BasePandasDataset` with absolute numeric value of each element.

        Returns
        -------
        BasePandasDataset
            Object containing the absolute value of each element.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.abs()

    def __and__(self, other):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("__and__", other, axis=0)

    def __rand__(self, other):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("__rand__", other, axis=0)

    def __array__(self, dtype=None):
        """
        Return the values as a NumPy array.

        Parameters
        ----------
        dtype : str or np.dtype, optional
            The dtype of returned array.

        Returns
        -------
        arr : np.ndarray
            NumPy representation of Modin object.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        WarningMessage.single_warning(
            "Calling __array__ on a modin object materializes all data into local memory.\n"
            + "Since this can be called by 3rd party libraries silently, it can lead to \n"
            + "unexpected delays or high memory usage. Use to_pandas() or to_numpy() to do \n"
            + "this once explicitly.",
        )
        arr = self.to_numpy(dtype)
        return arr

    @base_not_implemented()
    def __array_wrap__(self, result, context=None):
        """
        Get called after a ufunc and other functions.

        Parameters
        ----------
        result : np.ndarray
            The result of the ufunc or other function called on the NumPy array
            returned by __array__.
        context : tuple of (func, tuple, int), optional
            This parameter is returned by ufuncs as a 3-element tuple: (name of the
            ufunc, arguments of the ufunc, domain of the ufunc), but is not set by
            other NumPy functions.

        Returns
        -------
        BasePandasDataset
            Wrapped Modin object.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset

        # TODO: This is very inefficient. __array__ and as_matrix have been
        # changed to call the more efficient to_numpy, but this has been left
        # unchanged since we are not sure of its purpose.
        return self._default_to_pandas("__array_wrap__", result, context=context)

    def __copy__(self, deep=True):
        """
        Return the copy of the `BasePandasDataset`.

        Parameters
        ----------
        deep : bool, default: True
            Whether the copy should be deep or not.

        Returns
        -------
        BasePandasDataset
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.copy(deep=deep)

    def __deepcopy__(self, memo=None):
        """
        Return the deep copy of the `BasePandasDataset`.

        Parameters
        ----------
        memo : Any, optional
           Deprecated parameter.

        Returns
        -------
        BasePandasDataset
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.copy(deep=True)

    def __eq__(self, other):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.eq(other)

    @base_not_implemented()
    def __finalize__(self, other, method=None, **kwargs):
        """
        Propagate metadata from `other` to `self`.

        Parameters
        ----------
        other : BasePandasDataset
            The object from which to get the attributes that we are going
            to propagate.
        method : str, optional
            A passed method name providing context on where `__finalize__`
            was called.
        **kwargs : dict
            Additional keywords arguments to be passed to `__finalize__`.

        Returns
        -------
        BasePandasDataset
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._default_to_pandas("__finalize__", other, method=method, **kwargs)

    def __ge__(self, right):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.ge(right)

    def __getitem__(self, key):
        """
        Retrieve dataset according to `key`.

        Parameters
        ----------
        key : callable, scalar, slice, str or tuple
            The global row index to retrieve data from.

        Returns
        -------
        BasePandasDataset
            Located dataset.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        key = apply_if_callable(key, self)
        # If a slice is passed in, use .iloc[key].
        if isinstance(key, slice):
            if (is_integer(key.start) or key.start is None) and (
                is_integer(key.stop) or key.stop is None
            ):
                return self.iloc[key]
            else:
                return self.loc[key]

        # If the object calling getitem is a Series, only use .loc[key] to filter index.
        if isinstance(self, pd.Series):
            return self.loc[key]

        # Sometimes the result of a callable is a DataFrame (e.g. df[df > 0]) - use where.
        elif isinstance(key, pd.DataFrame):
            return self.where(cond=key)

        # If the object is a boolean list-like object, use .loc[key] to filter index.
        # The if statement is structured this way to avoid calling dtype and reduce query count.
        if isinstance(key, pd.Series):
            if key.dtype == bool:
                return self.loc[key]
        elif is_list_like(key):
            if hasattr(key, "dtype"):
                if key.dtype == bool:
                    return self.loc[key]
            if (all(is_bool(k) for k in key)) and len(key) > 0:
                return self.loc[key]

        # In all other cases, use .loc[:, key] to filter columns.
        return self.loc[:, key]

    __hash__ = None

    def __gt__(self, right):
        return self.gt(right)

    def __invert__(self):
        """
        Apply bitwise inverse to each element of the `BasePandasDataset`.

        Returns
        -------
        BasePandasDataset
            New BasePandasDataset containing bitwise inverse to each value.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.__constructor__(query_compiler=self._query_compiler.invert())

    def __le__(self, right):
        return self.le(right)

    def __len__(self) -> int:
        """
        Return length of info axis.

        Returns
        -------
        int
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._query_compiler.get_axis_len(axis=0)

    def __lt__(self, right):
        return self.lt(right)

    def __matmul__(self, other):
        """
        Compute the matrix multiplication between the `BasePandasDataset` and `other`.

        Parameters
        ----------
        other : BasePandasDataset or array-like
            The other object to compute the matrix product with.

        Returns
        -------
        BasePandasDataset, np.ndarray or scalar
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.dot(other)

    def __ne__(self, other):
        return self.ne(other)

    def __neg__(self):
        """
        Change the sign for every value of self.

        Returns
        -------
        BasePandasDataset
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.__constructor__(
            query_compiler=self._query_compiler.unary_op("__neg__")
        )

    def __nonzero__(self):
        """
        Evaluate `BasePandasDataset` as boolean object.

        Raises
        ------
        ValueError
            Always since truth value for self is ambiguous.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        raise ValueError(
            f"The truth value of a {self.__class__.__name__} is ambiguous. "
            + "Use a.empty, a.bool(), a.item(), a.any() or a.all()."
        )

    __bool__ = __nonzero__

    def __or__(self, other):
        return self._binary_op("__or__", other, axis=0)

    def __ror__(self, other):
        return self._binary_op("__ror__", other, axis=0)

    @base_not_implemented()
    def __sizeof__(self):
        """
        Generate the total memory usage for an `BasePandasDataset`.

        Returns
        -------
        int
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset

        return self._default_to_pandas("__sizeof__")

    def __str__(self):  # pragma: no cover
        """
        Return str(self).

        Returns
        -------
        str
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return repr(self)

    def __xor__(self, other):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("__xor__", other, axis=0)

    def __rxor__(self, other):
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self._binary_op("__rxor__", other, axis=0)

    @property
    def size(self) -> int:
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return np.prod(self.shape)  # type: ignore[return-value]

    @property
    def values(self) -> np.ndarray:
        """
        Return a NumPy representation of the `BasePandasDataset`.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        return self.to_numpy()

    def _repartition(self, axis: int | None = None):
        """
        Repartitioning Modin objects to get ideal partitions inside.

        Allows to improve performance where the query compiler can't improve
        yet by doing implicit repartitioning.

        Parameters
        ----------
        axis : {0, 1, None}, optional
            The axis along which the repartitioning occurs.
            `None` is used for repartitioning along both axes.

        Returns
        -------
        DataFrame or Series
            The repartitioned dataframe or series, depending on the original type.
        """
        # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
        allowed_axis_values = (0, 1, None)
        if axis not in allowed_axis_values:
            raise ValueError(
                f"Passed `axis` parameter: {axis}, but should be one of {allowed_axis_values}"
            )
        return self.__constructor__(
            query_compiler=self._query_compiler.repartition(axis=axis)
        )

    def __array_ufunc__(self, ufunc: np.ufunc, method: str, *inputs, **kwargs):
        """
        Apply the `ufunc` to the `BasePandasDataset`.

        Parameters
        ----------
        ufunc : np.ufunc
            The NumPy ufunc to apply.
        method : str
            The method to apply.
        *inputs : tuple
            The inputs to the ufunc.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        BasePandasDataset
            The result of the ufunc applied to the `BasePandasDataset`.
        """
        # Use pandas version of ufunc if it exists
        if method != "__call__":
            # Return sentinel value NotImplemented
            return NotImplemented
        from snowflake.snowpark.modin.plugin.utils.numpy_to_pandas import (
            numpy_to_pandas_universal_func_map,
        )

        if ufunc.__name__ in numpy_to_pandas_universal_func_map:
            ufunc = numpy_to_pandas_universal_func_map[ufunc.__name__]
            return ufunc(self, inputs[1:], kwargs)
        # return the sentinel NotImplemented if we do not support this function
        return NotImplemented

    def __array_function__(
        self, func: callable, types: tuple, args: tuple, kwargs: dict
    ):
        """
        Apply the `func` to the `BasePandasDataset`.

        Parameters
        ----------
        func : np.func
            The NumPy func to apply.
        types : tuple
            The types of the args.
        args : tuple
            The args to the func.
        kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        BasePandasDataset
            The result of the ufunc applied to the `BasePandasDataset`.
        """
        from snowflake.snowpark.modin.plugin.utils.numpy_to_pandas import (
            numpy_to_pandas_func_map,
        )

        if func.__name__ in numpy_to_pandas_func_map:
            return numpy_to_pandas_func_map[func.__name__](*args, **kwargs)
        else:
            # per NEP18 we raise NotImplementedError so that numpy can intercept
            return NotImplemented  # pragma: no cover
