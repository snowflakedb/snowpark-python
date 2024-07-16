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

"""Module houses `Series` class, that is distributed version of `pandas.Series`."""

from __future__ import annotations

from collections.abc import Hashable, Mapping, Sequence
from logging import getLogger
from typing import IO, TYPE_CHECKING, Any, Callable, Literal

import numpy as np
import numpy.typing as npt
import pandas
from modin.pandas.accessor import CachedAccessor, SparseAccessor
from modin.pandas.iterator import PartitionIterator
from pandas._libs.lib import NoDefault, is_integer, no_default
from pandas._typing import (
    AggFuncType,
    AnyArrayLike,
    Axis,
    FillnaOptions,
    IgnoreRaise,
    IndexKeyFunc,
    IndexLabel,
    Level,
    Renamer,
    Scalar,
)
from pandas.api.types import is_datetime64_any_dtype, is_string_dtype
from pandas.core.common import apply_if_callable, is_bool_indexer
from pandas.core.dtypes.common import is_bool_dtype, is_dict_like, is_list_like
from pandas.core.series import _coerce_method
from pandas.util._validators import validate_bool_kwarg

from snowflake.snowpark.modin.pandas.base import _ATTRS_NO_LOOKUP, BasePandasDataset
from snowflake.snowpark.modin.pandas.utils import (
    from_pandas,
    is_scalar,
    try_convert_index_to_native,
)
from snowflake.snowpark.modin.plugin._typing import DropKeep, ListLike
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    series_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    MODIN_UNNAMED_SERIES_LABEL,
    _inherit_docstrings,
)

if TYPE_CHECKING:
    from snowflake.snowpark.modin.pandas.dataframe import DataFrame  # pragma: no cover

# add this line to enable doc tests to run
from snowflake.snowpark.modin import pandas as pd  # noqa: F401

logger = getLogger(__name__)

SERIES_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE_ERROR_MESSAGE = (
    "Currently do not support Series or list-like keys with range-like values"
)

SERIES_SETITEM_SLICE_AS_SCALAR_VALUE_ERROR_MESSAGE = (
    "Currently do not support assigning a slice value as if it's a scalar value"
)

SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SERIES_ERROR_MESSAGE = (
    "Snowpark pandas DataFrame cannot be used as an indexer with Series"
)

SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SCALAR_ERROR_MESSAGE = (
    "Scalar key incompatible with {} value"
)

# Dictionary of extensions assigned to this class
_SERIES_EXTENSIONS_ = {}


@_inherit_docstrings(
    pandas.Series,
    excluded=[
        pandas.Series.flags,
        pandas.Series.info,
        pandas.Series.prod,
        pandas.Series.product,
        pandas.Series.reindex,
        pandas.Series.fillna,
    ],
    apilink="pandas.Series",
)
class Series(BasePandasDataset):
    _pandas_class = pandas.Series
    __array_priority__ = pandas.Series.__array_priority__

    def __init__(
        self,
        data=None,
        index=None,
        dtype=None,
        name=None,
        copy=False,
        fastpath=False,
        query_compiler=None,
    ) -> None:
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        # Siblings are other dataframes that share the same query compiler. We
        # use this list to update inplace when there is a shallow copy.
        self._siblings = []

        # modified:
        # Engine.subscribe(_update_engine)

        if isinstance(data, type(self)):
            query_compiler = data._query_compiler.copy()
            if index is not None:
                if any(i not in data.index for i in index):
                    ErrorMessage.not_implemented(
                        "Passing non-existent columns or index values to constructor "
                        + "not yet implemented."
                    )  # pragma: no cover
                query_compiler = data.loc[index]._query_compiler
        if query_compiler is None:
            # Defaulting to pandas
            if name is None:
                name = MODIN_UNNAMED_SERIES_LABEL
                if (
                    isinstance(data, (pandas.Series, pandas.Index, pd.Index))
                    and data.name is not None
                ):
                    name = data.name

            query_compiler = from_pandas(
                pandas.DataFrame(
                    pandas.Series(
                        data=try_convert_index_to_native(data),
                        index=try_convert_index_to_native(index),
                        dtype=dtype,
                        name=name,
                        copy=copy,
                        fastpath=fastpath,
                    )
                )
            )._query_compiler
        self._query_compiler = query_compiler.columnarize()
        if name is not None:
            self.name = name

    def _get_name(self):
        """
        Get the value of the `name` property.

        Returns
        -------
        hashable
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        name = self._query_compiler.columns[0]
        if name == MODIN_UNNAMED_SERIES_LABEL:
            return None
        return name

    def _set_name(self, name):
        """
        Set the value of the `name` property.

        Parameters
        ----------
        name : hashable
            Name value to set.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if name is None:
            name = MODIN_UNNAMED_SERIES_LABEL
        if isinstance(name, tuple):
            columns = pd.MultiIndex.from_tuples(tuples=[name])
        else:
            columns = [name]
        self._update_inplace(
            new_query_compiler=self._query_compiler.set_columns(columns)
        )

    name = property(_get_name, _set_name)
    _parent = None
    # Parent axis denotes axis that was used to select series in a parent dataframe.
    # If _parent_axis == 0, then it means that index axis was used via df.loc[row]
    # indexing operations and assignments should be done to rows of parent.
    # If _parent_axis == 1 it means that column axis was used via df[column] and assignments
    # should be done to columns of parent.
    _parent_axis = 0

    def __add__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.add(right)

    def __radd__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.radd(left)

    def __and__(self, other):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__and__(other)

    def __rand__(self, other):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__rand__(other)

    # add `_inherit_docstrings` decorator to force method link addition.
    @_inherit_docstrings(pandas.Series.__array__, apilink="pandas.Series.__array__")
    def __array__(self, dtype=None):  # noqa: PR01, RT01, D200
        """
        Return the values as a NumPy array.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__array__(dtype).flatten()

    def __contains__(self, key):
        """
        Check if `key` in the `Series.index`.

        Parameters
        ----------
        key : hashable
            Key to check the presence in the index.

        Returns
        -------
        bool
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return key in self.index

    def __copy__(self, deep=True):
        """
        Return the copy of the Series.

        Parameters
        ----------
        deep : bool, default: True
            Whether the copy should be deep or not.

        Returns
        -------
        Series
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.copy(deep=deep)

    def __deepcopy__(self, memo=None):
        """
        Return the deep copy of the Series.

        Parameters
        ----------
        memo : Any, optional
           Deprecated parameter.

        Returns
        -------
        Series
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.copy(deep=True)

    def __delitem__(self, key):
        """
        Delete item identified by `key` label.

        Parameters
        ----------
        key : hashable
            Key to delete.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if key not in self.keys():
            raise KeyError(key)
        self.drop(labels=key, inplace=True)

    def __divmod__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.divmod(right)

    def __rdivmod__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.rdivmod(left)

    def __floordiv__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.floordiv(right)

    def __rfloordiv__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.rfloordiv(right)

    def __getattr__(self, key):
        """
        Return item identified by `key`.

        Parameters
        ----------
        key : hashable
            Key to get.

        Returns
        -------
        Any

        Notes
        -----
        First try to use `__getattribute__` method. If it fails
        try to get `key` from `Series` fields.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        try:
            return object.__getattribute__(self, key)
        except AttributeError as err:
            if key not in _ATTRS_NO_LOOKUP:
                try:
                    value = self[key]
                    if isinstance(value, Series) and value.empty:
                        raise err
                    return value
                except Exception:
                    # We want to raise err if self[key] raises any kind of exception
                    raise err
            raise err

    __float__ = _coerce_method(float)
    __int__ = _coerce_method(int)

    def abs(self):
        """
        Return a Series with absolute numeric value of each element.

        Returns
        -------
        Series

        Examples
        --------
        >>> ser = pd.Series([1, -2.29, 3, -4.77])
        >>> ser
        0    1.00
        1   -2.29
        2    3.00
        3   -4.77
        dtype: float64

        >>> abs(ser)
        0    1.00
        1    2.29
        2    3.00
        3    4.77
        dtype: float64
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().abs()

    def __neg__(self):
        """
        Returns a Series with the sign changed for each element.

        Returns
        -------
        Series

        Examples
        --------
        >>> ser = pd.Series([1, -2.29, 3, -4.77])
        >>> ser
        0    1.00
        1   -2.29
        2    3.00
        3   -4.77
        dtype: float64

        >>> - ser
        0   -1.00
        1    2.29
        2   -3.00
        3    4.77
        dtype: float64
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__neg__()

    def __iter__(self):
        """
        Return an iterator of the values.

        Returns
        -------
        iterable
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._to_pandas().__iter__()

    def __mod__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.mod(right)

    def __rmod__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.rmod(left)

    def __mul__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.mul(right)

    def __rmul__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.rmul(left)

    def __or__(self, other):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__or__(other)

    def __ror__(self, other):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__ror__(other)

    def __xor__(self, other):  # pragma: no cover
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__xor__(other)

    def __rxor__(self, other):  # pragma: no cover
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__rxor__(other)

    def __pow__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.pow(right)

    def __rpow__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.rpow(left)

    def __repr__(self):
        """
        Return a string representation for a particular Series.

        Returns
        -------
        str
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        num_rows = pandas.get_option("display.max_rows") or 60
        num_cols = pandas.get_option("display.max_columns") or 20

        (
            row_count,
            col_count,
            temp_df,
        ) = self._query_compiler.build_repr_df(num_rows, num_cols)
        if isinstance(temp_df, pandas.DataFrame) and not temp_df.empty:
            temp_df = temp_df.iloc[:, 0]
        temp_str = repr(temp_df)
        freq_str = (
            f"Freq: {temp_df.index.freqstr}, "
            if isinstance(temp_df.index, pandas.DatetimeIndex)
            else ""
        )
        if self.name is not None:
            name_str = f"Name: {str(self.name)}, "
        else:
            name_str = ""
        if row_count > num_rows:
            len_str = f"Length: {row_count}, "
        else:
            len_str = ""
        dtype_str = "dtype: {}".format(
            str(self.dtype) + ")"
            if temp_df.empty
            else temp_str.rsplit("dtype: ", 1)[-1]
        )
        if row_count == 0:
            return f"Series([], {freq_str}{name_str}{dtype_str}"
        maxsplit = 1
        if (
            isinstance(temp_df, pandas.Series)
            and temp_df.name is not None
            and temp_df.dtype == "category"
        ):
            maxsplit = 2
        return temp_str.rsplit("\n", maxsplit)[0] + "\n{}{}{}{}".format(
            freq_str, name_str, len_str, dtype_str
        )

    def __round__(self, decimals=0):
        """
        Round each value in a Series to the given number of decimals.

        Parameters
        ----------
        decimals : int, default: 0
            Number of decimal places to round to.

        Returns
        -------
        Series
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().round(decimals)

    def __setitem__(self, key, value):
        """
        Set `value` identified by `key` in the Series.

        Parameters
        ----------
        key : hashable
            Key to set.
        value : Any
            Value to set.

        Examples
        --------
        Using the following series to set values on. __setitem__ is an inplace operation, so copies of `series`are made
        in the examples to highlight the different behaviors produced.
        >>> series = pd.Series([1, "b", 3], index=["a", "b", "c"])

        Using a scalar as the value to set a particular element.
        >>> s = series.copy()
        >>> s["c"] = "a"
        >>> s
        a    1
        b    b
        c    a
        dtype: object

        Using list-like objects as the key and value to set multiple elements.
        >>> s = series.copy()
        >>> s[["c", "a"]] = ["foo", "bar"]
        >>> s  # doctest: +SKIP
        a    bar
        b      2
        c    foo
        dtype: object

        Having a duplicate label in the key.
        >>> s = series.copy()
        >>> s[["c", "a", "c"]] = pd.Index(["foo", "bar", "baz"])
        >>> s  # doctest: +SKIP
        a    bar
        b      2
        c    baz
        dtype: object

        When using a Series as the value, its index does not matter.
        >>> s = series.copy()  # doctest: +SKIP
        >>> s[["a", "b"]] = pd.Series([9, 8], index=["foo", "bar"])
        >>> s  # doctest: +SKIP
        a    9
        b    8
        c    3
        dtype: int64
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        key = apply_if_callable(key, self)

        # Error Checking:
        # Currently do not support Series[scalar key] = Series item/DataFrame item since this results in a nested series
        # or df.
        if is_scalar(key) and isinstance(value, BasePandasDataset):
            raise ValueError(
                SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SCALAR_ERROR_MESSAGE.format(
                    "Snowpark pandas " + value.__class__.__name__
                    if isinstance(value, BasePandasDataset)
                    else value.__class__.__name__
                )
            )
        if isinstance(key, pd.DataFrame):
            raise ValueError(
                SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SERIES_ERROR_MESSAGE
            )
        elif (isinstance(key, pd.Series) or is_list_like(key)) and (
            isinstance(value, range)
        ):
            raise NotImplementedError(
                SERIES_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE_ERROR_MESSAGE
            )
        elif isinstance(value, slice):
            # Here, the whole slice is assigned as a scalar variable, i.e., a spot at an index gets a slice value.
            raise NotImplementedError(
                SERIES_SETITEM_SLICE_AS_SCALAR_VALUE_ERROR_MESSAGE
            )

        if isinstance(key, (slice, range)):
            if (key.start is None or is_integer(key.start)) and (  # pragma: no cover
                key.stop is None or is_integer(key.stop)
            ):
                # integer slice behaves the same as iloc slice
                self.iloc[key] = value  # pragma: no cover
            else:
                # TODO: SNOW-976232 once the slice test is added to test_setitem, code here should be covered.
                self.loc[key] = value  # pragma: no cover

        elif isinstance(value, Series):
            # If value is a Series, value's index doesn't matter/is ignored. However, loc setitem matches the key's
            # index with value's index. To emulate this behavior, treat the Series as if it is matching by position.
            #
            # For example,
            # With __setitem__, the index of value does not matter.
            # >>> series = pd.Series([1, 2, 3], index=["a", "b", "c"])
            # >>> series[["a", "b"]] = pd.Series([9, 8])
            # a    9
            # b    8
            # c    3
            # dtype: int64
            # value = pd.Series([9, 8], index=["foo", "bar"]) also produces same result as above.
            #
            # However, with loc setitem, index matters.
            # >>> series.loc[["a", "b"]] = pd.Series([9, 8])
            # a    NaN
            # b    NaN
            # c    3.0
            # dtype: float64
            #
            # >>> series.loc[["a", "b"]] = pd.Series([9, 8], index=["a", "b"])
            # a    9
            # b    8
            # c    3
            # dtype: int64
            # Due to the behavior above, loc setitem can work with any kind of value regardless of length.
            # With __setitem__, the length of the value must match length of the key. Currently, loc setitem can
            # handle this with boolean keys.

            # Convert list-like keys to Series.
            if not isinstance(key, pd.Series) and is_list_like(key):
                key = pd.Series(key)

            index_is_bool_indexer = False

            if isinstance(key, pd.Series) and is_bool_dtype(key.dtype):
                index_is_bool_indexer = True  # pragma: no cover
            elif is_bool_indexer(key):
                index_is_bool_indexer = True  # pragma: no cover

            new_qc = self._query_compiler.set_2d_labels(
                key._query_compiler if isinstance(key, BasePandasDataset) else key,
                slice(None),  # column key is not applicable to Series objects
                value._query_compiler,
                matching_item_columns_by_label=False,
                matching_item_rows_by_label=False,
                index_is_bool_indexer=index_is_bool_indexer,
            )
            self._update_inplace(new_query_compiler=new_qc)

        else:
            self.loc[key] = value

    def __sub__(self, right):
        return self.sub(right)

    def __rsub__(self, left):
        return self.rsub(left)

    def __truediv__(self, right):
        return self.truediv(right)

    def __rtruediv__(self, left):
        return self.rtruediv(left)

    __iadd__ = __add__
    __imul__ = __add__
    __ipow__ = __pow__
    __isub__ = __sub__
    __itruediv__ = __truediv__

    def add(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return Addition of series and other, element-wise (binary operator add).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().add(other, level=level, fill_value=fill_value, axis=axis)

    def radd(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return Addition of series and other, element-wise (binary operator radd).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().radd(other, level=level, fill_value=fill_value, axis=axis)

    def add_prefix(self, prefix):  # noqa: PR01, RT01, D200
        """
        Prefix labels with string `prefix`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        # pandas converts non-string prefix values into str and adds it to the index labels.
        return self.__constructor__(
            query_compiler=self._query_compiler.add_substring(
                str(prefix), substring_type="prefix", axis=0
            )
        )

    def add_suffix(self, suffix):
        """
        Suffix labels with string `suffix`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        # pandas converts non-string suffix values into str and appends it to the index labels.
        return self.__constructor__(
            query_compiler=self._query_compiler.add_substring(
                str(suffix), substring_type="suffix", axis=0
            )
        )

    def drop(
        self,
        labels: IndexLabel = None,
        axis: Axis = 0,
        index: IndexLabel = None,
        columns: IndexLabel = None,
        level: Level | None = None,
        inplace: bool = False,
        errors: IgnoreRaise = "raise",
    ) -> Series | None:
        """
        Drop specified labels from `BasePandasDataset`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().drop(
            labels=labels,
            axis=axis,
            index=index,
            columns=columns,
            level=level,
            inplace=inplace,
            errors=errors,
        )

    def aggregate(
        self, func: AggFuncType = None, axis: Axis = 0, *args: Any, **kwargs: Any
    ):
        return super().aggregate(func, axis, *args, **kwargs)

    agg = aggregate

    def apply(
        self,
        func: AggFuncType,
        convert_dtype: bool = True,
        args: tuple[Any, ...] = (),
        **kwargs: Any,
    ):
        """
        Apply a function along an axis of the `BasePandasDataset`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        self._validate_function(func)
        new_query_compiler = self._query_compiler.apply_on_series(func, args, **kwargs)

        if convert_dtype:
            # TODO SNOW-810614: call convert_dtypes for consistency
            WarningMessage.ignored_argument(
                operation="apply",
                argument="convert_dtype",
                message="convert_dtype is ignored in Snowflake backend",
            )

        return self.__constructor__(query_compiler=new_query_compiler)

    @series_not_implemented()
    def argmax(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return int position of the largest value in the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        result = self.idxmax(axis=axis, skipna=skipna, *args, **kwargs)
        if np.isnan(result) or result is pandas.NA:
            result = -1
        return result

    @series_not_implemented()
    def argmin(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return int position of the smallest value in the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        result = self.idxmin(axis=axis, skipna=skipna, *args, **kwargs)
        if np.isnan(result) or result is pandas.NA:
            result = -1
        return result

    @series_not_implemented()
    def argsort(self, axis=0, kind="quicksort", order=None):  # noqa: PR01, RT01, D200
        """
        Return the integer indices that would sort the Series values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas(
            pandas.Series.argsort, axis=axis, kind=kind, order=order
        )

    @series_not_implemented()
    def autocorr(self, lag=1):  # noqa: PR01, RT01, D200
        """
        Compute the lag-N autocorrelation.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.corr(self.shift(lag))

    @series_not_implemented()
    def between(self, left, right, inclusive: str = "both"):  # noqa: PR01, RT01, D200
        """
        Return boolean Series equivalent to left <= series <= right.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas(
            pandas.Series.between, left, right, inclusive=inclusive
        )

    @series_not_implemented()
    def compare(
        self,
        other: Series,
        align_axis: str | int = 1,
        keep_shape: bool = False,
        keep_equal: bool = False,
        result_names: tuple = ("self", "other"),
    ) -> Series:  # noqa: PR01, RT01, D200
        """
        Compare to another Series and show the differences.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if not isinstance(other, Series):
            raise TypeError(f"Cannot compare Series to {type(other)}")
        result = self.to_frame().compare(
            other.to_frame(),
            align_axis=align_axis,
            keep_shape=keep_shape,
            keep_equal=keep_equal,
            result_names=result_names,
        )
        if align_axis == "columns" or align_axis == 1:
            # pandas.DataFrame.Compare returns a dataframe with a multidimensional index object as the
            # columns so we have to change column object back.
            result.columns = pandas.Index(["self", "other"])
        else:
            result = result.squeeze().rename(None)
        return result

    @series_not_implemented()
    def corr(self, other, method="pearson", min_periods=None):  # noqa: PR01, RT01, D200
        """
        Compute correlation with `other` Series, excluding missing values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if method == "pearson":
            this, other = self.align(other, join="inner", copy=False)
            this = self.__constructor__(this)
            other = self.__constructor__(other)

            if len(this) == 0:
                return np.nan
            if len(this) != len(other):
                raise ValueError("Operands must have same size")

            if min_periods is None:
                min_periods = 1

            valid = this.notna() & other.notna()
            if not valid.all():
                this = this[valid]
                other = other[valid]
            if len(this) < min_periods:
                return np.nan

            this = this.astype(dtype="float64")
            other = other.astype(dtype="float64")
            this -= this.mean()
            other -= other.mean()

            other = other.__constructor__(query_compiler=other._query_compiler.conj())
            result = this * other / (len(this) - 1)
            result = np.array([result.sum()])

            stddev_this = ((this * this) / (len(this) - 1)).sum()
            stddev_other = ((other * other) / (len(other) - 1)).sum()

            stddev_this = np.array([np.sqrt(stddev_this)])
            stddev_other = np.array([np.sqrt(stddev_other)])

            result /= stddev_this * stddev_other

            np.clip(result.real, -1, 1, out=result.real)
            if np.iscomplexobj(result):
                np.clip(result.imag, -1, 1, out=result.imag)
            return result[0]

        return self.__constructor__(
            query_compiler=self._query_compiler.default_to_pandas(
                pandas.Series.corr,
                other._query_compiler,
                method=method,
                min_periods=min_periods,
            )
        )

    def count(self):
        return super().count()

    @series_not_implemented()
    def cov(
        self, other, min_periods=None, ddof: int | None = 1
    ):  # noqa: PR01, RT01, D200
        """b
        Compute covariance with Series, excluding missing values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        this, other = self.align(other, join="inner", copy=False)
        this = self.__constructor__(this)
        other = self.__constructor__(other)
        if len(this) == 0:
            return np.nan

        if len(this) != len(other):
            raise ValueError("Operands must have same size")

        if min_periods is None:
            min_periods = 1

        valid = this.notna() & other.notna()
        if not valid.all():
            this = this[valid]
            other = other[valid]

        if len(this) < min_periods:
            return np.nan

        this = this.astype(dtype="float64")
        other = other.astype(dtype="float64")

        this -= this.mean()
        other -= other.mean()

        other = other.__constructor__(query_compiler=other._query_compiler.conj())
        result = this * other / (len(this) - ddof)
        result = result.sum()
        return result

    def describe(
        self,
        percentiles: ListLike | None = None,
        include: ListLike | Literal["all"] | None = None,
        exclude: ListLike | None = None,
    ) -> Series:
        """
        Generate descriptive statistics.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().describe(
            percentiles=percentiles,
            include=None,
            exclude=None,
        )

    def diff(self, periods: int = 1):
        """
        First discrete difference of element.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().diff(periods=periods, axis=0)

    @series_not_implemented()
    def divmod(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return Integer division and modulo of series and `other`, element-wise (binary operator `divmod`).
        Not implemented
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions

    @series_not_implemented()
    def dot(self, other):  # noqa: PR01, RT01, D200
        """
        Compute the dot product between the Series and the columns of `other`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if isinstance(other, BasePandasDataset):
            common = self.index.union(other.index)
            if len(common) > len(self) or len(common) > len(other):  # pragma: no cover
                raise ValueError("Matrices are not aligned")

            if isinstance(other, Series):
                return self._reduce_dimension(
                    query_compiler=self._query_compiler.dot(
                        other.reindex(index=common), squeeze_self=True
                    )
                )
            else:
                return self.__constructor__(
                    query_compiler=self._query_compiler.dot(
                        other.reindex(index=common), squeeze_self=True
                    )
                )

        other = np.asarray(other)
        if self.shape[0] != other.shape[0]:
            raise ValueError(
                f"Dot product shape mismatch, {self.shape} vs {other.shape}"
            )

        if len(other.shape) > 1:
            return (
                self._query_compiler.dot(other, squeeze_self=True).to_numpy().squeeze()
            )

        return self._reduce_dimension(
            query_compiler=self._query_compiler.dot(other, squeeze_self=True)
        )

    def drop_duplicates(self, keep="first", inplace=False):  # noqa: PR01, RT01, D200
        """
        Return Series with duplicate values removed.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().drop_duplicates(keep=keep, inplace=inplace)

    def dropna(
        self,
        *,
        axis: Axis = 0,
        inplace: bool = False,
        how: str | NoDefault = no_default,
    ):
        """
        Return a new Series with missing values removed.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super()._dropna(axis=axis, inplace=inplace, how=how)

    def duplicated(self, keep: DropKeep = "first"):
        """
        Indicate duplicate Series values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        name = self.name
        series = self.to_frame().duplicated(keep=keep)
        # we are using df.duplicated method for series but its result will lose the series name, so we preserve it here
        series.name = name
        return series

    def eq(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return Equal to of series and `other`, element-wise (binary operator `eq`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().eq(other, level=level, axis=axis)

    def equals(self, other) -> bool:  # noqa: PR01, RT01, D200
        """
        Test whether two objects contain the same elements.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if isinstance(other, pandas.Series):
            # Copy into a Modin Series to simplify logic below
            other = self.__constructor__(other)

        if type(self) is not type(other) or not self.index.equals(other.index):
            return False

        old_name_self = self.name
        old_name_other = other.name
        try:
            self.name = "temp_name_for_equals_op"
            other.name = "temp_name_for_equals_op"
            # this function should return only scalar
            res = self.__constructor__(
                query_compiler=self._query_compiler.equals(other._query_compiler)
            )
        finally:
            self.name = old_name_self
            other.name = old_name_other
        return res.all()

    @series_not_implemented()
    def explode(self, ignore_index: bool = False):  # noqa: PR01, RT01, D200
        """
        Transform each element of a list-like to a row.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().explode(
            MODIN_UNNAMED_SERIES_LABEL if self.name is None else self.name,
            ignore_index=ignore_index,
        )

    @series_not_implemented()
    def factorize(
        self, sort=False, na_sentinel=no_default, use_na_sentinel=no_default
    ):  # noqa: PR01, RT01, D200
        """
        Encode the object as an enumerated type or categorical variable.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas(
            pandas.Series.factorize,
            sort=sort,
            na_sentinel=na_sentinel,
            use_na_sentinel=use_na_sentinel,
        )

    def case_when(self, caselist) -> Series:  # noqa: PR01, RT01, D200
        """
        Replace values where the conditions are True.
        """
        modin_type = type(self)
        caselist = [
            tuple(
                data._query_compiler if isinstance(data, modin_type) else data
                for data in case_tuple
            )
            for case_tuple in caselist
        ]
        return self.__constructor__(
            query_compiler=self._query_compiler.case_when(caselist=caselist)
        )

    def fillna(
        self,
        value: Hashable | Mapping | Series = None,
        *,
        method: FillnaOptions | None = None,
        axis: Axis | None = None,
        inplace: bool = False,
        limit: int | None = None,
        downcast: dict | None = None,
    ) -> Series | None:
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if isinstance(value, BasePandasDataset) and not isinstance(value, Series):
            raise TypeError(
                '"value" parameter must be a scalar, dict or Series, but '
                + f'you passed a "{type(value).__name__}"'
            )
        return super().fillna(
            self_is_series=True,
            value=value,
            method=method,
            axis=axis,
            inplace=inplace,
            limit=limit,
            downcast=downcast,
        )

    def floordiv(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Get Integer division of dataframe and `other`, element-wise (binary operator `floordiv`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().floordiv(other, level=level, fill_value=fill_value, axis=axis)

    def ge(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return greater than or equal to of series and `other`, element-wise (binary operator `ge`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().ge(other, level=level, axis=axis)

    def groupby(
        self,
        by=None,
        axis: Axis = 0,
        level: IndexLabel | None = None,
        as_index: bool = True,
        sort: bool = True,
        group_keys: bool = True,
        observed: bool | NoDefault = no_default,
        dropna: bool = True,
    ):
        """
        Group Series using a mapper or by a Series of columns.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        from snowflake.snowpark.modin.pandas.groupby import (
            SeriesGroupBy,
            validate_groupby_args,
        )

        validate_groupby_args(by, level, observed)

        if not as_index:
            raise TypeError("as_index=False only valid with DataFrame")

        axis = self._get_axis_number(axis)
        return SeriesGroupBy(
            self,
            by,
            axis,
            level,
            as_index,
            sort,
            group_keys,
            idx_name=None,
            observed=observed,
            dropna=dropna,
        )

    def gt(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return greater than of series and `other`, element-wise (binary operator `gt`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().gt(other, level=level, axis=axis)

    @series_not_implemented()
    def hist(
        self,
        by=None,
        ax=None,
        grid=True,
        xlabelsize=None,
        xrot=None,
        ylabelsize=None,
        yrot=None,
        figsize=None,
        bins=10,
        **kwds,
    ):  # noqa: PR01, RT01, D200
        """
        Draw histogram of the input series using matplotlib.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas(
            pandas.Series.hist,
            by=by,
            ax=ax,
            grid=grid,
            xlabelsize=xlabelsize,
            xrot=xrot,
            ylabelsize=ylabelsize,
            yrot=yrot,
            figsize=figsize,
            bins=bins,
            **kwds,
        )

    def idxmax(self, axis=0, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return the row label of the maximum value.

        Parameters
        ----------
        axis : {0 or 'index'}
            Unused. Parameter needed for compatibility with DataFrame.
        skipna : bool, default True
            Exclude NA/null values. If an entire Series is NA, the result will be NA.
        *args, **kwargs
            Additional arguments and keywords have no effect but might be accepted for compatibility with NumPy.

        Returns
        -------
        Index, the label of the maximum value.

        Examples
        --------
        >>> s = pd.Series(data=[1, None, 4, 3, 4],
        ...               index=['A', 'B', 'C', 'D', 'E'])
        >>> s.idxmax()
        'C'
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if skipna is None:
            skipna = True
        return super().idxmax(axis=axis, skipna=skipna, *args, **kwargs)

    def idxmin(self, axis=0, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return the row label of the minimum value.

        Parameters
        ----------
        axis : {0 or 'index'}
            Unused. Parameter needed for compatibility with DataFrame.
        skipna : bool, default True
            Exclude NA/null values. If an entire Series is NA, the result will be NA.
        *args, **kwargs
            Additional arguments and keywords have no effect but might be accepted for compatibility with NumPy.

        Returns
        -------
        Index, the label of the minimum value.

        Examples
        --------
        >>> s = pd.Series(data=[1, None, 4, 3, 4],
        ...               index=['A', 'B', 'C', 'D', 'E'])
        >>> s.idxmin()
        'A'
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if skipna is None:
            skipna = True
        return super().idxmin(axis=axis, skipna=skipna, *args, **kwargs)

    def info(
        self,
        verbose: bool | None = None,
        buf: IO[str] | None = None,
        max_cols: int | None = None,
        memory_usage: bool | str | None = None,
        show_counts: bool = True,
    ):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas(
            pandas.Series.info,
            verbose=verbose,
            buf=buf,
            max_cols=max_cols,
            memory_usage=memory_usage,
            show_counts=show_counts,
        )

    @series_not_implemented()
    def interpolate(
        self,
        method="linear",
        axis=0,
        limit=None,
        inplace=False,
        limit_direction: str | None = None,
        limit_area=None,
        downcast=None,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Fill NaN values using an interpolation method.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas(
            pandas.Series.interpolate,
            method=method,
            axis=axis,
            limit=limit,
            inplace=inplace,
            limit_direction=limit_direction,
            limit_area=limit_area,
            downcast=downcast,
            **kwargs,
        )

    @series_not_implemented()
    def item(self):  # noqa: RT01, D200
        """
        Return the first element of the underlying data as a Python scalar.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self[0]

    @series_not_implemented()
    def items(self):  # noqa: D200
        """
        Lazily iterate over (index, value) tuples.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        def item_builder(s):
            return s.name, s.squeeze()

        partition_iterator = PartitionIterator(self.to_frame(), 0, item_builder)
        yield from partition_iterator

    def keys(self):  # noqa: RT01, D200
        """
        Return alias for index.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.index

    def kurt(
        self,
        axis: Axis | None | NoDefault = no_default,
        skipna=True,
        numeric_only=False,
        **kwargs,
    ):  # noqa: PR01, RT01, D200
        """
        Return unbiased kurtosis over requested axis.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        axis = self._get_axis_number(axis)
        return super().kurt(axis, skipna, numeric_only, **kwargs)

    kurtosis = kurt

    def le(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return less than or equal to of series and `other`, element-wise (binary operator `le`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().le(other, level=level, axis=axis)

    def lt(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return less than of series and `other`, element-wise (binary operator `lt`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().lt(other, level=level, axis=axis)

    def map(
        self,
        arg: Callable | Mapping | Series,
        na_action: Literal["ignore"] | None = None,
    ) -> Series:
        """
        Map values of Series according to input correspondence.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.__constructor__(
            query_compiler=self._query_compiler.map(arg, na_action)
        )

    def mask(
        self,
        cond: DataFrame | Series | Callable | AnyArrayLike,
        other: DataFrame | Series | Callable | Scalar | None = np.nan,
        inplace: bool = False,
        axis: Axis | None = None,
        level: Level | None = None,
    ):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().mask(
            cond,
            other=other,
            inplace=inplace,
            axis=axis,
            level=level,
        )

    @series_not_implemented()
    def memory_usage(self, index=True, deep=False):  # noqa: PR01, RT01, D200
        """
        Return the memory usage of the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if index:
            result = self._reduce_dimension(
                self._query_compiler.memory_usage(index=False, deep=deep)
            )
            index_value = self.index.memory_usage(deep=deep)
            return result + index_value
        return super().memory_usage(index=index, deep=deep)

    def mod(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return Modulo of series and `other`, element-wise (binary operator `mod`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().mod(other, level=level, fill_value=fill_value, axis=axis)

    @series_not_implemented()
    def mode(self, dropna=True):  # noqa: PR01, RT01, D200
        """
        Return the mode(s) of the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().mode(numeric_only=False, dropna=dropna)

    def mul(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return multiplication of series and `other`, element-wise (binary operator `mul`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().mul(other, level=level, fill_value=fill_value, axis=axis)

    multiply = mul

    def rmul(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return multiplication of series and `other`, element-wise (binary operator `mul`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rmul(other, level=level, fill_value=fill_value, axis=axis)

    def ne(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return not equal to of series and `other`, element-wise (binary operator `ne`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().ne(other, level=level, axis=axis)

    def nlargest(self, n=5, keep="first"):  # noqa: PR01, RT01, D200
        """
        Return the largest `n` elements.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if len(self._query_compiler.columns) == 0:
            # pandas returns empty series when requested largest/smallest from empty series
            return self.__constructor__(data=[], dtype=float)
        return Series(
            query_compiler=self._query_compiler.nlargest(
                n=n, columns=self.name, keep=keep
            )
        )

    def nsmallest(self, n=5, keep="first"):  # noqa: PR01, RT01, D200
        """
        Return the smallest `n` elements.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if len(self._query_compiler.columns) == 0:
            # pandas returns empty series when requested largest/smallest from empty series
            return self.__constructor__(data=[], dtype=float)
        return self.__constructor__(
            query_compiler=self._query_compiler.nsmallest(
                n=n, columns=self.name, keep=keep
            )
        )

    def set_axis(
        self,
        labels: IndexLabel,
        *,
        axis: Axis = 0,
        copy: bool | NoDefault = no_default,  # ignored
    ):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if not is_scalar(axis):
            raise TypeError(f"{type(axis).__name__} is not a valid type for axis.")
        return super().set_axis(
            labels=labels,
            # 'rows', 'index, and 0 are valid axis values for Series.
            # 'columns' and 1 are valid axis values only for DataFrame.
            axis=pandas.Series._get_axis_name(axis),
            copy=copy,
        )

    @series_not_implemented()
    def unstack(self, level=-1, fill_value=None):  # noqa: PR01, RT01, D200
        """
        Unstack, also known as pivot, Series with MultiIndex to produce DataFrame.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        from snowflake.snowpark.modin.pandas.dataframe import DataFrame

        result = DataFrame(
            query_compiler=self._query_compiler.unstack(level, fill_value)
        )

        return result.droplevel(0, axis=1) if result.columns.nlevels > 1 else result

    @series_not_implemented()
    @property
    def plot(
        self,
        kind="line",
        ax=None,
        figsize=None,
        use_index=True,
        title=None,
        grid=None,
        legend=False,
        style=None,
        logx=False,
        logy=False,
        loglog=False,
        xticks=None,
        yticks=None,
        xlim=None,
        ylim=None,
        rot=None,
        fontsize=None,
        colormap=None,
        table=False,
        yerr=None,
        xerr=None,
        label=None,
        secondary_y=False,
        **kwds,
    ):  # noqa: PR01, RT01, D200
        """
        Make plot of Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._to_pandas().plot

    def pow(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return exponential power of series and `other`, element-wise (binary operator `pow`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().pow(other, level=level, fill_value=fill_value, axis=axis)

    @series_not_implemented()
    def prod(
        self,
        axis=None,
        skipna=True,
        level=None,
        numeric_only=False,
        min_count=0,
        **kwargs,
    ):
        validate_bool_kwarg(skipna, "skipna", none_allowed=False)
        axis = self._get_axis_number(axis)
        if level is not None:
            if (
                not self._query_compiler.has_multiindex(axis=axis)
                and level > 0
                or level < -1
                and level != self.index.name
            ):
                raise ValueError("level > 0 or level < -1 only valid with MultiIndex")
            return self.groupby(level=level, axis=axis, sort=False).prod(
                numeric_only=numeric_only, min_count=min_count, **kwargs
            )
        new_index = self.columns if axis else self.index
        if min_count > len(new_index):
            return np.nan

        data = self._validate_dtypes_sum_prod_mean(axis, numeric_only, ignore_axis=True)
        if min_count > 1:
            return data._reduce_dimension(
                data._query_compiler.prod_min_count(
                    axis=axis,
                    skipna=skipna,
                    level=level,
                    numeric_only=numeric_only,
                    min_count=min_count,
                    **kwargs,
                )
            )
        return data._reduce_dimension(
            data._query_compiler.prod(
                axis=axis,
                skipna=skipna,
                level=level,
                numeric_only=numeric_only,
                min_count=min_count,
                **kwargs,
            )
        )

    product = prod

    @series_not_implemented()
    def ravel(self, order="C"):  # noqa: PR01, RT01, D200
        """
        Return the flattened underlying data as an ndarray.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        data = self._query_compiler.to_numpy().flatten(order=order)
        if isinstance(self.dtype, pandas.CategoricalDtype):
            data = pandas.Categorical(data, dtype=self.dtype)

        return data

    def reindex(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError(
                    "Series.reindex() takes from 1 to 2 positional arguments but 3 were given"
                )
            if "index" in kwargs:
                raise TypeError(
                    "Series.reindex() got multiple values for argument 'index'"
                )
            kwargs.update({"index": args[0]})
        index = kwargs.pop("index", None)
        method = kwargs.pop("method", None)
        level = kwargs.pop("level", None)
        copy = kwargs.pop("copy", True)
        limit = kwargs.pop("limit", None)
        tolerance = kwargs.pop("tolerance", None)
        fill_value = kwargs.pop("fill_value", None)
        kwargs.pop("axis", None)
        if kwargs:
            raise TypeError(
                "Series.reindex() got an unexpected keyword "
                + f"argument '{list(kwargs.keys())[0]}'"
            )
        return super().reindex(
            index=index,
            columns=None,
            method=method,
            level=level,
            copy=copy,
            limit=limit,
            tolerance=tolerance,
            fill_value=fill_value,
        )

    def rename_axis(
        self,
        mapper=no_default,
        *,
        index=no_default,
        axis=0,
        copy=True,
        inplace=False,
    ) -> Series | None:  # noqa: PR01, RT01, D200
        """
        Set the name of the axis for the index or columns.
        """
        return super().rename_axis(
            mapper=mapper, index=index, axis=axis, copy=copy, inplace=inplace
        )

    def rename(
        self,
        index: Renamer | Hashable | None = None,
        *,
        axis: Axis | None = None,
        copy: bool | None = None,
        inplace: bool = False,
        level: Level | None = None,
        errors: IgnoreRaise = "ignore",
    ) -> Series | None:
        """
        Alter Series index labels or name.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if axis is not None:
            # make sure we raise if an invalid 'axis' is passed.
            # note: axis is unused. It's needed for compatibility with DataFrame.
            self._get_axis_number(axis)

        if copy is not None:
            WarningMessage.ignored_argument(
                operation="series.rename",
                argument="copy",
                message="copy parameter has been ignored with Snowflake execution engine",
            )

        if callable(index) or is_dict_like(index):
            if isinstance(index, dict):
                index = Series(index)
            new_qc = self._query_compiler.rename(
                index_renamer=index, level=level, errors=errors
            )
            new_series = self._create_or_update_from_compiler(
                new_query_compiler=new_qc, inplace=inplace
            )
            if not inplace and hasattr(self, "name"):
                new_series.name = self.name
            return new_series
        else:
            # just change Series.name
            if inplace:
                self.name = index
            else:
                self_cp = self.copy()
                self_cp.name = index
                return self_cp

    @series_not_implemented()
    def repeat(self, repeats, axis=None):  # noqa: PR01, RT01, D200
        """
        Repeat elements of a Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if (isinstance(repeats, int) and repeats == 0) or (
            is_list_like(repeats) and len(repeats) == 1 and repeats[0] == 0
        ):
            return self.__constructor__()

        return self.__constructor__(query_compiler=self._query_compiler.repeat(repeats))

    def reset_index(
        self,
        level=None,
        drop=False,
        name=no_default,
        inplace=False,
        allow_duplicates=False,
    ):
        """
        Generate a new Series with the index reset.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if name is no_default:
            # For backwards compatibility, keep columns as [0] instead of
            #  [None] when self.name is None
            name = 0 if self.name is None else self.name

        if not drop and inplace:
            raise TypeError(
                "Cannot reset_index inplace on a Series to create a DataFrame"
            )
        else:
            obj = self.copy()
            obj.name = name
            new_query_compiler = obj._query_compiler.reset_index(
                drop=drop,
                level=level,
                col_level=0,
                col_fill="",
                allow_duplicates=allow_duplicates,
                names=None,
            )
            return self._create_or_update_from_compiler(new_query_compiler, inplace)

    @series_not_implemented()
    def rdivmod(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return integer division and modulo of series and `other`, element-wise (binary operator `rdivmod`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions

    def rfloordiv(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return integer division of series and `other`, element-wise (binary operator `rfloordiv`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rfloordiv(other, level=level, fill_value=fill_value, axis=axis)

    def rmod(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return modulo of series and `other`, element-wise (binary operator `rmod`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rmod(other, level=level, fill_value=fill_value, axis=axis)

    def round(self, decimals=0, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Round each value in a Series to the given number of decimals.

        Parameters
        ----------
        decimals : int, default 0
            Number of decimal places to round to. If decimals is negative, it specifies the number of positions to the left of the decimal point.
        *args, **kwargs
            Additional arguments and keywords have no effect but might be accepted for compatibility with NumPy.

        Returns
        -------
        Series
            Rounded values of the Series.

        See Also
        --------
            numpy.around : Round values of an np.array.
            DataFrame.round : Round values of a DataFrame.

        Examples
        --------
        >>> s = pd.Series([0.1, 1.3, 2.7])
        >>> s.round()
        0    0.0
        1    1.0
        2    3.0
        dtype: float64
        """
        return super().round(decimals, args=args, **kwargs)

    def rpow(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return exponential power of series and `other`, element-wise (binary operator `rpow`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rpow(other, level=level, fill_value=fill_value, axis=axis)

    def rsub(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return subtraction of series and `other`, element-wise (binary operator `rsub`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rsub(other, level=level, fill_value=fill_value, axis=axis)

    def rtruediv(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return floating division of series and `other`, element-wise (binary operator `rtruediv`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rtruediv(other, level=level, fill_value=fill_value, axis=axis)

    rdiv = rtruediv

    def quantile(
        self,
        q: Scalar | ListLike = 0.5,
        interpolation: Literal[
            "linear", "lower", "higher", "midpoint", "nearest"
        ] = "linear",
    ):
        """
        Return value at the given quantile.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().quantile(
            q=q,
            axis=0,
            numeric_only=False,
            interpolation=interpolation,
            method="single",
        )

    @series_not_implemented()
    def reorder_levels(self, order):  # noqa: PR01, RT01, D200
        """
        Rearrange index levels using input order.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().reorder_levels(order)

    def replace(
        self,
        to_replace=None,
        value=no_default,
        inplace=False,
        limit=None,
        regex=False,
        method: str | NoDefault = no_default,
    ):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        inplace = validate_bool_kwarg(inplace, "inplace")
        # The following errors cannot be raised by query compiler because we don't know
        # if frontend object is Series or DataFrame.
        if to_replace is not None and is_dict_like(value):
            raise ValueError(
                "In Series.replace 'to_replace' must be None if the 'value' is dict-like"
            )
        if is_dict_like(to_replace) and value != no_default:
            raise ValueError(
                "In Series.replace 'to_replace' cannot be dict-like if 'value' is provided"
            )
        new_query_compiler = self._query_compiler.replace(
            to_replace=to_replace,
            value=value,
            limit=limit,
            regex=regex,
            method=method,
        )
        return self._create_or_update_from_compiler(new_query_compiler, inplace)

    @series_not_implemented()
    def searchsorted(self, value, side="left", sorter=None):  # noqa: PR01, RT01, D200
        """
        Find indices where elements should be inserted to maintain order.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        searchsorted_qc = self._query_compiler
        if sorter is not None:
            # `iloc` method works slowly (https://github.com/modin-project/modin/issues/1903),
            # so _default_to_pandas is used for now
            # searchsorted_qc = self.iloc[sorter].reset_index(drop=True)._query_compiler
            # sorter = None
            return self._default_to_pandas(
                pandas.Series.searchsorted, value, side=side, sorter=sorter
            )
        # searchsorted should return item number irrespective of Series index, so
        # Series.index is always set to pandas.RangeIndex, which can be easily processed
        # on the query_compiler level
        if not isinstance(searchsorted_qc.index, pandas.RangeIndex):
            searchsorted_qc = searchsorted_qc.reset_index(drop=True)

        result = self.__constructor__(
            query_compiler=searchsorted_qc.searchsorted(
                value=value, side=side, sorter=sorter
            )
        ).squeeze()

        # matching pandas output
        if not is_scalar(value) and not is_list_like(result):
            result = np.array([result])
        elif isinstance(result, type(self)):
            result = result.to_numpy()

        return result

    def sort_values(
        self,
        axis: Axis = 0,
        ascending: bool | int | Sequence[bool] | Sequence[int] = True,
        inplace: bool = False,
        kind: str = "quicksort",
        na_position: str = "last",
        ignore_index: bool = False,
        key: IndexKeyFunc | None = None,
    ):
        """
        Sort by the values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        from snowflake.snowpark.modin.pandas.dataframe import DataFrame

        if is_list_like(ascending) and len(ascending) != 1:
            raise ValueError(
                f"Length of ascending ({len(ascending)}) must be 1 for Series"
            )

        if axis is not None:
            # Validate `axis`
            self._get_axis_number(axis)

        # When we convert to a DataFrame, the name is automatically converted to 0 if it
        # is None, so we do this to avoid a KeyError.
        by = self.name if self.name is not None else 0
        result = (
            DataFrame(self.copy())
            .sort_values(
                by=by,
                ascending=ascending,
                inplace=False,
                kind=kind,
                na_position=na_position,
                ignore_index=ignore_index,
                key=key,
            )
            .squeeze(axis=1)
        )
        result.name = self.name
        return self._create_or_update_from_compiler(
            result._query_compiler, inplace=inplace
        )

    sparse = CachedAccessor("sparse", SparseAccessor)

    def squeeze(self, axis: Axis | None = None):
        """
        Squeeze 1 dimensional axis objects into scalars.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if axis is not None:
            # Validate `axis`
            pandas.Series._get_axis_number(axis)
        if len(self) == 1:
            return self._reduce_dimension(self._query_compiler)
        else:
            return self.copy()

    def sub(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return subtraction of Series and `other`, element-wise (binary operator `sub`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().sub(other, level=level, fill_value=fill_value, axis=axis)

    subtract = sub

    @series_not_implemented()
    def swaplevel(self, i=-2, j=-1, copy=True):  # noqa: PR01, RT01, D200
        """
        Swap levels `i` and `j` in a `MultiIndex`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas("swaplevel", i=i, j=j, copy=copy)

    def take(
        self,
        indices: list | AnyArrayLike,
        axis: Axis = 0,
        **kwargs,
    ):
        """
        Return the elements in the given positional indices along an axis.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().take(indices, axis=0, **kwargs)

    def to_dict(self, into: type[dict] = dict) -> dict:
        """
        Convert Series to {label -> value} dict or dict-like object.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._to_pandas().to_dict(into=into)

    def to_frame(
        self, name: Hashable = no_default
    ) -> DataFrame:  # noqa: PR01, RT01, D200
        """
        Convert Series to {label -> value} dict or dict-like object.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        from snowflake.snowpark.modin.pandas.dataframe import DataFrame

        if name is None:
            name = no_default

        self_cp = self.copy()
        if name is not no_default:
            self_cp.name = name

        return DataFrame(self_cp)

    def to_list(self) -> list:
        """
        Return a list of the values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.values.tolist()

    def to_numpy(
        self,
        dtype: npt.DTypeLike | None = None,
        copy: bool = False,
        na_value: object = no_default,
        **kwargs: Any,
    ) -> np.ndarray:
        """
        Return the NumPy ndarray representing the values in this Series or Index.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return (
            super()
            .to_numpy(
                dtype=dtype,
                copy=copy,
                na_value=na_value,
                **kwargs,
            )
            .flatten()
        )

    tolist = to_list

    # TODO(williamma12): When we implement to_timestamp, have this call the version
    # in base.py
    @series_not_implemented()
    def to_period(self, freq=None, copy=True):  # noqa: PR01, RT01, D200
        """
        Cast to PeriodArray/Index at a particular frequency.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas("to_period", freq=freq, copy=copy)

    @series_not_implemented()
    def to_string(
        self,
        buf=None,
        na_rep="NaN",
        float_format=None,
        header=True,
        index=True,
        length=False,
        dtype=False,
        name=False,
        max_rows=None,
        min_rows=None,
    ):  # noqa: PR01, RT01, D200
        """
        Render a string representation of the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas(
            pandas.Series.to_string,
            buf=buf,
            na_rep=na_rep,
            float_format=float_format,
            header=header,
            index=index,
            length=length,
            dtype=dtype,
            name=name,
            max_rows=max_rows,
        )

    # TODO(williamma12): When we implement to_timestamp, have this call the version
    # in base.py
    @series_not_implemented()
    def to_timestamp(self, freq=None, how="start", copy=True):  # noqa: PR01, RT01, D200
        """
        Cast to DatetimeIndex of Timestamps, at beginning of period.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas("to_timestamp", freq=freq, how=how, copy=copy)

    def transpose(self, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return the transpose, which is by definition `self`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self

    T = property(transpose)

    def truediv(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return floating division of series and `other`, element-wise (binary operator `truediv`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().truediv(other, level=level, fill_value=fill_value, axis=axis)

    div = divide = truediv

    @series_not_implemented()
    def truncate(
        self, before=None, after=None, axis=None, copy=True
    ):  # noqa: PR01, RT01, D200
        """
        Truncate a Series before and after some index value.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._default_to_pandas(
            pandas.Series.truncate, before=before, after=after, axis=axis, copy=copy
        )

    def unique(self):
        """
        Return unique values of Series object.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.__constructor__(
            query_compiler=self._query_compiler.unique()
        ).to_numpy()

    def update(self, other):  # noqa: PR01, D200
        """
        Modify Series in place using values from passed Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if not isinstance(other, Series):
            other = self.__constructor__(other)
        query_compiler = self._query_compiler.series_update(other)
        self._update_inplace(new_query_compiler=query_compiler)

    def value_counts(
        self,
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        bins: int | None = None,
        dropna: bool = True,
    ):
        """
        Return a Series containing counts of unique values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.__constructor__(
            query_compiler=self._query_compiler.value_counts(
                subset=None,
                normalize=normalize,
                sort=sort,
                ascending=ascending,
                bins=bins,
                dropna=dropna,
            ).set_index_names([self.name]),
            name="proportion" if normalize else "count",
        )

    @series_not_implemented()
    def view(self, dtype=None):  # noqa: PR01, RT01, D200
        """
        Create a new view of the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.__constructor__(
            query_compiler=self._query_compiler.series_view(dtype=dtype)
        )

    def where(
        self,
        cond: DataFrame | Series | Callable | AnyArrayLike,
        other: DataFrame | Series | Callable | Scalar | None = np.nan,
        inplace: bool = False,
        axis: Axis | None = None,
        level: Level | None = None,
    ):
        """
        Replace values where the condition is False.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().where(
            cond,
            other=other,
            inplace=inplace,
            axis=axis,
            level=level,
        )

    @series_not_implemented()
    def xs(
        self, key, axis=0, level=None, drop_level=True
    ):  # pragma: no cover # noqa: PR01, D200
        """
        Return cross-section from the Series/DataFrame.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions

    @property
    def attrs(self):  # noqa: RT01, D200
        """
        Return dictionary of global attributes of this dataset.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions

        def attrs(df):
            return df.attrs

        return self._default_to_pandas(attrs)

    @series_not_implemented()
    @property
    def array(self):  # noqa: RT01, D200
        """
        Return the ExtensionArray of the data backing this Series or Index.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions

        def array(df):
            return df.array

        return self._default_to_pandas(array)

    @property
    def axes(self):  # noqa: RT01, D200
        """
        Return a list of the row axis labels.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return [self.index]

    @property
    def cat(self):  # noqa: RT01, D200
        """
        Accessor object for categorical properties of the Series values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        from .series_utils import CategoryMethods

        return CategoryMethods(self)

    @property
    def dt(self):  # noqa: RT01, D200
        """
        Accessor object for datetimelike properties of the Series values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        current_dtype = self.dtype
        if not is_datetime64_any_dtype(current_dtype):
            raise AttributeError("Can only use .dt accessor with datetimelike values")

        from modin.pandas.series_utils import DatetimeProperties

        if DatetimeProperties._Series is not Series:
            del (
                DatetimeProperties._Series
            )  # Replace modin's Series class with Snowpark pandas Series
            DatetimeProperties._Series = Series

        return DatetimeProperties(self)

    @property
    def dtype(self):  # noqa: RT01, D200
        """
        Return the dtype object of the underlying data.
        See :func:`DataFrame.dtypes` for exact behavior.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3])
        >>> s.dtype
        dtype('int64')
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._query_compiler.dtypes.squeeze()

    dtypes = dtype

    @property
    def empty(self) -> bool:
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return len(self) == 0

    @property
    def hasnans(self):  # noqa: RT01, D200
        """
        Return True if Series has any nans.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.isna().sum() > 0

    def isna(self):
        """
        Detect missing values.

        Returns
        -------
        The result of detecting missing values.
        """
        return super().isna()

    def isnull(self):
        """
        Detect missing values.

        Returns
        -------
        The result of detecting missing values.
        """
        return super().isnull()

    @property
    def is_monotonic_increasing(self):  # noqa: RT01, D200
        """
        Return True if values in the Series are monotonic_increasing.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._reduce_dimension(self._query_compiler.is_monotonic_increasing())

    @property
    def is_monotonic_decreasing(self):  # noqa: RT01, D200
        """
        Return True if values in the Series are monotonic_decreasing.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self._reduce_dimension(self._query_compiler.is_monotonic_decreasing())

    @property
    def is_unique(self):  # noqa: RT01, D200
        """
        Return True if values in the Series are unique.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.nunique(dropna=False) == len(self)

    @series_not_implemented()
    @property
    def nbytes(self):  # noqa: RT01, D200
        """
        Return the number of bytes in the underlying data.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.memory_usage(index=False)

    @property
    def ndim(self) -> int:
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return 1

    def nunique(self, dropna=True):  # noqa: PR01, RT01, D200
        """
        Return number of unique elements in the object.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().nunique(dropna=dropna)

    @property
    def shape(
        self,
    ) -> tuple(int,):
        return (len(self),)

    def shift(
        self,
        periods: int | Sequence[int] = 1,
        freq=None,
        axis: Axis = 0,
        fill_value: Hashable = no_default,
        suffix: str | None = None,
    ):
        """
        Shift index by desired number of periods with an optional time `freq`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if axis == 1:
            # pandas compatible error.
            raise ValueError("No axis named 1 for object type Series")

        return super().shift(periods, freq, axis, fill_value, suffix)

    @property
    def str(self):  # noqa: RT01, D200
        """
        Vectorized string functions for Series and Index.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        current_dtype = self.dtype
        if not is_string_dtype(current_dtype):
            raise AttributeError("Can only use .str accessor with string values!")

        from modin.pandas.series_utils import StringMethods

        if StringMethods._Series is not Series:
            del (
                StringMethods._Series
            )  # Replace modin's Series class with Snowpark pandas Series
            StringMethods._Series = Series

        return StringMethods(self)

    def _to_pandas(
        self,
        *,
        statement_params: dict[str, str] | None = None,
        **kwargs: Any,
    ):
        """
        Convert Snowpark pandas Series to pandas Series

        Args:
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
            pandas series
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        df = self._query_compiler.to_pandas(statement_params=statement_params, **kwargs)
        if len(df.columns) == 0:
            return pandas.Series([])
        series = df[df.columns[0]]
        # special case when series is wrapped as dataframe, but has not label.
        # This is indicated with MODIN_UNNAMED_SERIES_LABEL
        if self._query_compiler.columns[0] == MODIN_UNNAMED_SERIES_LABEL:
            series.name = None

        return series

    def _to_datetime(self, **kwargs):
        """
        Convert `self` to datetime.

        Parameters
        ----------
        **kwargs : dict
            Optional arguments to use during query compiler's
            `to_datetime` invocation.

        Returns
        -------
        datetime
            Series of datetime64 dtype.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.__constructor__(
            query_compiler=self._query_compiler.series_to_datetime(**kwargs)
        )

    def _to_numeric(self, **kwargs: Any) -> Series:
        """
        Convert `self` to numeric.

        Parameters
        ----------
        **kwargs : dict
            Optional arguments to use during query compiler's
            `to_numeric` invocation.

        Returns
        -------
        numeric
            Series of numeric dtype.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.__constructor__(
            query_compiler=self._query_compiler.to_numeric(**kwargs)
        )

    def _qcut(
        self,
        q: int | ListLike,
        retbins: bool = False,
        duplicates: Literal["raise", "drop"] = "raise",
    ) -> Series:
        """
        Quantile-based discretization function.

        See SnowflakeQueryCompiler.qcut for details.

        """

        return self.__constructor__(
            query_compiler=self._query_compiler.qcut(q, retbins, duplicates)
        )

    def _reduce_dimension(self, query_compiler):
        """
        Try to reduce the dimension of data from the `query_compiler`.

        Parameters
        ----------
        query_compiler : BaseQueryCompiler
            Query compiler to retrieve the data.

        Returns
        -------
        pandas.Series or pandas.DataFrame.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return query_compiler.to_pandas().squeeze()

    def _validate_dtypes_sum_prod_mean(self, axis, numeric_only, ignore_axis=False):
        """
        Validate data dtype for `sum`, `prod` and `mean` methods.

        Parameters
        ----------
        axis : {0, 1}
            Axis to validate over.
        numeric_only : bool
            Whether or not to allow only numeric data.
            If True and non-numeric data is found, exception
            will be raised.
        ignore_axis : bool, default: False
            Whether or not to ignore `axis` parameter.

        Returns
        -------
        Series

        Notes
        -----
        Actually returns unmodified `self` object,
        added for compatibility with Modin DataFrame.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self

    def _validate_dtypes(self, numeric_only=False):
        """
        Check that all the dtypes are the same.

        Parameters
        ----------
        numeric_only : bool, default: False
            Whether or not to allow only numeric data.
            If True and non-numeric data is found, exception
            will be raised.

        Notes
        -----
        Actually does nothing, added for compatibility with Modin DataFrame.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        pass

    def _get_numeric_data(self, axis: int):
        """
        Grab only numeric data from Series.

        Parameters
        ----------
        axis : {0, 1}
            Axis to inspect on having numeric types only.

        Returns
        -------
        Series

        Notes
        -----
        `numeric_only` parameter is not supported by Series, so this method
        does not do anything. The method is added for compatibility with Modin DataFrame.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self

    def _update_inplace(self, new_query_compiler):
        """
        Update the current Series in-place using `new_query_compiler`.

        Parameters
        ----------
        new_query_compiler : BaseQueryCompiler
            QueryCompiler to use to manage the data.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        super()._update_inplace(new_query_compiler=new_query_compiler)
        # Propagate changes back to parent so that column in dataframe had the same contents
        if self._parent is not None:
            if self._parent_axis == 0:
                self._parent.loc[self.name] = self
            else:
                self._parent[self.name] = self

    def _create_or_update_from_compiler(self, new_query_compiler, inplace=False):
        """
        Return or update a Series with given `new_query_compiler`.

        Parameters
        ----------
        new_query_compiler : PandasQueryCompiler
            QueryCompiler to use to manage the data.
        inplace : bool, default: False
            Whether or not to perform update or creation inplace.

        Returns
        -------
        Series, DataFrame or None
            None if update was done, Series or DataFrame otherwise.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        assert (
            isinstance(new_query_compiler, type(self._query_compiler))
            or type(new_query_compiler) in self._query_compiler.__class__.__bases__
        ), f"Invalid Query Compiler object: {type(new_query_compiler)}"
        if not inplace and new_query_compiler.is_series_like():
            return self.__constructor__(query_compiler=new_query_compiler)
        elif not inplace:
            # This can happen with things like `reset_index` where we can add columns.
            from snowflake.snowpark.modin.pandas.dataframe import DataFrame

            return DataFrame(query_compiler=new_query_compiler)
        else:
            self._update_inplace(new_query_compiler=new_query_compiler)

    def _repartition(self):
        """
        Repartitioning Series to get ideal partitions inside.

        Allows to improve performance where the query compiler can't improve
        yet by doing implicit repartitioning.

        Returns
        -------
        Series
            The repartitioned Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super()._repartition(axis=0)

    # Persistance support methods - BEGIN
    @classmethod
    def _inflate_light(cls, query_compiler, name):
        """
        Re-creates the object from previously-serialized lightweight representation.

        The method is used for faster but not disk-storable persistence.

        Parameters
        ----------
        query_compiler : BaseQueryCompiler
            Query compiler to use for object re-creation.
        name : str
            The name to give to the new object.

        Returns
        -------
        Series
            New Series based on the `query_compiler`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return cls(query_compiler=query_compiler, name=name)

    @classmethod
    def _inflate_full(cls, pandas_series):
        """
        Re-creates the object from previously-serialized disk-storable representation.

        Parameters
        ----------
        pandas_series : pandas.Series
            Data to use for object re-creation.

        Returns
        -------
        Series
            New Series based on the `pandas_series`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return cls(data=pandas_series)

    @series_not_implemented()
    def __reduce__(self):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions

        self._query_compiler.finalize()
        # if PersistentPickle.get():
        #    return self._inflate_full, (self._to_pandas(),)
        return self._inflate_light, (self._query_compiler, self.name)

    # Persistance support methods - END
