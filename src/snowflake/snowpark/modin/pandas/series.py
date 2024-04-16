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
from textwrap import dedent
from typing import IO, TYPE_CHECKING, Any, Callable, Literal

import numpy as np
import numpy.typing as npt
import pandas
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
from pandas.errors import SpecificationError
from pandas.util._decorators import doc
from pandas.util._validators import validate_bool_kwarg

from snowflake.snowpark.modin.pandas.accessor import CachedAccessor, SparseAccessor
from snowflake.snowpark.modin.pandas.base import _ATTRS_NO_LOOKUP, BasePandasDataset
from snowflake.snowpark.modin.pandas.iterator import PartitionIterator
from snowflake.snowpark.modin.pandas.shared_docs import _shared_docs
from snowflake.snowpark.modin.pandas.utils import _doc_binary_op, from_pandas, is_scalar
from snowflake.snowpark.modin.plugin._typing import DropKeep, ListLike
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    MODIN_UNNAMED_SERIES_LABEL,
    _create_operator_docstring,
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

_shared_doc_kwargs = {
    "axes": "index",
    "klass": "Series",
    "axes_single_arg": "{0 or 'index'}",
    "axis": """axis : {0 or 'index'}
        Unused. Parameter needed for compatibility with DataFrame.""",
    "inplace": """inplace : bool, default False
        If True, performs operation inplace and returns None.""",
    "unique": "np.ndarray",
    "duplicated": "Series",
    "optional_by": "",
    "optional_reindex": """
index : array-like, optional
    New labels for the index. Preferably an Index object to avoid
    duplicating data.
axis : int or str, optional
    Unused.""",
}
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
    ],
    apilink="pandas.Series",
)
class Series(BasePandasDataset):
    """
    Snowpark pandas representation of `pandas.Series` with a lazily-evaluated relational dataset.

    A Series is considered lazy because it encapsulates the computation or query required to produce
    the final dataset. The computation is not performed until the datasets need to be displayed, or i/o
    methods like to_pandas, to_snowflake are called.

    Internally, the underlying data are stored as Snowflake table with rows and columns.

    Parameters
    ----------
    data : modin.pandas.Series, array-like, Iterable, dict, or scalar value, optional
        Contains data stored in Series. If data is a dict, argument order is
        maintained.
    index : array-like or Index (1d), optional
        Values must be hashable and have the same length as `data`.
    dtype : str, np.dtype, or pandas.ExtensionDtype, optional
        Data type for the output Series. If not specified, this will be
        inferred from `data`.
    name : str, optional
        The name to give to the Series.
    copy : bool, default: False
        Copy input data.
    fastpath : bool, default: False
        `pandas` internal parameter.
    query_compiler : BaseQueryCompiler, optional
        A query compiler object to create the Series from.

    Examples
    --------
    Constructing Series from a dictionary with an Index specified

    >>> d = {'a': 1, 'b': 2, 'c': 3}
    >>> ser = pd.Series(data=d, index=['a', 'b', 'c'])
    >>> ser
    a    1
    b    2
    c    3
    dtype: int64

    The keys of the dictionary match with the Index values, hence the Index
    values have no effect.

    >>> d = {'a': 1, 'b': 2, 'c': 3}
    >>> ser = pd.Series(data=d, index=['x', 'y', 'z'])
    >>> ser
    x   NaN
    y   NaN
    z   NaN
    dtype: float64
    """

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
                    isinstance(data, (pandas.Series, pandas.Index))
                    and data.name is not None
                ):
                    name = data.name

            query_compiler = from_pandas(
                pandas.DataFrame(
                    pandas.Series(
                        data=data,
                        index=index,
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

    @_doc_binary_op(operation="addition", bin_op="add")
    def __add__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.add(right)

    @_doc_binary_op(operation="addition", bin_op="radd", right="left")
    def __radd__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.radd(left)

    @_doc_binary_op(operation="union", bin_op="and", right="other")
    def __and__(self, other):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__and__(other)

    @_doc_binary_op(operation="union", bin_op="and", right="other")
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

    @_doc_binary_op(
        operation="integer division and modulo",
        bin_op="divmod",
        returns="tuple of two Series",
    )
    def __divmod__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.divmod(right)

    @_doc_binary_op(
        operation="integer division and modulo",
        bin_op="divmod",
        right="left",
        returns="tuple of two Series",
    )
    def __rdivmod__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.rdivmod(left)

    @_doc_binary_op(operation="integer division", bin_op="floordiv")
    def __floordiv__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.floordiv(right)

    @_doc_binary_op(operation="integer division", bin_op="floordiv")
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

    @_doc_binary_op(operation="modulo", bin_op="mod")
    def __mod__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.mod(right)

    @_doc_binary_op(operation="modulo", bin_op="mod", right="left")
    def __rmod__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.rmod(left)

    @_doc_binary_op(operation="multiplication", bin_op="mul")
    def __mul__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.mul(right)

    @_doc_binary_op(operation="multiplication", bin_op="mul", right="left")
    def __rmul__(self, left):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.rmul(left)

    @_doc_binary_op(operation="disjunction", bin_op="or", right="other")
    def __or__(self, other):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__or__(other)

    @_doc_binary_op(operation="disjunction", bin_op="or", right="other")
    def __ror__(self, other):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__ror__(other)

    @_doc_binary_op(operation="exclusive or", bin_op="xor", right="other")
    def __xor__(self, other):  # pragma: no cover
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__xor__(other)

    @_doc_binary_op(operation="exclusive or", bin_op="xor", right="other")
    def __rxor__(self, other):  # pragma: no cover
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().__rxor__(other)

    @_doc_binary_op(operation="exponential power", bin_op="pow")
    def __pow__(self, right):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self.pow(right)

    @_doc_binary_op(operation="exponential power", bin_op="pow", right="left")
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

    @_doc_binary_op(operation="subtraction", bin_op="sub")
    def __sub__(self, right):
        return self.sub(right)

    @_doc_binary_op(operation="subtraction", bin_op="sub", right="left")
    def __rsub__(self, left):
        return self.rsub(left)

    @_doc_binary_op(operation="floating division", bin_op="truediv")
    def __truediv__(self, right):
        return self.truediv(right)

    @_doc_binary_op(operation="floating division", bin_op="truediv", right="left")
    def __rtruediv__(self, left):
        return self.rtruediv(left)

    __iadd__ = __add__
    __imul__ = __add__
    __ipow__ = __pow__
    __isub__ = __sub__
    __itruediv__ = __truediv__

    @_create_operator_docstring(pandas.core.series.Series.add, overwrite_existing=True)
    def add(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return Addition of series and other, element-wise (binary operator add).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().add(other, level=level, fill_value=fill_value, axis=axis)

    @_create_operator_docstring(pandas.core.series.Series.radd, overwrite_existing=True)
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


        For Series, the row labels are prefixed.
        For DataFrame, the column labels are prefixed.

        Parameters
        ----------
        prefix : str
            The string to add before each label.

        Returns
        -------
        Series or DataFrame
            New Series or DataFrame with updated labels.

        See Also
        --------
        Series.add_suffix: Suffix row labels with string `suffix`.
        DataFrame.add_suffix: Suffix column labels with string `suffix`.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3, 4])
        >>> s
        0    1
        1    2
        2    3
        3    4
        dtype: int64

        >>> s.add_prefix('item_')
        item_0    1
        item_1    2
        item_2    3
        item_3    4
        dtype: int64

        >>> df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [3, 4, 5, 6]})
        >>> df
           A  B
        0  1  3
        1  2  4
        2  3  5
        3  4  6

        >>> df.add_prefix('col_')
           col_A  col_B
        0      1      3
        1      2      4
        2      3      5
        3      4      6
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

        For Series, the row labels are suffixed.
        For DataFrame, the column labels are suffixed.

        Parameters
        ----------
        suffix : str
            The string to add after each label.

        Returns
        -------
        Series or DataFrame
            New Series or DataFrame with updated labels.

        See Also
        --------
        Series.add_prefix: Prefix row labels with string `prefix`.
        DataFrame.add_prefix: Prefix column labels with string `prefix`.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3, 4])
        >>> s
        0    1
        1    2
        2    3
        3    4
        dtype: int64

        >>> s.add_suffix('_item')
        0_item    1
        1_item    2
        2_item    3
        3_item    4
        dtype: int64

        >>> df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [3, 4, 5, 6]})
        >>> df
           A  B
        0  1  3
        1  2  4
        2  3  5
        3  4  6

        >>> df.add_suffix('_col')
           A_col  B_col
        0      1      3
        1      2      4
        2      3      5
        3      4      6
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
        Return Series with specified index labels removed.

        Remove elements of a Series based on specifying the index labels.
        When using a MultiIndex, labels on different levels can be removed
        by specifying the level.

        Parameters
        ----------
        labels : single label or list-like
            Index labels to drop.
        axis : {0 or 'index'}
            Unused. Parameter needed for compatibility with DataFrame.
        index : single label or list-like
            Redundant for application on Series, but 'index' can be used instead
            of 'labels'.
        columns : single label or list-like
            No change is made to the Series; use 'index' or 'labels' instead.
        level : int or level name, optional
            For MultiIndex, level for which the labels will be removed.
        inplace : bool, default False
            If True, do operation inplace and return None.
        errors : {'ignore', 'raise'}, default 'raise'
            If 'ignore', suppress error and only existing labels are dropped.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` or None
            Series with specified index labels removed or None if ``inplace=True``.

        Raises
        ------
        KeyError
            If none of the labels are found in the index.

        See Also
        --------
        Series.reindex : Return only specified index labels of Series.
        Series.dropna : Return series without null values.
        Series.drop_duplicates : Return Series with duplicate values removed.
        DataFrame.drop : Drop specified labels from rows or columns.

        Examples
        --------
        >>> s = pd.Series(data=np.arange(3), index=['A', 'B', 'C'])
        >>> s
        A    0
        B    1
        C    2
        dtype: int64

        Drop labels B en C

        >>> s.drop(labels=['B', 'C'])
        A    0
        dtype: int64

        Drop 2nd level label in MultiIndex Series

        >>> midx = pd.MultiIndex(levels=[['lama', 'cow', 'falcon'],
        ...                              ['speed', 'weight', 'length']],
        ...                      codes=[[0, 0, 0, 1, 1, 1, 2, 2, 2],
        ...                             [0, 1, 2, 0, 1, 2, 0, 1, 2]])
        >>> s = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3],
        ...               index=midx)
        >>> s
        lama    speed      45.0
                weight    200.0
                length      1.2
        cow     speed      30.0
                weight    250.0
                length      1.5
        falcon  speed     320.0
                weight      1.0
                length      0.3
        dtype: float64

        >>> s.drop(labels='weight', level=1)
        lama    speed      45.0
                length      1.2
        cow     speed      30.0
                length      1.5
        falcon  speed     320.0
                length      0.3
        dtype: float64
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

    _agg_examples_doc = dedent(
        """
    Examples
    --------
    >>> s = pd.Series([1, 2, 3, 4])
    >>> s
    0    1
    1    2
    2    3
    3    4
    dtype: int64

    >>> s.agg('min')
    1

    >>> s.agg(['min', 'max'])
    min    1
    max    4
    dtype: int64
    """
    )

    @doc(
        _shared_docs["aggregate"],
        klass=_shared_doc_kwargs["klass"],
        axis=_shared_doc_kwargs["axis"],
        examples=_agg_examples_doc,
    )
    def aggregate(
        self, func: AggFuncType = None, axis: Axis = 0, *args: Any, **kwargs: Any
    ):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if is_dict_like(func):
            raise SpecificationError(
                "Value for func argument in dict format is not allowed for Series aggregate."
            )

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
        Invoke function on values of Series.

        Can be ufunc (a NumPy function that applies to the entire Series)
        or a Python function that only works on single values.

        Parameters
        ----------
        func : function
            Python function or NumPy ufunc to apply.
        convert_dtype : bool, default None
            Try to find better dtype for elementwise function results. convert_dtype has been
            ignored with Snowflake execution engine.
        args : tuple
            Positional arguments passed to func after the series value.
        **kwargs
            Additional keyword arguments passed to func.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` or Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            If func returns a Series object the result will be a DataFrame.


        See Also
        --------
        :func:`Series.map <snowflake.snowpark.modin.pandas.Series.map>` : For applying more complex functions on a Series.

        :func:`DataFrame.apply <snowflake.snowpark.modin.pandas.DataFrame.apply>` : Apply a function row-/column-wise.

        :func:`DataFrame.applymap <snowflake.snowpark.modin.pandas.DataFrame.applymap>` : Apply a function elementwise on a whole DataFrame.

        Notes
        -----
        1. When the type annotation of return value is provided on ``func``, the result will be cast
        to the corresponding dtype. When no type annotation is provided, data will be converted
        to Variant type in Snowflake and leave as dtype=object. In this case, the return value must
        be JSON-serializable.

        2. Under the hood, we use `Snowflake Vectorized Python UDFs <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-batch>`_.
        to implement apply() method. You can find type mappings from Snowflake SQL types to pandas
        dtypes `here <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-batch#type-support>`_.

        3. Snowflake supports two types of NULL values in variant data: `JSON NULL and SQL NULL <https://docs.snowflake.com/en/user-guide/semistructured-considerations#null-values>`_.
        When no type annotation is provided and Variant data is returned, Python ``None`` is translated to
        JSON NULL, and all other pandas missing values (np.nan, pd.NA, pd.NaT) are translated to SQL NULL.

        4. For working with 3rd-party-packages see :func:`DataFrame.apply <snowflake.snowpark.modin.pandas.DataFrame.apply>`.
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

    def argmax(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return int position of the largest value in the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        result = self.idxmax(axis=axis, skipna=skipna, *args, **kwargs)
        if np.isnan(result) or result is pandas.NA:
            result = -1
        return result

    def argmin(self, axis=None, skipna=True, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return int position of the smallest value in the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        result = self.idxmin(axis=axis, skipna=skipna, *args, **kwargs)
        if np.isnan(result) or result is pandas.NA:
            result = -1
        return result

    def argsort(self, axis=0, kind="quicksort", order=None):  # noqa: PR01, RT01, D200
        """
        Return the integer indices that would sort the Series values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas(
            pandas.Series.argsort, axis=axis, kind=kind, order=order
        )

    def autocorr(self, lag=1):  # noqa: PR01, RT01, D200
        """
        Compute the lag-N autocorrelation.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self.corr(self.shift(lag))

    def between(self, left, right, inclusive: str = "both"):  # noqa: PR01, RT01, D200
        """
        Return boolean Series equivalent to left <= series <= right.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas(
            pandas.Series.between, left, right, inclusive=inclusive
        )

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
        ErrorMessage.not_implemented()
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

    def corr(self, other, method="pearson", min_periods=None):  # noqa: PR01, RT01, D200
        """
        Compute correlation with `other` Series, excluding missing values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
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

    def cov(
        self, other, min_periods=None, ddof: int | None = 1
    ):  # noqa: PR01, RT01, D200
        """
        Compute covariance with Series, excluding missing values.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
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

        For non-numeric datasets, computes `count` (# of non-null items), `unique` (# of unique items),
        `top` (the mode; the element at the lowest position if multiple), and `freq` (# of times the mode appears).

        For numeric datasets, computes `count` (# of non-null items), `mean`, `std`, `min`,
        the specified percentiles, and `max`.

        Parameters
        ----------
        percentiles: Optional[ListLike], default None
            The percentiles to compute for numeric columns. If unspecified, defaults to [0.25, 0.5, 0.75],
            which returns the 25th, 50th, and 75th percentiles. All values should fall between 0 and 1.
            The median (0.5) will always be added to the displayed percentile if not already included;
            the min and max are always displayed in addition to the percentiles.
        include: Optional[List[str, ExtensionDtype | np.dtype]] | "all", default None
            Ignored for Series.
        exclude: Optional[List[str, ExtensionDtype | np.dtype]], default None
            Ignored for Series.

        Returns
        -------
        Series
            A series containing statistics for the dataset.

        Examples
        --------
        Describing numeric data:

        >>> pd.Series([1, 2, 3]).describe()  # doctest: +NORMALIZE_WHITESPACE
        count    3.0
        mean     2.0
        std      1.0
        min      1.0
        25%      1.5
        50%      2.0
        75%      2.5
        max      3.0
        dtype: float64

        Describing non-numeric data:

        >>> pd.Series(['a', 'b', 'c']).describe()  # doctest: +NORMALIZE_WHITESPACE
        count     3
        unique    3
        top       a
        freq      1
        dtype: object
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

        Calculates the difference of a Series element compared with another element in the Series (default is element in previous row).

        Parameters
        ----------
        periods : int, default 1
            Periods to shift for calculating difference, accepts negative values.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series`
            Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` with the first differences of the Series.

        Notes
        -----
        For boolean dtypes, this uses operator.xor() rather than operator.sub(). The result is calculated according
        to current dtype in Series, however dtype of the result is always float64.

        Examples
        --------
        Difference with previous row

        >>> s = pd.Series([1, 1, 2, 3, 5, 8])
        >>> s.diff()
        0    NaN
        1    0.0
        2    1.0
        3    1.0
        4    2.0
        5    3.0
        dtype: float64

        Difference with 3rd previous row

        >>> s.diff(periods=3)
        0    NaN
        1    NaN
        2    NaN
        3    2.0
        4    4.0
        5    6.0
        dtype: float64

        Difference with following row

        >>> s.diff(periods=-1)
        0    0.0
        1   -1.0
        2   -1.0
        3   -2.0
        4   -3.0
        5    NaN
        dtype: float64
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().diff(periods=periods, axis=0)

    def divmod(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return Integer division and modulo of series and `other`, element-wise (binary operator `divmod`).
        Not implemented
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()

    def dot(self, other):  # noqa: PR01, RT01, D200
        """
        Compute the dot product between the Series and the columns of `other`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()  # pragma: no cover

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

        Parameters
        ----------
        keep : {'first', 'last', False}, default 'first'
            Method to handle dropping duplicates:
            'first' : Drop duplicates except for the first occurrence.
            'last' : Drop duplicates except for the last occurrence.
            False : Drop all duplicates.
        inplace : bool, default False
            If True, performs operation inplace and returns None.
        ignore_index : bool, default False
            If True, the resulting axis will be labeled 0, 1, …, n - 1.

        Returns
        -------
        Series or None
            Series with duplicates dropped or None if inplace=True.

        Examples
        --------
        Generate a Series with duplicated entries.

        >>> s = pd.Series(['llama', 'cow', 'llama', 'beetle', 'llama', 'hippo'],
        ...                 name='animal')
        >>> s
        0     llama
        1       cow
        2     llama
        3    beetle
        4     llama
        5     hippo
        Name: animal, dtype: object

        With the 'keep' parameter, the selection behaviour of duplicated values can be changed. The value 'first' keeps the first occurrence for each set of duplicated entries. The default value of keep is 'first'.

        >>> s.drop_duplicates()
        0     llama
        1       cow
        3    beetle
        5     hippo
        Name: animal, dtype: object

        The value 'last' for parameter 'keep' keeps the last occurrence for each set of duplicated entries.

        >>> s.drop_duplicates(keep='last')
        1       cow
        3    beetle
        4     llama
        5     hippo
        Name: animal, dtype: object

        The value False for parameter 'keep' discards all sets of duplicated entries.

        >>> s.drop_duplicates(keep=False)
        1       cow
        3    beetle
        5     hippo
        Name: animal, dtype: object
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

        Parameters
        ----------
        axis : {0 or 'index'}
            Unused. Parameter needed for compatibility with DataFrame.
        inplace : bool, default False
            If True, do operation inplace and return None.
        how : str, optional
            Not in use. Kept for compatibility.

        Returns
        -------
        Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` or None
            Series with NA entries dropped from it or None if ``inplace=True``.

        See Also
        --------
        Series.isna: Indicate missing values.
        Series.notna : Indicate existing (non-missing) values.
        Series.fillna : Replace missing values.
        DataFrame.dropna : Drop rows or columns which contain NA values.
        Index.dropna : Drop missing indices.

        Examples
        --------
        >>> ser = pd.Series([1., 2., np.nan])
        >>> ser
        0    1.0
        1    2.0
        2    NaN
        dtype: float64

        Drop NA values from a Series.

        >>> ser.dropna()
        0    1.0
        1    2.0
        dtype: float64

        Keep the Series with valid entries in the same variable.

        >>> ser.dropna(inplace=True)
        >>> ser
        0    1.0
        1    2.0
        dtype: float64

        Empty strings are not considered NA values. ``None`` is considered an
        NA value.

        >>> ser = pd.Series([np.NaN, 2, pd.NaT, '', None, 'I stay'])
        >>> ser  # doctest: +NORMALIZE_WHITESPACE
        0      None
        1         2
        2      None
        3
        4      None
        5    I stay
        dtype: object
        >>> ser.dropna()  # doctest: +NORMALIZE_WHITESPACE
        1         2
        3
        5    I stay
        dtype: object
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super()._dropna(axis=axis, inplace=inplace, how=how)

    def duplicated(self, keep: DropKeep = "first"):
        """
        Indicate duplicate Series values.

        Duplicated values are indicated as ``True`` values in the resulting
        Series. Either all duplicates, all except the first or all except the
        last occurrence of duplicates can be indicated.

        Parameters
        ----------
        keep : {'first', 'last', False}, default 'first'
            Method to handle dropping duplicates:

            - 'first' : Mark duplicates as ``True`` except for the first
              occurrence.
            - 'last' : Mark duplicates as ``True`` except for the last
              occurrence.
            - ``False`` : Mark all duplicates as ``True``.

        Returns
        -------
        Snowpark pandas Series[bool]
            Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` indicating whether each value has occurred
            in the preceding values.

        See Also
        --------
        Index.duplicated : Equivalent method on pandas.Index.
        DataFrame.duplicated : Equivalent method on pandas.DataFrame.
        Series.drop_duplicates : Remove duplicate values from Series.

        Examples
        --------
        By default, for each set of duplicated values, the first occurrence is
        set on False and all others on True:

        >>> animals = pd.Series(['llama', 'cow', 'llama', 'beetle', 'llama'])
        >>> animals.duplicated()
        0    False
        1    False
        2     True
        3    False
        4     True
        dtype: bool

        which is equivalent to

        >>> animals.duplicated(keep='first')
        0    False
        1    False
        2     True
        3    False
        4     True
        dtype: bool

        By using 'last', the last occurrence of each set of duplicated values
        is set on False and all others on True:

        >>> animals.duplicated(keep='last')
        0     True
        1    False
        2     True
        3    False
        4    False
        dtype: bool

        By setting keep on ``False``, all duplicates are True:

        >>> animals.duplicated(keep=False)
        0     True
        1    False
        2     True
        3    False
        4     True
        dtype: bool
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        name = self.name
        series = self.to_frame().duplicated(keep=keep)
        # we are using df.duplicated method for series but its result will lose the series name, so we preserve it here
        series.name = name
        return series

    @_create_operator_docstring(pandas.core.series.Series.eq, overwrite_existing=True)
    def eq(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return Equal to of series and `other`, element-wise (binary operator `eq`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().eq(other, level=level, axis=axis)

    def equals(self, other):  # noqa: PR01, RT01, D200
        """
        Test whether two objects contain the same elements.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()  # pragma: no cover

        return (
            self.name == other.name
            and self.index.equals(other.index)
            and self.eq(other).all()
        )

    def explode(self, ignore_index: bool = False):  # noqa: PR01, RT01, D200
        """
        Transform each element of a list-like to a row.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()

        return super().explode(
            MODIN_UNNAMED_SERIES_LABEL if self.name is None else self.name,
            ignore_index=ignore_index,
        )

    def factorize(
        self, sort=False, na_sentinel=no_default, use_na_sentinel=no_default
    ):  # noqa: PR01, RT01, D200
        """
        Encode the object as an enumerated type or categorical variable.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas(
            pandas.Series.factorize,
            sort=sort,
            na_sentinel=na_sentinel,
            use_na_sentinel=use_na_sentinel,
        )

    @_inherit_docstrings(pandas.Series.fillna, apilink="pandas.Series.fillna")
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

    @_create_operator_docstring(
        pandas.core.series.Series.floordiv, overwrite_existing=True
    )
    def floordiv(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Get Integer division of dataframe and `other`, element-wise (binary operator `floordiv`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().floordiv(other, level=level, fill_value=fill_value, axis=axis)

    @_create_operator_docstring(pandas.core.series.Series.ge, overwrite_existing=True)
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

        Args:
            by: mapping, function, label, Snowpark pandas Series or a list of such. Used to determine the groups for the groupby.
                If by is a function, it’s called on each value of the object’s index. If a dict or Series is
                passed, the Series or dict VALUES will be used to determine the groups (the Series’ values are first aligned;
                see .align() method). If a list or ndarray of length equal to the selected axis is passed (see the groupby
                user guide), the values are used as-is to determine the groups. A label or list of labels may be passed
                to group by the columns in self. Notice that a tuple is interpreted as a (single) key.

            axis: {0 or ‘index’, 1 or ‘columns’}, default 0
                Split along rows (0) or columns (1). For Series this parameter is unused and defaults to 0.

            level: int, level name, or sequence of such, default None
                If the axis is a MultiIndex (hierarchical), group by a particular level or levels. Do not specify both by and level.

            as_index: bool, default True
                    For aggregated output, return object with group labels as the index. Only relevant for DataFrame input.
                    as_index=False is effectively “SQL-style” grouped output.

            sort: bool, default True
                Sort group keys. Groupby preserves the order of rows within each group. Note that in pandas,
                better performance can be achieved by turning sort off, this is not going to be true with Snowpark
                pandas API. When sort=False, the performance will be no better than sort=True.

            group_keys: bool, default True
                    When calling apply and the by argument produces a like-indexed (i.e. a transform) result, add group
                    keys to index to identify pieces. By default, group keys are not included when the result’s index
                    (and column) labels match the inputs, and are included otherwise.

            observed: bool, default False
                    This only applies if any of the groupers are Categoricals. If True: only show observed values for
                    categorical groupers. If False: show all values for categorical groupers. This parameter is
                    currently ignored with Snowpark pandas API, since Category type is currently not supported with
                    Snowpark pandas API.

            dropna: bool, default True
                    If True, and if group keys contain NA values, NA values together with row/column will be dropped.
                    If False, NA values will also be treated as the key in groups.

        Returns:
            Snowpark pandas SeriesGroupBy: Returns a groupby object that contains information about the groups.

        Examples::
            >>> ser = pd.Series([390., 350., 30., 20.],
            ...                 index=['Falcon', 'Falcon', 'Parrot', 'Parrot'],
            ...                 name="Max Speed")
            >>> ser
            Falcon    390.0
            Falcon    350.0
            Parrot     30.0
            Parrot     20.0
            Name: Max Speed, dtype: float64
            >>> ser.groupby(["a", "b", "a", "b"]).mean()
            a    210.0
            b    185.0
            Name: Max Speed, dtype: float64
            >>> ser.groupby(level=0).mean()
            Falcon    370.0
            Parrot     25.0
            Name: Max Speed, dtype: float64

            **Grouping by Indexes**

            We can groupby different levels of a hierarchical index
            using the `level` parameter:

            >>> arrays = [['Falcon', 'Falcon', 'Parrot', 'Parrot'],
            ...           ['Captive', 'Wild', 'Captive', 'Wild']]
            >>> index = pd.MultiIndex.from_arrays(arrays, names=('Animal', 'Type'))
            >>> ser = pd.Series([390., 350., 30., 20.], index=index, name="Max Speed")
            >>> ser    # doctest: +NORMALIZE_WHITESPACE
            Animal  Type
            Falcon  Captive    390.0
                    Wild       350.0
            Parrot  Captive     30.0
                    Wild        20.0
            Name: Max Speed, dtype: float64
            >>> ser.groupby(level=0).mean()     # doctest: +NORMALIZE_WHITESPACE
            Animal
            Falcon    370.0
            Parrot     25.0
            Name: Max Speed, dtype: float64
            >>> ser.groupby(level="Type").mean()        # doctest: +NORMALIZE_WHITESPACE
            Type
            Captive    210.0
            Wild       185.0
            Name: Max Speed, dtype: float64

            We can also choose to include `NA` in group keys or not by defining
            `dropna` parameter, the default setting is `True`.

            >>> ser = pd.Series([1, 2, 3, 3], index=["a", 'a', 'b', np.nan])
            >>> ser.groupby(level=0).sum()      # doctest: +SKIP
            a    3
            b    3
            dtype: int64

            >>> ser.groupby(level=0, dropna=False).sum()        # doctest: +SKIP
            a    3
            b    3
            NaN  3
            dtype: int64
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

    @_create_operator_docstring(pandas.core.series.Series.gt, overwrite_existing=True)
    def gt(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return greater than of series and `other`, element-wise (binary operator `gt`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().gt(other, level=level, axis=axis)

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
        ErrorMessage.not_implemented()
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
        ErrorMessage.not_implemented()
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

    def item(self):  # noqa: RT01, D200
        """
        Return the first element of the underlying data as a Python scalar.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self[0]

    def items(self):  # noqa: D200
        """
        Lazily iterate over (index, value) tuples.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()

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

    @_create_operator_docstring(pandas.core.series.Series.le, overwrite_existing=True)
    def le(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return less than or equal to of series and `other`, element-wise (binary operator `le`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().le(other, level=level, axis=axis)

    @_create_operator_docstring(pandas.core.series.Series.lt, overwrite_existing=True)
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
        Map values of Series according to an input mapping or function.

        Used for substituting each value in a Series with another value,
        that may be derived from a function, a ``dict`` or
        a :class:`Series`.

        Parameters
        ----------
        arg : function, collections.abc.Mapping subclass or Series
            Mapping correspondence.
        na_action : {None, 'ignore'}, default None
            If 'ignore', propagate NULL values, without passing them to the
            mapping correspondence. Note that, it will not bypass NaN values
            in a FLOAT column in Snowflake.

        Returns
        -------
        Series
            Same index as caller.

        See Also
        --------
        :func:`Series.apply <snowflake.snowpark.modin.pandas.Series.apply>` : For applying more complex functions on a Series.

        :func:`DataFrame.apply <snowflake.snowpark.modin.pandas.DataFrame.apply>` : Apply a function row-/column-wise.

        :func:`DataFrame.applymap <snowflake.snowpark.modin.pandas.DataFrame.applymap>` : Apply a function elementwise on a whole DataFrame.

        Notes
        -----
        When ``arg`` is a dictionary, values in Series that are not in the
        dictionary (as keys) are converted to ``NaN``. However, if the
        dictionary is a ``dict`` subclass that defines ``__missing__`` (i.e.
        provides a method for default values), then this default is used
        rather than ``NaN``.

        Examples
        --------
        >>> s = pd.Series(['cat', 'dog', np.nan, 'rabbit'])
        >>> s
        0       cat
        1       dog
        2      None
        3    rabbit
        dtype: object

        ``map`` accepts a ``dict`` or a ``Series``. Values that are not found
        in the ``dict`` are converted to ``NaN``, unless the dict has a default
        value (e.g. ``defaultdict``):

        >>> s.map({'cat': 'kitten', 'dog': 'puppy'})
        0    kitten
        1     puppy
        2      None
        3      None
        dtype: object

        It also accepts a function:

        >>> s.map('I am a {}'.format)
        0       I am a cat
        1       I am a dog
        2      I am a <NA>
        3    I am a rabbit
        dtype: object

        To avoid applying the function to missing values (and keep them as
        ``NaN``) ``na_action='ignore'`` can be used:

        >>> s.map('I am a {}'.format, na_action='ignore')
        0       I am a cat
        1       I am a dog
        2             None
        3    I am a rabbit
        dtype: object

        Note that in the above example, the missing value in Snowflake is NULL,
        it is mapped to ``None`` in a string/object column.
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
        """
        Replace values where the condition is True.

        Args:
            cond: bool Series/DataFrame, array-like or callable
                Where cond is False, keep the original value. Where True, replace with corresponding value from other.
                If cond is callable, it is computed on the Series/DataFrame and should return boolean Series/DataFrame
                or array. The callable must not change input Series/DataFrame (though pandas doesn't check it).

            other: scalar, Series/DataFrame, or callable
                Entries where cond is True are replaced with corresponding value from other. If other is callable,
                it is computed on the Series/DataFrame and should return scalar or Series/DataFrame. The callable
                must not change input Series/DataFrame (though pandas doesn’t check it).

            inplace: bool, default False
                Whether to perform the operation in place on the data.

            axis: int, default None
                Alignment axis if needed. For Series this parameter is unused and defaults to 0.

            level: int, default None
                Alignment level if needed.

        Returns:
            Same type as caller or None if inplace=True.

        See Also:
            Series.where : Replace values where the condition is False.

        Notes:
            The mask method is an application of the if-then idiom. For each element in the calling DataFrame, if cond
            is False the element is used; otherwise the corresponding element from the DataFrame other is used. If the
            axis of other does not align with axis of cond Series/DataFrame, the misaligned index positions will be
            filled with True.

            The signature for DataFrame.where() differs from numpy.where(). Roughly df1.where(m, df2) is equivalent to
            np.where(m, df1, df2).

            For further details and examples see the mask documentation in indexing.

            The dtype of the object takes precedence. The fill value is casted to the object’s dtype, if this can be
            done losslessly.

        Examples::
        >>> s = pd.Series(range(5))
        >>> s.mask(s > 0)  # doctest: +NORMALIZE_WHITESPACE
        0    0.0
        1    NaN
        2    NaN
        3    NaN
        4    NaN
        dtype: float64

        >>> s = pd.Series(range(5))
        >>> t = pd.Series([True, False])
        >>> s.mask(t, 99)  # doctest: +NORMALIZE_WHITESPACE
        0    99
        1     1
        2    99
        3    99
        4    99
        dtype: int64

        >>> s.mask(s > 1, 10)  # doctest: +NORMALIZE_WHITESPACE
        0     0
        1     1
        2    10
        3    10
        4    10
        dtype: int64
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().mask(
            cond,
            other=other,
            inplace=inplace,
            axis=axis,
            level=level,
        )

    def memory_usage(self, index=True, deep=False):  # noqa: PR01, RT01, D200
        """
        Return the memory usage of the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()  # pragma: no cover

        if index:
            result = self._reduce_dimension(
                self._query_compiler.memory_usage(index=False, deep=deep)
            )
            index_value = self.index.memory_usage(deep=deep)
            return result + index_value
        return super().memory_usage(index=index, deep=deep)

    @_create_operator_docstring(pandas.core.series.Series.mod, overwrite_existing=True)
    def mod(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return Modulo of series and `other`, element-wise (binary operator `mod`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().mod(other, level=level, fill_value=fill_value, axis=axis)

    def mode(self, dropna=True):  # noqa: PR01, RT01, D200
        """
        Return the mode(s) of the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return super().mode(numeric_only=False, dropna=dropna)

    @_create_operator_docstring(pandas.core.series.Series.mul, overwrite_existing=True)
    def mul(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return multiplication of series and `other`, element-wise (binary operator `mul`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().mul(other, level=level, fill_value=fill_value, axis=axis)

    multiply = mul

    @_create_operator_docstring(pandas.core.series.Series.rmul, overwrite_existing=True)
    def rmul(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return multiplication of series and `other`, element-wise (binary operator `mul`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rmul(other, level=level, fill_value=fill_value, axis=axis)

    @_create_operator_docstring(pandas.core.series.Series.ne, overwrite_existing=True)
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
        ErrorMessage.not_implemented()
        return self._default_to_pandas(pandas.Series.nlargest, n=n, keep=keep)

    def nsmallest(self, n=5, keep="first"):  # noqa: PR01, RT01, D200
        """
        Return the smallest `n` elements.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self.__constructor__(
            query_compiler=self._query_compiler.nsmallest(n=n, keep=keep)
        )

    def set_axis(
        self,
        labels: IndexLabel,
        *,
        axis: Axis = 0,
        copy: bool | NoDefault = no_default,  # ignored
    ):
        """
        Assign desired index to given axis.

        Parameters
        ----------
        labels : list-like, Index, MultiIndex
            The values for the new index.
        axis : {index (0), rows(0)}, default 0
            Axis for the function to be applied on.
            For `Series` this parameter is unused and defaults to 0.
        copy : bool, default True
            this parameter is unused.

        Returns
        -------
        Series

        Examples
        --------
        >>> ser = pd.Series(["apple", "banana", "cauliflower"])
        >>> ser.set_axis(["A:", "B:", "C:"], axis="index")
        A:          apple
        B:         banana
        C:    cauliflower
        dtype: object

        >>> ser.set_axis([1000, 45, -99.23], axis=0)
         1000.00          apple
         45.00           banana
        -99.23      cauliflower
        dtype: object
        """
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

    def unstack(self, level=-1, fill_value=None):  # noqa: PR01, RT01, D200
        """
        Unstack, also known as pivot, Series with MultiIndex to produce DataFrame.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        from snowflake.snowpark.modin.pandas.dataframe import DataFrame

        result = DataFrame(
            query_compiler=self._query_compiler.unstack(level, fill_value)
        )

        return result.droplevel(0, axis=1) if result.columns.nlevels > 1 else result

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
        ErrorMessage.not_implemented()
        return self._to_pandas().plot

    @_create_operator_docstring(pandas.core.series.Series.pow, overwrite_existing=True)
    def pow(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return exponential power of series and `other`, element-wise (binary operator `pow`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().pow(other, level=level, fill_value=fill_value, axis=axis)

    def prod(
        self,
        axis=None,
        skipna=True,
        level=None,
        numeric_only=False,
        min_count=0,
        **kwargs,
    ):
        ErrorMessage.not_implemented()
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

    def ravel(self, order="C"):  # noqa: PR01, RT01, D200
        """
        Return the flattened underlying data as an ndarray.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        data = self._query_compiler.to_numpy().flatten(order=order)
        if isinstance(self.dtype, pandas.CategoricalDtype):
            data = pandas.Categorical(data, dtype=self.dtype)

        return data

    def reindex(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError("Only one positional argument ('index') is allowed")
            if "index" in kwargs:
                raise TypeError(
                    "'index' passed as both positional and keyword argument"
                )
            kwargs.update({"index": args[0]})
        index = kwargs.pop("index", None)
        method = kwargs.pop("method", None)
        level = kwargs.pop("level", None)
        copy = kwargs.pop("copy", True)
        limit = kwargs.pop("limit", None)
        tolerance = kwargs.pop("tolerance", None)
        fill_value = kwargs.pop("fill_value", None)
        if kwargs:
            raise TypeError(
                "reindex() got an unexpected keyword "
                + f'argument "{list(kwargs.keys())[0]}"'
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

        Function / dict values must be unique (1-to-1). Labels not contained in
        a dict / Series will be left as-is. Extra labels listed don't throw an
        error.

        Alternatively, change ``Series.name`` with a scalar value.

        Parameters
        ----------
        index : scalar, hashable sequence, dict-like or function optional
            Functions or dict-like are transformations to apply to
            the index.
            Scalar or hashable sequence-like will alter the ``Series.name``
            attribute.
        axis : {0 or 'index'}
            Unused. Parameter needed for compatibility with DataFrame.
        copy : bool, default True
            Also copy underlying data. copy has been ignored with Snowflake execution engine.
        inplace : bool, default False
            Whether to return a new Series. If True the value of copy is ignored.
        level : int or level name, default None
            In case of MultiIndex, only rename labels in the specified level.
        errors : {'ignore', 'raise'}, default 'ignore'
            If 'raise', raise `KeyError` when a `dict-like mapper` or
            `index` contains labels that are not present in the index being transformed.
            If 'ignore', existing keys will be renamed and extra keys will be ignored.

        Returns
        -------
        Series or None
            Series with index labels or name altered or None if ``inplace=True``.

        See Also
        --------
        DataFrame.rename : Corresponding DataFrame method.
        Series.rename_axis : Set the name of the axis.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3])
        >>> s
        0    1
        1    2
        2    3
        dtype: int64
        >>> s.rename("my_name")  # scalar, changes Series.name
        0    1
        1    2
        2    3
        Name: my_name, dtype: int64
        >>> s.rename(lambda x: x ** 2)  # function, changes labels
        0    1
        1    2
        4    3
        dtype: int8
        >>> s.rename({1: 3, 2: 5})  # mapping, changes labels
        0    1
        3    2
        5    3
        dtype: int64
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

    def repeat(self, repeats, axis=None):  # noqa: PR01, RT01, D200
        """
        Repeat elements of a Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
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
        Generate a new DataFrame or Series with the index reset.

        This is useful when the index needs to be treated as a column, or
        when the index is meaningless and needs to be reset to the default
        before another operation.

        Parameters
        ----------
        level : int, str, tuple, or list, default optional
            For a Series with a MultiIndex, only remove the specified levels
            from the index. Removes all levels by default.
        drop : bool, default False
            Just reset the index, without inserting it as a column in
            the new DataFrame.
        name : object, optional
            The name to use for the column containing the original Series
            values. Uses ``self.name`` by default. This argument is ignored
            when `drop` is True.
        inplace : bool, default False
            Modify the Series in place (do not create a new object).
        allow_duplicates : bool, default False
            Allow duplicate column labels to be created.

        Returns
        -------
        Series or DataFrame or None
            When `drop` is False (the default), a DataFrame is returned.
            The newly created columns will come first in the DataFrame,
            followed by the original Series values.
            When `drop` is True, a `Series` is returned.
            In either case, if ``inplace=True``, no value is returned.

        See Also
        --------
        DataFrame.reset_index: Analogous function for DataFrame.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3, 4], name='foo',
        ...               index=pd.Index(['a', 'b', 'c', 'd'], name='idx'))

        Generate a DataFrame with default index.

        >>> s.reset_index()
          idx  foo
        0   a    1
        1   b    2
        2   c    3
        3   d    4

        To specify the name of the new column use `name`.

        >>> s.reset_index(name='values')
          idx  values
        0   a       1
        1   b       2
        2   c       3
        3   d       4

        To generate a new Series with the default set `drop` to True.

        >>> s.reset_index(drop=True)
        0    1
        1    2
        2    3
        3    4
        Name: foo, dtype: int64

        To update the Series in place, without generating a new one
        set `inplace` to True. Note that it also requires ``drop=True``.

        >>> s.reset_index(inplace=True, drop=True)
        >>> s
        0    1
        1    2
        2    3
        3    4
        Name: foo, dtype: int64

        The `level` parameter is interesting for Series with a multi-level
        index.

        >>> arrays = [np.array(['bar', 'bar', 'baz', 'baz']),
        ...           np.array(['one', 'two', 'one', 'two'])]
        >>> s2 = pd.Series(
        ...     range(4), name='foo',
        ...     index=pd.MultiIndex.from_arrays(arrays,
        ...                                     names=['a', 'b']))

        To remove a specific level from the Index, use `level`.

        >>> s2.reset_index(level='a')  # doctest: +NORMALIZE_WHITESPACE
               a  foo
        b
        one  bar    0
        two  bar    1
        one  baz    2
        two  baz    3

        If `level` is not set, all levels are removed from the Index.

        >>> s2.reset_index()
             a    b  foo
        0  bar  one    0
        1  bar  two    1
        2  baz  one    2
        3  baz  two    3
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

    def rdivmod(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return integer division and modulo of series and `other`, element-wise (binary operator `rdivmod`).

        not yet implemented
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()

    @_create_operator_docstring(
        pandas.core.series.Series.rfloordiv, overwrite_existing=True
    )
    def rfloordiv(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return integer division of series and `other`, element-wise (binary operator `rfloordiv`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rfloordiv(other, level=level, fill_value=fill_value, axis=axis)

    @_create_operator_docstring(pandas.core.series.Series.rmod, overwrite_existing=True)
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

    @_create_operator_docstring(pandas.core.series.Series.rpow, overwrite_existing=True)
    def rpow(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return exponential power of series and `other`, element-wise (binary operator `rpow`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rpow(other, level=level, fill_value=fill_value, axis=axis)

    @_create_operator_docstring(pandas.core.series.Series.rsub, overwrite_existing=True)
    def rsub(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return subtraction of series and `other`, element-wise (binary operator `rsub`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().rsub(other, level=level, fill_value=fill_value, axis=axis)

    @_create_operator_docstring(
        pandas.core.series.Series.rtruediv, overwrite_existing=True
    )
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

        Parameters
        ----------
        q: float or array-like of float, default 0.5
            Value between 0 <= q <= 1, the quantile(s) to compute.
            Currently unsupported if q is a Snowpandas DataFrame or Series.
        interpolation: {"linear", "lower", "higher", "midpoint", "nearest"}, default "linear"
            Specifies the interpolation method to use if a quantile lies between two data points
            *i* and *j*:

            * linear: *i* + (*j* - *i*) * *fraction*, where *fraction* is the fractional part of the
              index surrounded by *i* and *j*.
            * lower: *i*.
            * higher: *j*.
            * nearest: *i* or *j*, whichever is nearest.
            * midpoint: (*i* + *j*) / 2.

        Returns
        -------
        float or Series
            If ``q`` is an array, a Series will be returned where the index is ``q`` and the values
            are the quantiles.
            If ``q`` is a float, the float value of that quantile will be returned.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3, 4])

        With a scalar q:

        >>> s.quantile(.5)
        2.5

        With a list q:

        >>> s.quantile([.25, .5, .75]) # doctest: +NORMALIZE_WHITESPACE
        0.25    1.75
        0.50    2.50
        0.75    3.25
        dtype: float64

        Values considered NaN do not affect the result:

        >>> s = pd.Series([None, 0, 25, 50, 75, 100, np.nan])
        >>> s.quantile([0, 0.25, 0.5, 0.75, 1]) # doctest: +NORMALIZE_WHITESPACE
        0.00      0.0
        0.25     25.0
        0.50     50.0
        0.75     75.0
        1.00    100.0
        dtype: float64
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().quantile(
            q=q,
            axis=0,
            numeric_only=False,
            interpolation=interpolation,
            method="single",
        )

    def reorder_levels(self, order):  # noqa: PR01, RT01, D200
        """
        Rearrange index levels using input order.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
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
        """
        Replace values given in `to_replace` with `value`.

        Values of the DataFrame are replaced with other values dynamically.
        This differs from updating with ``.loc`` or ``.iloc``, which require
        you to specify a location to update with some value.

        Parameters
        ----------
        to_replace : str, regex, list, dict, Series, int, float, or None
            How to find the values that will be replaced.

            * numeric, str or regex:

                - numeric: numeric values equal to `to_replace` will be
                  replaced with `value`
                - str: string exactly matching `to_replace` will be replaced
                  with `value`
                - regex: regexs matching `to_replace` will be replaced with
                  `value`

            * list of str, regex, or numeric:

                - First, if `to_replace` and `value` are both lists, they
                  **must** be the same length.
                - Second, if ``regex=True`` then all the strings in **both**
                  lists will be interpreted as regexs otherwise they will match
                  directly. This doesn't matter much for `value` since there
                  are only a few possible substitution regexes you can use.
                - str, regex and numeric rules apply as above.

            * dict:

                - Dicts can be used to specify different replacement values
                  for different existing values. For example,
                  ``{{'a': 'b', 'y': 'z'}}`` replaces the value 'a' with 'b' and
                  'y' with 'z'. To use a dict in this way, the optional `value`
                  parameter should not be given.
                - For a DataFrame a dict can specify that different values
                  should be replaced in different columns. For example,
                  ``{{'a': 1, 'b': 'z'}}`` looks for the value 1 in column 'a'
                  and the value 'z' in column 'b' and replaces these values
                  with whatever is specified in `value`. The `value` parameter
                  should not be ``None`` in this case. You can treat this as a
                  special case of passing two lists except that you are
                  specifying the column to search in.
                - For a DataFrame nested dictionaries, e.g.,
                  ``{{'a': {{'b': np.nan}}}}``, are read as follows: look in column
                  'a' for the value 'b' and replace it with NaN. The optional `value`
                  parameter should not be specified to use a nested dict in this
                  way. You can nest regular expressions as well. Note that
                  column names (the top-level dictionary keys in a nested
                  dictionary) **cannot** be regular expressions.

            * None:

                - This means that the `regex` argument must be a string,
                  compiled regular expression, or list, dict, ndarray or
                  Series of such elements. If `value` is also ``None`` then
                  this **must** be a nested dictionary or Series.

            See the examples section for examples of each of these.
        value : scalar, dict, list, str, regex, default None
            Value to replace any values matching `to_replace` with.
            For a DataFrame a dict of values can be used to specify which
            value to use for each column (columns not in the dict will not be
            filled). Regular expressions, strings and lists or dicts of such
            objects are also allowed.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
        limit : int, default None
            Maximum size gap to forward or backward fill.
            This parameter is not supported.
        regex : bool or same types as `to_replace`, default False
            Whether to interpret `to_replace` and/or `value` as regular
            expressions. Alternatively, this could be a regular expression or a
            list, dict, or array of regular expressions in which case
            `to_replace` must be ``None``.
        method : {{'pad', 'ffill', 'bfill'}}
            The method to use when for replacement, when `to_replace` is a
            scalar, list or tuple and `value` is ``None``.
            This parameter is not supported.

        Returns
        -------
        DataFrame
            DataFrame Object after replacement if inplace=False, None otherwise.

        Raises
        ------
        AssertionError
            * If `regex` is not a ``bool`` and `to_replace` is not ``None``.

        TypeError
            * If `to_replace` is not a scalar, array-like, ``dict``, or ``None``
            * If `to_replace` is a ``dict`` and `value` is not a ``list``,
              ``dict``, ``ndarray``, or ``Series``
            * If `to_replace` is ``None`` and `regex` is not compilable
              into a regular expression or is a list, dict, ndarray, or
              Series.
            * When replacing multiple ``bool`` or ``datetime64`` objects and
              the arguments to `to_replace` does not match the type of the
              value being replaced

        ValueError
            * If a ``list`` or an ``ndarray`` is passed to `to_replace` and
              `value` but they are not the same length.

        NotImplementedError
            * If ``method`` or ``limit`` is provided.

        Notes
        -----
        * Regex substitution is performed under the hood using snowflake backend.
          which supports POSIX ERE syntax for regular expressions. Please check usage
          notes for details.
          https://docs.snowflake.com/en/sql-reference/functions-regexp#general-usage-notes
        * Regular expressions only replace string values. If a regular expression is
          created to match floating point numbers, it will only match string data not
          numeric data.
        * This method has *a lot* of options. You are encouraged to experiment
          and play with this method to gain intuition about how it works.

        Examples
        --------

        **Scalar `to_replace` and `value`**

        >>> s = pd.Series([1, 2, 3, 4, 5])
        >>> s.replace(1, 5)
        0    5
        1    2
        2    3
        3    4
        4    5
        dtype: int64

        **dict-like `to_replace`**

        >>> s.replace({1: 10, 2: 100})
        0     10
        1    100
        2      3
        3      4
        4      5
        dtype: int64

        **Regular expression `to_replace`**

        >>> s = pd.Series(['bat', 'foo', 'bait'])
        >>> s.replace(to_replace=r'^ba.$', value='new', regex=True)
        0     new
        1     foo
        2    bait
        dtype: object

        >>> s.replace(regex=r'^ba.$', value='new')
        0     new
        1     foo
        2    bait
        dtype: object

        >>> s.replace(regex={r'^ba.$': 'new', 'foo': 'xyz'})
        0     new
        1     xyz
        2    bait
        dtype: object

        >>> s.replace(regex=[r'^ba.$', 'foo'], value='new')
        0     new
        1     new
        2    bait
        dtype: object

        Compare the behavior of ``s.replace({{'a': None}})`` and
        ``s.replace('a', None)`` to understand the peculiarities
        of the `to_replace` parameter:

        >>> s = pd.Series([10, 'a', 'a', 'b', 'a'])

        When one uses a dict as the `to_replace` value, it is like the
        value(s) in the dict are equal to the `value` parameter.
        ``s.replace({{'a': None}})`` is equivalent to
        ``s.replace(to_replace={{'a': None}}, value=None, method=None)``:

        >>> s.replace({'a': None})
        0      10
        1    None
        2    None
        3       b
        4    None
        dtype: object

        On the other hand, if ``None`` is explicitly passed for ``value``, it will
        also be respected:

        >>> s.replace('a', None)
        0      10
        1    None
        2    None
        3       b
        4    None
        dtype: object
        """
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

    def searchsorted(self, value, side="left", sorter=None):  # noqa: PR01, RT01, D200
        """
        Find indices where elements should be inserted to maintain order.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
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

        Sort a Series in ascending or descending order by some
        criterion.

        Parameters
        ----------
        axis : {0 or 'index'}
            Unused. Parameter needed for compatibility with DataFrame.
        ascending : bool or list of bools, default True
            If True, sort values in ascending order, otherwise descending.
        inplace : bool, default False
            If True, perform operation in-place.
        kind : {'quicksort', 'mergesort', 'heapsort', 'stable'} default 'None'
            Choice of sorting algorithm. By default, Snowpark Pandaas performs
            unstable sort. Please use 'stable' to perform stable sort. Other choices
            'quicksort', 'mergesort' and 'heapsort' are ignored.
        na_position : {'first' or 'last'}, default 'last'
            Argument 'first' puts NaNs at the beginning, 'last' puts NaNs at
            the end.
        ignore_index : bool, default False
            If True, the resulting axis will be labeled 0, 1, …, n - 1.
        key : callable, optional
            If not None, apply the key function to the series values
            before sorting. This is similar to the `key` argument in the
            builtin :meth:`sorted` function, with the notable difference that
            this `key` function should be *vectorized*. It should expect a
            ``Series`` and return an array-like.

        Returns
        -------
        Series or None
            Series ordered by values or None if ``inplace=True``.

        Notes
        -----
        Snowpark pandas API doesn't currently support distributed computation of
        sort_values when 'key' argument is provided.

        See Also
        --------
        Series.sort_index : Sort by the Series indices.
        DataFrame.sort_values : Sort DataFrame by the values along either axis.
        DataFrame.sort_index : Sort DataFrame by indices.

        Examples
        --------
        >>> s = pd.Series([np.nan, 1, 3, 10, 5])
        >>> s
        0     NaN
        1     1.0
        2     3.0
        3    10.0
        4     5.0
        dtype: float64

        Sort values ascending order (default behaviour)

        >>> s.sort_values(ascending=True)
        1     1.0
        2     3.0
        4     5.0
        3    10.0
        0     NaN
        dtype: float64

        Sort values descending order

        >>> s.sort_values(ascending=False)
        3    10.0
        4     5.0
        2     3.0
        1     1.0
        0     NaN
        dtype: float64

        Sort values inplace

        >>> s.sort_values(ascending=False, inplace=True)
        >>> s
        3    10.0
        4     5.0
        2     3.0
        1     1.0
        0     NaN
        dtype: float64

        Sort values putting NAs first

        >>> s.sort_values(na_position='first')
        0     NaN
        1     1.0
        2     3.0
        4     5.0
        3    10.0
        dtype: float64

        Sort a series of strings

        >>> s = pd.Series(['z', 'b', 'd', 'a', 'c'])
        >>> s
        0    z
        1    b
        2    d
        3    a
        4    c
        dtype: object

        >>> s.sort_values()
        3    a
        1    b
        4    c
        2    d
        0    z
        dtype: object

        Sort using a key function. Your `key` function will be
        given the ``Series`` of values and should return an array-like.

        >>> s = pd.Series(['a', 'B', 'c', 'D', 'e'])
        >>> s.sort_values()
        1    B
        3    D
        0    a
        2    c
        4    e
        dtype: object
        >>> s.sort_values(key=lambda x: x.str.lower())
        0    a
        1    B
        2    c
        3    D
        4    e
        dtype: object

        NumPy ufuncs work well here. For example, we can
        sort by the ``sin`` of the value

        >>> s = pd.Series([-4, -2, 0, 2, 4])
        >>> s.sort_values(key=np.sin)
        1   -2
        4    4
        2    0
        0   -4
        3    2
        dtype: int8

        More complicated user-defined functions can be used,
        as long as they expect a Series and return an array-like

        >>> s.sort_values(key=lambda x: (np.tan(x.cumsum())))
        0   -4
        3    2
        4    4
        1   -2
        2    0
        dtype: int8
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

        Series or DataFrames with a single element are squeezed to a scalar.
        DataFrames with a single column or a single row are squeezed to a
        Series. Otherwise, the object is unchanged.

        This method is most useful when you don't know if your
        object is a Series or DataFrame, but you do know it has just a single
        column. In that case you can safely call `squeeze` to ensure you have a
        Series.

        Parameters
        ----------
        axis : {0 or 'index', 1 or 'columns', None}, default None
            A specific axis to squeeze. By default, all length-1 axes are
            squeezed. For `Series` this parameter is unused and defaults to `None`.

        Returns
        -------
        DataFrame, Series, or scalar
            The projection after squeezing `axis` or all the axes.

        See Also
        --------
        Series.iloc : Integer-location based indexing for selecting scalars.
        DataFrame.iloc : Integer-location based indexing for selecting Series.
        Series.to_frame : Inverse of DataFrame.squeeze for a
            single-column DataFrame.

        Examples
        --------
        >>> primes = pd.Series([2, 3, 5, 7])

        Slicing might produce a Series with a single value:

        >>> even_primes = primes[primes % 2 == 0]   # doctest: +SKIP
        >>> even_primes   # doctest: +SKIP
        0    2
        dtype: int64

        >>> even_primes.squeeze()   # doctest: +SKIP
        2

        Squeezing objects with more than one value in every axis does nothing:

        >>> odd_primes = primes[primes % 2 == 1]   # doctest: +SKIP
        >>> odd_primes   # doctest: +SKIP
        1    3
        2    5
        3    7
        dtype: int64

        >>> odd_primes.squeeze()   # doctest: +SKIP
        1    3
        2    5
        3    7
        dtype: int64

        Squeezing is even more effective when used with DataFrames.

        >>> df = pd.DataFrame([[1, 2], [3, 4]], columns=['a', 'b'])
        >>> df
           a  b
        0  1  2
        1  3  4

        Slicing a single column will produce a DataFrame with the columns
        having only one value:

        >>> df_a = df[['a']]
        >>> df_a
           a
        0  1
        1  3

        So the columns can be squeezed down, resulting in a Series:

        >>> df_a.squeeze('columns')
        0    1
        1    3
        Name: a, dtype: int64

        Slicing a single row from a single column will produce a single
        scalar DataFrame:

        >>> df_0a = df.loc[df.index < 1, ['a']]
        >>> df_0a
           a
        0  1

        Squeezing the rows produces a single scalar Series:

        >>> df_0a.squeeze('rows')
        a    1
        Name: 0, dtype: int64

        Squeezing all axes will project directly into a scalar:

        >>> df_0a.squeeze()
        1
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if axis is not None:
            # Validate `axis`
            pandas.Series._get_axis_number(axis)
        if len(self) == 1:
            return self._reduce_dimension(self._query_compiler)
        else:
            return self.copy()

    @_create_operator_docstring(pandas.core.series.Series.sub, overwrite_existing=True)
    def sub(self, other, level=None, fill_value=None, axis=0):  # noqa: PR01, RT01, D200
        """
        Return subtraction of Series and `other`, element-wise (binary operator `sub`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().sub(other, level=level, fill_value=fill_value, axis=axis)

    subtract = sub

    def swaplevel(self, i=-2, j=-1, copy=True):  # noqa: PR01, RT01, D200
        """
        Swap levels `i` and `j` in a `MultiIndex`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas("swaplevel", i=i, j=j, copy=copy)

    def take(
        self,
        indices: list | AnyArrayLike,
        axis: Axis = 0,
        **kwargs,
    ):
        """
        Return the elements in the given *positional* indices along an axis.

        This means that we are not indexing according to actual values in
        the index attribute of the object. We are indexing according to the
        actual position of the element in the object.

        Parameters
        ----------
        indices : array-like
            An array of ints indicating which positions to take.
        axis : {0 or 'index', 1 or 'columns', None}, default 0
            The axis on which to select elements. ``0`` means that we are
            selecting rows, ``1`` means that we are selecting columns.
            For `Series` this parameter is unused and defaults to 0.
        **kwargs
            For compatibility with :meth:`numpy.take`. Has no effect on the
            output.

        Returns
        -------
        same type as caller
            An array-like containing the elements taken from the object.

        See Also
        --------
        Series.take : Take a subset of a Series by the given positional indices.
        DataFrame.loc : Select a subset of a DataFrame by labels.
        DataFrame.iloc : Select a subset of a DataFrame by positions.

        Examples
        --------
        >>> ser = pd.Series([-1, 5, 6, 2, 4])
        >>> ser
        0   -1
        1    5
        2    6
        3    2
        4    4
        dtype: int64

        Take elements at positions 0 and 3 along the axis 0 (default).

        >>> ser.take([0, 3])
        0   -1
        3    2
        dtype: int64


        For `Series` axis parameter is unused and defaults to 0.

        >>> ser.take([0, 3], axis=1)
        0   -1
        3    2
        dtype: int64

        We may take elements using negative integers for positive indices,
        starting from the end of the object, just like with Python lists.

        >>> ser.take([-1, -2])
        4    4
        3    2
        dtype: int64

        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().take(indices, axis=0, **kwargs)

    def to_dict(self, into: type[dict] = dict) -> dict:
        """
        Convert Series to {label -> value} dict or dict-like object.
        Note that this method will pull the data to the client side.

        Parameters
        ----------
        into : class, default dict
            The collections.abc.Mapping subclass to use as the return
            object. Can be the actual class or an empty
            instance of the mapping type you want.  If you want a
            collections.defaultdict, you must pass it initialized.

        Returns
        -------
        collections.abc.Mapping
            Key-value representation of Series.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3, 4])
        >>> s.to_dict()
        {0: 1, 1: 2, 2: 3, 3: 4}
        >>> from collections import OrderedDict, defaultdict
        >>> s.to_dict(OrderedDict)
        OrderedDict([(0, 1), (1, 2), (2, 3), (3, 4)])
        >>> dd = defaultdict(list)
        >>> s.to_dict(dd)
        defaultdict(<class 'list'>, {0: 1, 1: 2, 2: 3, 3: 4})
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
        A NumPy ndarray representing the values in this Series or Index.

        Parameters
        ----------
        dtype : str or numpy.dtype, optional
            The dtype to pass to :meth:`numpy.asarray`.
        copy : bool, default False
            This argument is ignored in Snowflake backend. The data from Snowflake
            will be retrieved into the client, and a numpy array containing this
            data will be returned.
        na_value : Any, optional
            The value to use for missing values. The default value depends
            on `dtype` and the type of the array.
        **kwargs
            Additional keywords passed through to the ``to_numpy`` method
            of the underlying array (for extension arrays).

        Returns
        -------
        numpy.ndarray
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
    def to_period(self, freq=None, copy=True):  # noqa: PR01, RT01, D200
        """
        Cast to PeriodArray/Index at a particular frequency.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas("to_period", freq=freq, copy=copy)

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
        ErrorMessage.not_implemented()
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
    def to_timestamp(self, freq=None, how="start", copy=True):  # noqa: PR01, RT01, D200
        """
        Cast to DatetimeIndex of Timestamps, at beginning of period.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas("to_timestamp", freq=freq, how=how, copy=copy)

    def transpose(self, *args, **kwargs):  # noqa: PR01, RT01, D200
        """
        Return the transpose, which is by definition `self`.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return self

    T = property(transpose)

    @_create_operator_docstring(
        pandas.core.series.Series.truediv, overwrite_existing=True
    )
    def truediv(
        self, other, level=None, fill_value=None, axis=0
    ):  # noqa: PR01, RT01, D200
        """
        Return floating division of series and `other`, element-wise (binary operator `truediv`).
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().truediv(other, level=level, fill_value=fill_value, axis=axis)

    div = divide = truediv

    def truncate(
        self, before=None, after=None, axis=None, copy=True
    ):  # noqa: PR01, RT01, D200
        """
        Truncate a Series before and after some index value.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
        return self._default_to_pandas(
            pandas.Series.truncate, before=before, after=after, axis=axis, copy=copy
        )

    def unique(self):
        """
        Return unique values of Series object.

        Uniques are returned in order of appearance. Hash table-based unique,
        therefore does NOT sort.

        Returns
        -------
        ndarray
            The unique values returned as a NumPy array. See Notes.

        See Also
        --------
        Series.drop_duplicates : Return Series with duplicate values removed.
        unique : Top-level unique method for any 1-d array-like object.
        Index.unique : Return Index with unique values from an Index object.

        Notes
        -----
        Returns the unique values as a NumPy array. This includes

            * Datetime with Timezone
            * IntegerNA

        See Examples section.

        Examples
        --------
        >>> pd.Series([2, 1, 3, 3], name='A').unique()
        array([2, 1, 3])

        >>> pd.Series([pd.Timestamp('2016-01-01', tz='US/Eastern')
        ...            for _ in range(3)]).unique()
        array([Timestamp('2015-12-31 21:00:00-0800', tz='America/Los_Angeles')],
              dtype=object)

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

        The resulting object will be in descending order so that the
        first element is the most frequently-occurring element.
        Excludes NA values by default.

        Parameters
        ----------
        normalize : bool, default False
            If True then the object returned will contain the relative
            frequencies of the unique values. Being different from native pandas,
            Snowpark pandas will return a Series with `decimal.Decimal` values.
        sort : bool, default True
            Sort by frequencies when True. Preserve the order of the data when False.
            When there is a tie between counts, the order is still deterministic, but
            may be different from the result from native pandas.
        ascending : bool, default False
            Sort in ascending order.
        bins : int, optional
            Rather than count values, group them into half-open bins,
            a convenience for ``pd.cut``, only works with numeric data.
            This argument is not supported yet.
        dropna : bool, default True
            Don't include counts of NaN.

        Returns
        -------
        Series

        See Also
        --------
        Series.count: Number of non-NA elements in a Series.
        DataFrame.count: Number of non-NA elements in a DataFrame.
        DataFrame.value_counts: Equivalent method on DataFrames.

        Examples
        --------
        >>> s = pd.Series([3, 1, 2, 3, 4, np.nan])
        >>> s.value_counts()
        3.0    2
        1.0    1
        2.0    1
        4.0    1
        Name: count, dtype: int64

        With `normalize` set to `True`, returns the relative frequency by
        dividing all values by the sum of values.

        >>> s.value_counts(normalize=True)
        3.0    0.4
        1.0    0.2
        2.0    0.2
        4.0    0.2
        Name: proportion, dtype: float64

        **dropna**

        With `dropna` set to `False` we can also see NaN index values.

        >>> s.value_counts(dropna=False)
        3.0    2
        1.0    1
        2.0    1
        4.0    1
        NaN    1
        Name: count, dtype: int64
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

    def view(self, dtype=None):  # noqa: PR01, RT01, D200
        """
        Create a new view of the Series.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
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

        Args:
            cond: bool Series/DataFrame, array-like, or callable
                Where cond is True, keep the original value. Where False, replace with corresponding value from other.
                If cond is callable, it is computed on the Series/DataFrame and should return boolean Series/DataFrame
                or array. The callable must not change input Series/DataFrame (though pandas doesn’t check it).

            other: scalar, Series/DataFrame, or callable
                Entries where cond is False are replaced with corresponding value from other. If other is callable,
                it is computed on the Series/DataFrame and should return scalar or Series/DataFrame. The callable must
                not change input Series/DataFrame (though pandas doesn’t check it). If not specified, entries will be
                filled with the corresponding NULL value (np.nan for numpy dtypes, pd.NA for extension dtypes).

            inplace: bool, default False
                Whether to perform the operation in place on the data.

            axis: int, default None
                Alignment axis if needed. For Series this parameter is unused and defaults to 0.

            level: int, default None
                Alignment level if needed.

        Returns:
            Same type as caller or None if inplace=True.

        Notes:
            The where method is an application of the if-then idiom. For each element in the calling DataFrame, if cond
            is True the element is used; otherwise the corresponding element from the DataFrame other is used. If the
            axis of other does not align with axis of cond Series/DataFrame, the misaligned index positions will be
            filled with False.

            The signature for DataFrame.where() differs from numpy.where(). Roughly df1.where(m, df2) is equivalent to
            np.where(m, df1, df2).

            For further details and examples see the where documentation in indexing.

            The dtype of the object takes precedence. The fill value is casted to the object’s dtype, if this can be
            done losslessly.

        Examples::
        >>> s = pd.Series(range(5))
        >>> s.where(s > 0)  # doctest: +NORMALIZE_WHITESPACE
        0    NaN
        1    1.0
        2    2.0
        3    3.0
        4    4.0
        dtype: float64

        >>> s = pd.Series(range(5))
        >>> t = pd.Series([True, False])
        >>> s.where(t, 99)  # doctest: +NORMALIZE_WHITESPACE
        0     0
        1    99
        2    99
        3    99
        4    99
        dtype: int64

        >>> s.where(s > 1, 10)  # doctest: +NORMALIZE_WHITESPACE
        0    10
        1    10
        2    2
        3    3
        4    4
        dtype: int64
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        return super().where(
            cond,
            other=other,
            inplace=inplace,
            axis=axis,
            level=level,
        )

    def xs(
        self, key, axis=0, level=None, drop_level=True
    ):  # pragma: no cover # noqa: PR01, D200
        """
        Return cross-section from the Series/DataFrame.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented("")

    @property
    def attrs(self):  # noqa: RT01, D200
        """
        Return dictionary of global attributes of this dataset.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions

        def attrs(df):
            return df.attrs

        return self._default_to_pandas(attrs)

    @property
    def array(self):  # noqa: RT01, D200
        """
        Return the ExtensionArray of the data backing this Series or Index.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()

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

        from .series_utils import DatetimeProperties

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
        Detect missing values for an array-like object.

        This function takes a scalar or array-like object and indicates whether values are missing (NaN in numeric
        arrays, None or NaN in object arrays, NaT in datetimelike).

        Parameters
        ----------
        obj : scalar or array-like
                Object to check for null or missing values.

        Returns
        -------
        bool or array-like of bool
            For scalar input, returns a scalar boolean. For array input, returns an array of boolean indicating whether
            each corresponding element is missing.

        Examples
        --------
        >>> df = pd.DataFrame([['ant', 'bee', 'cat'], ['dog', None, 'fly']])
        >>> df
             0     1    2
        0  ant   bee  cat
        1  dog  None  fly
        >>> df.isna()
               0      1      2
        0  False  False  False
        1  False   True  False
        >>> df.isnull()
               0      1      2
        0  False  False  False
        1  False   True  False
        """
        return super().isna()

    def isnull(self):
        """
        Detect missing values for an array-like object.

        This function takes a scalar or array-like object and indicates whether values are missing (NaN in numeric
        arrays, None or NaN in object arrays, NaT in datetimelike).

        Parameters
        ----------
        obj : scalar or array-like
                Object to check for null or missing values.

        Returns
        -------
        bool or array-like of bool
            For scalar input, returns a scalar boolean. For array input, returns an array of boolean indicating whether
            each corresponding element is missing.

        Examples
        --------
        >>> df = pd.DataFrame([['ant', 'bee', 'cat'], ['dog', None, 'fly']])
        >>> df
             0     1    2
        0  ant   bee  cat
        1  dog  None  fly
        >>> df.isna()
               0      1      2
        0  False  False  False
        1  False   True  False
        >>> df.isnull()
               0      1      2
        0  False  False  False
        1  False   True  False
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

    @property
    def nbytes(self):  # noqa: RT01, D200
        """
        Return the number of bytes in the underlying data.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()
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
        periods: int = 1,
        freq=None,
        axis: Axis = 0,
        fill_value: Hashable = no_default,
    ):
        """
        Shift data by desired number of periods and replace columns with fill_value (default: None).

        Snowpark pandas does not support `freq` currently.

        The axis parameter is unused, and defaults to 0.

        Parameters
        ----------
        periods : int
            Number of periods to shift. Can be positive or negative.
        freq : not supported, default None
        axis : {0 or 'index', 1 or 'columns', None}, default None
            Shift direction. This parameter is unused and expects 0, 'index' or None.
        fill_value : object, optional
            The scalar value to use for newly introduced missing values.
            the default depends on the dtype of `self`.
            For numeric data, ``np.nan`` is used.
            For datetime, timedelta, or period data, etc. :attr:`NaT` is used.
            For extension dtypes, ``self.dtype.na_value`` is used.

        Returns
        -------
        Series
            Copy of input object, shifted.

        Examples
        --------
        >>> s = pd.Series([10, 20, 15, 30, 45],
        ...                   index=pd.date_range("2020-01-01", "2020-01-05"))
        >>> s
        2020-01-01    10
        2020-01-02    20
        2020-01-03    15
        2020-01-04    30
        2020-01-05    45
        Freq: None, dtype: int64

        >>> s.shift(periods=3)
        2020-01-01     NaN
        2020-01-02     NaN
        2020-01-03     NaN
        2020-01-04    10.0
        2020-01-05    20.0
        Freq: None, dtype: float64


        >>> s.shift(periods=-2)
        2020-01-01    15.0
        2020-01-02    30.0
        2020-01-03    45.0
        2020-01-04     NaN
        2020-01-05     NaN
        Freq: None, dtype: float64


        >>> s.shift(periods=3, fill_value=0)
        2020-01-01     0
        2020-01-02     0
        2020-01-03     0
        2020-01-04    10
        2020-01-05    20
        Freq: None, dtype: int64

        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        if axis == 1:
            # pandas compatible error.
            raise ValueError("No axis named 1 for object type Series")

        return super().shift(periods, freq, axis, fill_value)

    @property
    def str(self):  # noqa: RT01, D200
        """
        Vectorized string functions for Series and Index.
        """
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        current_dtype = self.dtype
        if not is_string_dtype(current_dtype):
            raise AttributeError("Can only use .str accessor with string values!")

        from .series_utils import StringMethods

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

    def __reduce__(self):
        # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
        ErrorMessage.not_implemented()  # pragma: no cover

        self._query_compiler.finalize()
        # if PersistentPickle.get():
        #    return self._inflate_full, (self._to_pandas(),)
        return self._inflate_light, (self._query_compiler, self.name)

    # Persistance support methods - END
