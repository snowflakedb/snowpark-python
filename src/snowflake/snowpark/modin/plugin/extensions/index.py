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

"""Module houses ``Index`` class, that is distributed version of ``pandas.Index``."""

from __future__ import annotations

import inspect
from functools import cached_property
from typing import Any, Callable, Hashable, Iterable, Iterator, Literal

import modin
import numpy as np
import pandas as native_pd
from modin.pandas.base import BasePandasDataset
from pandas import get_option
from pandas._libs import lib
from pandas._libs.lib import is_list_like, is_scalar
from pandas._typing import ArrayLike, DateTimeErrorChoices, DtypeObj, NaPosition, Scalar
from pandas.core.arrays import ExtensionArray
from pandas.core.dtypes.base import ExtensionDtype
from pandas.core.dtypes.common import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_timedelta64_dtype,
    pandas_dtype,
)
from pandas.core.dtypes.inference import is_hashable

from snowflake.snowpark.modin.pandas import DataFrame, Series
from snowflake.snowpark.modin.pandas.utils import try_convert_index_to_native
from snowflake.snowpark.modin.plugin._internal.telemetry import TelemetryMeta
from snowflake.snowpark.modin.plugin._internal.timestamp_utils import DateTimeOrigin
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    index_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.types import ArrayType

_CONSTRUCTOR_DEFAULTS = {
    "dtype": None,
    "copy": False,
    "name": None,
    "tupleize_cols": True,
}


class Index(metaclass=TelemetryMeta):

    # Equivalent index type in native pandas
    _NATIVE_INDEX_TYPE = native_pd.Index

    _comparables: list[str] = ["name"]

    def __new__(
        cls,
        data: ArrayLike | native_pd.Index | Series | None = None,
        dtype: str | np.dtype | ExtensionDtype | None = _CONSTRUCTOR_DEFAULTS["dtype"],
        copy: bool = _CONSTRUCTOR_DEFAULTS["copy"],
        name: object = _CONSTRUCTOR_DEFAULTS["name"],
        tupleize_cols: bool = _CONSTRUCTOR_DEFAULTS["tupleize_cols"],
        query_compiler: SnowflakeQueryCompiler = None,
    ) -> Index:
        """
        Override __new__ method to control new instance creation of Index.
        Depending on data type, it will create an Index or DatetimeIndex instance.

        Parameters
        ----------
        data : array-like (1-dimensional), pandas.Index, modin.pandas.Series, optional
        dtype : str, numpy.dtype, or ExtensionDtype, optional
            Data type for the output Index. If not specified, this will be
            inferred from `data`.
            See the :ref:`user guide <basics.dtypes>` for more usages.
        copy : bool, default False
            Copy input data.
        name : object
            Name to be stored in the index.
        tupleize_cols : bool (default: True)
            When True, attempt to create a MultiIndex if possible.
        query_compiler : SnowflakeQueryCompiler, optional
            A query compiler object to create the ``Index`` from.
        Returns
        -------
            New instance of Index or DatetimeIndex.
            DatetimeIndex object will be returned if the column/data have datetime type.
        """
        from snowflake.snowpark.modin.plugin.extensions.datetime_index import (
            DatetimeIndex,
        )
        from snowflake.snowpark.modin.plugin.extensions.timedelta_index import (
            TimedeltaIndex,
        )

        kwargs = {
            "dtype": dtype,
            "copy": copy,
            "name": name,
            "tupleize_cols": tupleize_cols,
        }
        query_compiler = cls._init_query_compiler(
            data, _CONSTRUCTOR_DEFAULTS, query_compiler, **kwargs
        )
        dtype = query_compiler.index_dtypes[0]
        if is_datetime64_any_dtype(dtype):
            return DatetimeIndex(query_compiler=query_compiler)
        if is_timedelta64_dtype(dtype):
            return TimedeltaIndex(query_compiler=query_compiler)
        index = object.__new__(cls)
        # Initialize the Index
        index._query_compiler = query_compiler
        # `_parent` keeps track of any Series or DataFrame that this Index is a part of.
        index._parent = None
        return index

    def __init__(
        self,
        data: ArrayLike | native_pd.Index | Series | None = None,
        dtype: str | np.dtype | ExtensionDtype | None = _CONSTRUCTOR_DEFAULTS["dtype"],
        copy: bool = _CONSTRUCTOR_DEFAULTS["copy"],
        name: object = _CONSTRUCTOR_DEFAULTS["name"],
        tupleize_cols: bool = _CONSTRUCTOR_DEFAULTS["tupleize_cols"],
        query_compiler: SnowflakeQueryCompiler = None,
    ) -> None:
        """
        Immutable sequence used for indexing and alignment.

        The basic object storing axis labels for all pandas objects.

        Parameters
        ----------
        data : array-like (1-dimensional), pandas.Index, modin.pandas.Series, optional
        dtype : str, numpy.dtype, or ExtensionDtype, optional
            Data type for the output Index. If not specified, this will be
            inferred from `data`.
            See the :ref:`user guide <basics.dtypes>` for more usages.
        copy : bool, default False
            Copy input data.
        name : object
            Name to be stored in the index.
        tupleize_cols : bool (default: True)
            When True, attempt to create a MultiIndex if possible.
        query_compiler : SnowflakeQueryCompiler, optional
            A query compiler object to create the ``Index`` from.
        Notes
        -----
        An Index instance can **only** contain hashable objects.
        An Index instance *cannot* hold numpy float16 dtype.

        Examples
        --------
        >>> pd.Index([1, 2, 3])
        Index([1, 2, 3], dtype='int64')

        >>> pd.Index(list('abc'))
        Index(['a', 'b', 'c'], dtype='object')

        # Snowpark pandas only supports signed integers so cast to uint won't work
        >>> pd.Index([1, 2, 3], dtype="uint8")
        Index([1, 2, 3], dtype='int64')
        """
        # Index is already initialized in __new__ method. We keep this method only for
        # docstring generation.

    @classmethod
    def _init_query_compiler(
        cls,
        data: ArrayLike | native_pd.Index | Series | None,
        ctor_defaults: dict,
        query_compiler: SnowflakeQueryCompiler = None,
        **kwargs: Any,
    ) -> SnowflakeQueryCompiler:
        if query_compiler:
            # Raise warning if `data` is query compiler with non-default arguments.
            for arg_name, arg_value in kwargs.items():
                assert (
                    arg_value == ctor_defaults[arg_name]
                ), f"Non-default argument '{arg_name}={arg_value}' when constructing Index with query compiler"
        elif isinstance(data, BasePandasDataset):
            if data.ndim != 1:
                raise ValueError("Index data must be 1 - dimensional")
            series_has_no_name = data.name is None
            idx = (
                data.to_frame().set_index(0 if series_has_no_name else data.name).index
            )
            if series_has_no_name:
                idx.name = None
            query_compiler = idx._query_compiler
        elif isinstance(data, Index):
            query_compiler = data._query_compiler
        else:
            query_compiler = DataFrame(
                index=cls._NATIVE_INDEX_TYPE(data=data, **kwargs)
            )._query_compiler

        if len(query_compiler.columns):
            query_compiler = query_compiler.drop(columns=query_compiler.columns)
        return query_compiler

    def __getattr__(self, key: str) -> Any:
        """
        Return item identified by `key`.

        Parameters
        ----------
        key : str
            Key to get.

        Returns
        -------
        Any

        Notes
        -----
        This method also helps raise NotImplementedError for APIs out
        of current scope that are not implemented.
        """
        try:
            return object.__getattribute__(self, key)
        except AttributeError as err:
            if not key.startswith("_"):
                native_index = self._NATIVE_INDEX_TYPE([])
                if hasattr(native_index, key):
                    # Any methods that not supported by the current Index.py but exist in a
                    # native pandas index object should raise a not implemented error for now.
                    ErrorMessage.not_implemented(f"Index.{key} is not yet implemented")
            raise err

    def _binary_ops(self, method: str, other: Any) -> Index:
        if isinstance(other, Index):
            other = other.to_series().reset_index(drop=True)
        series = getattr(self.to_series().reset_index(drop=True), method)(other)
        qc = series._query_compiler
        qc = qc.set_index_from_columns(qc.columns, include_index=False)
        # Use base constructor to ensure that the correct type is returned.
        idx = Index(query_compiler=qc)
        idx.name = series.name
        return idx

    def _unary_ops(self, method: str) -> Index:
        return self.__constructor__(
            getattr(self.to_series().reset_index(drop=True), method)()
        )

    def __add__(self, other: Any) -> Index:
        return self._binary_ops("__add__", other)

    def __radd__(self, other: Any) -> Index:
        return self._binary_ops("__radd__", other)

    def __mul__(self, other: Any) -> Index:
        return self._binary_ops("__mul__", other)

    def __rmul__(self, other: Any) -> Index:
        return self._binary_ops("__rmul__", other)

    def __neg__(self) -> Index:
        return self._unary_ops("__neg__")

    def __sub__(self, other: Any) -> Index:
        return self._binary_ops("__sub__", other)

    def __rsub__(self, other: Any) -> Index:
        return self._binary_ops("__rsub__", other)

    def __truediv__(self, other: Any) -> Index:
        return self._binary_ops("__truediv__", other)

    def __rtruediv__(self, other: Any) -> Index:
        return self._binary_ops("__rtruediv__", other)

    def __floordiv__(self, other: Any) -> Index:
        return self._binary_ops("__floordiv__", other)

    def __rfloordiv__(self, other: Any) -> Index:
        return self._binary_ops("__rfloordiv__", other)

    def __pow__(self, other: Any) -> Index:
        return self._binary_ops("__pow__", other)

    def __rpow__(self, other: Any):
        return self._binary_ops("__rpow__", other)

    def __mod__(self, other: Any) -> Index:
        return self._binary_ops("__mod__", other)

    def __rmod__(self, other: Any):
        return self._binary_ops("__rmod__", other)

    def __eq__(self, other: Any) -> Index:
        return self._binary_ops("eq", other)

    def __ne__(self, other: Any) -> Index:
        return self._binary_ops("ne", other)

    def __ge__(self, other: Any) -> Index:
        return self._binary_ops("ge", other)

    def __gt__(self, other: Any) -> Index:
        return self._binary_ops("gt", other)

    def __le__(self, other: Any) -> Index:
        return self._binary_ops("le", other)

    def __lt__(self, other: Any) -> Index:
        return self._binary_ops("lt", other)

    def __or__(self, other: Any) -> Index:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __and__(self, other: Any) -> Index:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __xor__(self, other: Any) -> Index:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __lshift__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rshift__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rand__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __ror__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rxor__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rlshift__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def __rrshift__(self, n: int) -> int:
        ErrorMessage.not_implemented(
            f"Index.{inspect.currentframe().f_code.co_name} is not yet implemented"
        )

    def to_pandas(
        self,
        *,
        statement_params: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> native_pd.Index:
        """
        Convert Snowpark pandas Index to pandas Index.

        Args:
        statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
            pandas Index
                A native pandas Index representation of self
        """
        return self._query_compiler._modin_frame.index_columns_pandas_index(
            statement_params=statement_params, **kwargs
        )

    @cached_property
    def __constructor__(self):
        """
        Returns: Type of the instance.
        """
        return type(self)

    def _set_parent(self, parent: Series | DataFrame):
        """
        Set the parent object of the current Index to a given Series or DataFrame.
        """
        self._parent = parent

    @property
    def values(self) -> ArrayLike:
        """
        Return an array representing the data in the Index.

        Returns
        -------
        numpy.ndarray or ExtensionArray
            array representing the index data

        See Also
        --------
        Index.array : Reference to the underlying data.

        Examples
        --------
        For :class:`pd.Index`:

        >>> idx = pd.Index([1, 2, 3])
        >>> idx
        Index([1, 2, 3], dtype='int64')
        >>> idx.values
        array([1, 2, 3])
        """
        return self.to_pandas().values

    @property
    def is_monotonic_increasing(self) -> bool:
        """
        Return a boolean if the values are equal or increasing.

        Returns
        -------
        bool
            Whether the values are equal or increasing

        See Also
        --------
        Index.is_monotonic_decreasing : Check if the values are equal or decreasing

        Examples
        --------
        >>> pd.Index([1, 2, 3]).is_monotonic_increasing
        True
        >>> pd.Index([1, 2, 2]).is_monotonic_increasing
        True
        >>> pd.Index([1, 3, 2]).is_monotonic_increasing
        False
        """
        return self.to_series().is_monotonic_increasing

    @property
    def is_monotonic_decreasing(self) -> bool:
        """
        Return a boolean if the values are equal or decreasing.

        Returns
        -------
        bool
            Whether the values are equal or decreasing

        See Also
        --------
        Index.is_monotonic_increasing : Check if the values are equal or increasing

        Examples
        --------
        >>> pd.Index([3, 2, 1]).is_monotonic_decreasing
        True
        >>> pd.Index([3, 2, 2]).is_monotonic_decreasing
        True
        >>> pd.Index([3, 1, 2]).is_monotonic_decreasing
        False
        """
        return self.to_series().is_monotonic_decreasing

    @property
    def is_unique(self) -> bool:
        """
        Return if the index has unique values.

        Returns
        -------
        bool
            True if the index has all unique values, False otherwise.

        See Also
        --------
        Index.has_duplicates : Inverse method that checks if it has duplicate values.

        Examples
        --------
        >>> idx = pd.Index([1, 5, 7, 7])
        >>> idx.is_unique
        False

        >>> idx = pd.Index([1, 5, 7])
        >>> idx.is_unique
        True

        >>> idx = pd.Index(["Watermelon", "Orange", "Apple",
        ...                 "Watermelon"])
        >>> idx.is_unique
        False

        >>> idx = pd.Index(["Orange", "Apple",
        ...                 "Watermelon"])
        >>> idx.is_unique
        True
        """
        return self._query_compiler._modin_frame.has_unique_index()

    @property
    def has_duplicates(self) -> bool:
        """
        Check if the Index has duplicate values.

        Returns
        -------
        bool
            True if the index has duplicate values, False otherwise.

        See Also
        --------
        Index.is_unique : Inverse method that checks if it has unique values.

        Examples
        --------
        >>> idx = pd.Index([1, 5, 7, 7])
        >>> idx.has_duplicates
        True

        >>> idx = pd.Index([1, 5, 7])
        >>> idx.has_duplicates
        False

        >>> idx = pd.Index(["Watermelon", "Orange", "Apple",
        ...                 "Watermelon"])
        >>> idx.has_duplicates
        True

        >>> idx = pd.Index(["Orange", "Apple",
        ...                 "Watermelon"])
        >>> idx.has_duplicates
        False
        """
        return not self.is_unique

    def unique(self, level: Hashable | None = None) -> Index:
        """
        Return unique values in the index.

        Unique values are returned in order of appearance, this does NOT sort.

        Parameters
        ----------
        level : int or hashable, optional
            Only return values from specified level (for MultiIndex).
            If int, gets the level by integer position, else by level name.

        Returns
        -------
        Index

        See Also
        --------
        unique : Numpy array of unique values in that column.
        Series.unique : Return unique values of a Series object.

        Examples
        --------
        >>> idx = pd.Index([1, 1, 2, 3, 3])
        >>> idx.unique()
        Index([1, 2, 3], dtype='int64')
        """
        if level not in [None, 0, -1]:
            raise IndexError(
                f"Too many levels: Index has only 1 level, {level} is not a valid level number."
            )
        return self.__constructor__(
            query_compiler=self._query_compiler.groupby_agg(
                by=self._query_compiler.get_index_names(axis=0),
                agg_func={},
                axis=0,
                groupby_kwargs={"sort": False, "as_index": True, "dropna": False},
                agg_args=[],
                agg_kwargs={},
            )
        )

    @property
    def dtype(self) -> DtypeObj:
        """
        Get the dtype object of the underlying data.

        Returns
        -------
        DtypeObj
            The dtype of the underlying data.

        Examples
        --------
        >>> idx = pd.Index([1, 2, 3])
        >>> idx
        Index([1, 2, 3], dtype='int64')
        >>> idx.dtype
        dtype('int64')
        """
        return self._query_compiler.index_dtypes[0]

    @property
    def shape(self) -> tuple:
        """
        Get a tuple of the shape of the underlying data.

        Returns
        -------
        tuple
            A tuple representing the shape of self

        Examples
        --------
        >>> idx = pd.Index([1, 2, 3])
        >>> idx
        Index([1, 2, 3], dtype='int64')
        >>> idx.shape
        (3,)
        """
        return (len(self),)

    def astype(self, dtype: str | type | ExtensionDtype, copy: bool = True) -> Index:
        """
        Create an Index with values cast to dtypes.

        The class of a new Index is determined by dtype. When conversion is
        impossible, a TypeError exception is raised.

        Parameters
        ----------
        dtype : numpy dtype or pandas type
            Note that any signed integer `dtype` is treated as ``'int64'``,
            and any unsigned integer `dtype` is treated as ``'uint64'``,
            regardless of the size.
        copy : bool, default True
            By default, astype always returns a newly allocated object.
            If copy is set to False and internal requirements on dtype are
            satisfied, the original data is used to create a new Index
            or the original Index is returned.

        Returns
        -------
        Index
            Index with values cast to specified dtype.

        Examples
        --------
        >>> idx = pd.Index([1, 2, 3])
        >>> idx
        Index([1, 2, 3], dtype='int64')
        >>> idx.astype('float')
        Index([1.0, 2.0, 3.0], dtype='float64')
        """
        if dtype is not None:
            dtype = pandas_dtype(dtype)

        if self.dtype == dtype:
            # Ensure that self.astype(self.dtype) is self
            return self.copy() if copy else self

        col_dtypes = {
            column: dtype for column in self._query_compiler.get_index_names()
        }
        new_query_compiler = self._query_compiler.astype_index(col_dtypes)

        if is_datetime64_any_dtype(dtype):
            # local import to avoid circular dependency.
            from snowflake.snowpark.modin.plugin.extensions.datetime_index import (
                DatetimeIndex,
            )

            return DatetimeIndex(query_compiler=new_query_compiler)

        return Index(query_compiler=new_query_compiler)

    @property
    def name(self) -> Hashable:
        """
        Get the index name.

        Returns
        -------
        Hashable
            Name of this index.

        Examples
        --------
        >>> idx = pd.Index([1, 2, 3], name='x')
        >>> idx
        Index([1, 2, 3], dtype='int64', name='x')
        >>> idx.name
        'x'
        """
        return self.names[0] if self.names else None

    @name.setter
    def name(self, value: Hashable) -> None:
        """
        Set Index name.
        """
        if not is_hashable(value):
            raise TypeError(f"{type(self).__name__}.name must be a hashable type")
        self._query_compiler = self._query_compiler.set_index_names([value])
        if self._parent is not None:
            self._parent._update_inplace(
                new_query_compiler=self._parent._query_compiler.set_index_names([value])
            )

    def _get_names(self) -> list[Hashable]:
        """
        Get names of index
        """
        return self._query_compiler.get_index_names()

    def _set_names(self, values: list) -> None:
        """
        Set new names on index. Each name has to be a hashable type.

        Parameters
        ----------
        values : str or sequence
            name(s) to set

        Raises
        ------
        TypeError if each name is not hashable.
        """
        if not is_list_like(values):
            raise ValueError("Names must be a list-like")
        if isinstance(values, Index):
            values = values.to_list()
        self._query_compiler = self._query_compiler.set_index_names(values)
        if self._parent is not None:
            self._parent._update_inplace(
                new_query_compiler=self._parent._query_compiler.set_index_names(values)
            )

    names = property(fset=_set_names, fget=_get_names)

    def set_names(
        self, names: Any, level: Any = None, inplace: bool = False
    ) -> Index | None:
        """
        Set Index name.

        Able to set new names partially and by level.

        Parameters
        ----------
        names : label or list of label or dict-like for MultiIndex
            Name(s) to set.

        level : int, label or list of int or label, optional

        inplace : bool, default False
            Modifies the object directly, instead of creating a new Index.

        Returns
        -------
        Index or None
            The same type as the caller or None if ``inplace=True``.

        Examples
        --------
        >>> idx = pd.Index([1, 2, 3, 4])
        >>> idx
        Index([1, 2, 3, 4], dtype='int64')
        >>> idx.set_names('quarter')
        Index([1, 2, 3, 4], dtype='int64', name='quarter')
        """
        if is_list_like(names) and len(names) > 1:
            raise ValueError(
                f"Since Index is a single index object in Snowpark pandas, "
                f"the length of new names must be 1, got {len(names)}."
            )
        if level is not None and level not in [0, -1]:
            raise IndexError(
                f"Level does not exist: Index has only 1 level, {level} is not a valid level number."
            )
        if inplace:
            name = names[0] if is_list_like(names) else names
            self.name = name
            return None
        else:
            res = self.__constructor__(query_compiler=self._query_compiler)
            res.name = names if is_scalar(names) else names[0]
            return res

    @property
    def ndim(self) -> int:
        """
        Number of dimensions of the underlying data, by definition 1.
        """
        return 1

    @property
    def size(self) -> int:
        """
        Get the number of elements in the underlying data.

        Returns
        -------
        int
            The number of elements in self

        Examples
        -------
        >>> idx = pd.Index([1, 2, 3])
        >>> idx
        Index([1, 2, 3], dtype='int64')
        >>> idx.size
        3
        """
        return len(self)

    @property
    def nlevels(self) -> int:
        """
        Number of levels.
        """
        return 1

    @property
    def empty(self) -> bool:
        """
        Whether the index is empty.

        Returns
        -------
        bool
            True if the index has no elements, False otherwise.

        Examples
        -------
        >>> idx = pd.Index([1, 2, 3])
        >>> idx
        Index([1, 2, 3], dtype='int64')
        >>> idx.empty
        False

        >>> idx = pd.Index([], dtype='int64')
        >>> idx
        Index([], dtype='int64')
        >>> idx.empty
        True
        """
        return self.size == 0

    @property
    def T(self, *args: Any, **kwargs: Any) -> Index:
        """
        Return the transpose, which is by definition self.

        Parameters
        ----------
        *args : Any
            Optional positional arguments for compatibility with other T APIs.
        **kwargs : Any
            Optional keyword arguments for compatibility with other T APIs.

        Returns
        -------
        Index
            This is self
        """
        return self

    def all(self, *args, **kwargs) -> bool | ExtensionArray:
        """
        Return whether all elements are Truthy.

        Parameters
        ----------
        *args : Any
            Required for compatibility with numpy.
        **kwargs : Any
            Required for compatibility with numpy.

        Returns
        -------
        bool or array-like (if axis is specified)
            A single element array-like may be converted to bool.

        See Also
        --------
        Index.any : Return whether any element in an Index is True.
        Series.any : Return whether any element in a Series is True.
        Series.all : Return whether all elements in a Series are True.

        Notes
        -----
        Not a Number (NaN), positive infinity and negative infinity
        evaluate to True because these are not equal to zero.
        `*args` and `**kwargs` are present for compatibility with numpy
        and not used with Snowpark pandas.

        Examples
        --------
        True, because nonzero integers are considered True.

        >>> pd.Index([1, 2, 3]).all()
        True

        False, because 0 is considered False.

        >>> pd.Index([0, 1, 2]).all()
        False
        """
        return self.to_series().all(**kwargs)

    def any(self, *args, **kwargs) -> bool | ExtensionArray:
        """
        Return whether any element is Truthy.

        Parameters
        ----------
        *args
            Required for compatibility with numpy.
        **kwargs
            Required for compatibility with numpy.

        Returns
        -------
        bool or array-like (if axis is specified)
            A single element array-like may be converted to bool.

        See Also
        --------
        Index.all : Return whether all elements are True.
        Series.all : Return whether all elements are True.

        Notes
        -----
        Not a Number (NaN), positive infinity and negative infinity
        evaluate to True because these are not equal to zero.
        `*args` and `**kwargs` are present for compatibility with numpy
        and not used with Snowpark pandas.

        Examples
        --------
        >>> index = pd.Index([0, 1, 2])
        >>> index.any()
        True

        >>> index = pd.Index([0, 0, 0])
        >>> index.any()
        False
        """
        return self.to_series().any(**kwargs)

    def argmin(self, axis=None, skipna: bool = True, *args, **kwargs) -> int:
        """
        Return int position of the smallest value in the Series.

        If the minimum is achieved in multiple locations, the first row position is returned.

        Parameters
        ----------
        axis : {None}
            Unused. Parameter needed for compatibility with DataFrame.
        skipna: bool, default True
            Exclude NA/null values when showing the result.
        *args, **kwargs
            Additional arguments and keywords for compatibility with NumPy.

        Returns
        -------
        int
            Row position of the minimum value.

        See Also
        --------
        Series.argmin : Return position of the minimum value.
        Series.argmax : Return position of the maximum value.
        numpy.ndarray.argmin : Equivalent method for numpy arrays.
        Series.idxmax : Return index label of the maximum values.
        Series.idxmin : Return index label of the minimum values.

        Note
        ----
        `*args` and `**kwargs` are present for compatibility with numpy and not used with Snowpark pandas.
        """
        return self.to_series().argmin(skipna=skipna, *args, **kwargs)

    def argmax(self, axis=None, skipna: bool = True, *args, **kwargs) -> int:
        """
        Return int position of the largest value in the Series.

        If the maximum is achieved in multiple locations, the first row position is returned.

        Parameters
        ----------
        axis : {None}
            Unused. Parameter needed for compatibility with DataFrame.
        skipna: bool, default True
            Exclude NA/null values when showing the result.
        *args, **kwargs
            Additional arguments and keywords for compatibility with NumPy.

        Returns
        -------
        int
            Row position of the maximum value.

        See Also
        --------
        Series.argmin : Return position of the minimum value.
        Series.argmax : Return position of the maximum value.
        numpy.ndarray.argmax : Equivalent method for numpy arrays.
        Series.idxmax : Return index label of the maximum values.
        Series.idxmin : Return index label of the minimum values.

        Note
        ----
        `*args` and `**kwargs` are present for compatibility with numpy and not used with Snowpark pandas.
        """
        return self.to_series().argmax(skipna=skipna, *args, **kwargs)

    def copy(
        self,
        name: Hashable | None = None,
        deep: bool = False,
    ) -> Index:
        """
        Make a copy of this object.

        Name is set on the new object.

        Parameters
        ----------
        name : Label, optional
            Set name for new object.
        deep : bool, default False

        Returns
        -------
        Index
            Index refers to new object which is a copy of this object.

        Notes
        -----
        In most cases, there should be no functional difference from using
        ``deep``, but if ``deep`` is passed it will attempt to deepcopy.

        Examples
        --------
        >>> idx = pd.Index(['a', 'b', 'c'])
        >>> new_idx = idx.copy()
        >>> idx is new_idx
        False
        """
        WarningMessage.ignored_argument(operation="copy", argument="deep", message="")
        return self.__constructor__(
            query_compiler=self._query_compiler.copy(), name=name
        )

    @index_not_implemented()
    def delete(self) -> None:
        """
        Make new Index with passed location(-s) deleted.

        Parameters
        ----------
        loc : int or list of int
            Location of item(-s) which will be deleted.
            Use a list of locations to delete more than one value at the same time.

        Returns
        -------
        Index
            Will be the same type as self, except for RangeIndex.

        See Also
        --------
        numpy.delete : Delete any rows and column from NumPy array (ndarray).
        """
        # TODO: SNOW-1458146 implement delete

    @index_not_implemented()
    def drop(
        self,
        labels: Any,
        errors: Literal["ignore", "raise"] = "raise",
    ) -> Index:
        """
        Make new Index with the passed list of labels deleted.

        Parameters
        ----------
        labels : array-like or scalar
        errors : {'ignore', 'raise'}, default 'raise'
            If 'ignore', suppress the error and existing labels are dropped.

        Returns
        -------
        Index
            The index created will have the same type as self.

        Raises
        ------
        KeyError
            If all labels are not found in the selected axis
        """
        # TODO: SNOW-1458146 implement drop

    @index_not_implemented()
    def drop_duplicates(self) -> None:
        """
        Return Index with duplicate values removed.

        Parameters
        ----------
        keep : {'first', 'last', ``False``}, default 'first'
            - 'first' : Drop duplicates except for the first occurrence.
            - 'last' : Drop duplicates except for the last occurrence.
            - ``False`` : Drop all duplicates.

        Returns
        -------
        Index

        See Also
        --------
        Series.drop_duplicates : Equivalent method on Series.
        DataFrame.drop_duplicates : Equivalent method on DataFrame.
        Index.duplicated : Related method on Index, indicating duplicate Index values.
        """
        # TODO: SNOW-1458147 implement drop_duplicates

    @index_not_implemented()
    def duplicated(self, keep: Literal["first", "last", False] = "first") -> np.ndarray:
        """
        Indicate duplicate index values.

        Duplicated values are indicated as ``True`` values in the resulting
        array. Either all duplicates, all except the first, or all except the
        last occurrence of duplicates can be indicated.

        Parameters
        ----------
        keep : {'first', 'last', False}, default 'first'
            The value or values in a set of duplicates to mark as missing.

            - 'first' : Mark duplicates as ``True`` except for the first
              occurrence.
            - 'last' : Mark duplicates as ``True`` except for the last
              occurrence.
            - ``False`` : Mark all duplicates as ``True``.

        Returns
        -------
        np.ndarray[bool]
            An array where duplicated values are indicated as ``True``

        See Also
        --------
        Series.duplicated : Equivalent method on pandas.Series.
        DataFrame.duplicated : Equivalent method on pandas.DataFrame.
        """
        # TODO: SNOW-1458147 implement duplicated

    def equals(self, other: Any) -> bool:
        """
        Determine if two Index objects are equal.

        The things that are being compared are:

        * The elements inside the Index object.
        * The order of the elements inside the Index object.

        Parameters
        ----------
        other : Any
            The other object to compare against.

        Returns
        -------
        bool
            True if "other" is an Index and it has the same elements and order
            as the calling index; False otherwise.

        Examples
        --------
        >>> idx1 = pd.Index([1, 2, 3])
        >>> idx1
        Index([1, 2, 3], dtype='int64')
        >>> idx1.equals(pd.Index([1, 2, 3]))
        True

        The elements inside are compared

        >>> idx2 = pd.Index(["1", "2", "3"])
        >>> idx2
        Index(['1', '2', '3'], dtype='object')

        >>> idx1.equals(idx2)
        True

        The order is compared

        >>> ascending_idx = pd.Index([1, 2, 3])
        >>> ascending_idx
        Index([1, 2, 3], dtype='int64')
        >>> descending_idx = pd.Index([3, 2, 1])
        >>> descending_idx
        Index([3, 2, 1], dtype='int64')
        >>> ascending_idx.equals(descending_idx)
        False

        The dtype is *not* compared

        >>> int64_idx = pd.Index([1, 2, 3], dtype='int64')
        >>> int64_idx
        Index([1, 2, 3], dtype='int64')

        # Snowpark pandas only supports signed integers so cast to uint won't work
        >>> uint64_idx = pd.Index([1, 2, 3], dtype='uint64')
        >>> uint64_idx
        Index([1, 2, 3], dtype='int64')
        >>> int64_idx.equals(uint64_idx)
        True
        """
        if self is other:
            return True

        if not isinstance(other, (type(self), self._NATIVE_INDEX_TYPE)):
            return False

        if isinstance(other, self._NATIVE_INDEX_TYPE):
            # Same as DataFrame/Series equals. Convert native Index to Snowpark pandas
            # Index for comparison.
            other = self.__constructor__(other)

        return self._query_compiler.index_equals(other._query_compiler)

    def identical(self, other: Any) -> bool:
        """
        Similar to equals, but checks that object attributes and types are also equal.

        Returns
        -------
        bool
            If two Index objects have equal elements and same type True,
            otherwise False.

        Examples
        --------
        >>> idx1 = pd.Index(['1', '2', '3'])
        >>> idx2 = pd.Index(['1', '2', '3'])
        >>> idx2.identical(idx1)
        True

        >>> idx1 = pd.Index(['1', '2', '3'], name="A")
        >>> idx2 = pd.Index(['1', '2', '3'], name="B")
        >>> idx2.identical(idx1)
        False
        """
        return (
            all(
                getattr(self, c, None) == getattr(other, c, None)
                for c in self._comparables
            )
            and type(self) == type(other)
            and self.dtype == other.dtype
            and self.equals(other)
        )

    @index_not_implemented()
    def insert(self) -> None:
        """
        Make new Index inserting new item at location.

        Follows Python numpy.insert semantics for negative values.

        Parameters
        ----------
        loc : int
        item : object

        Returns
        -------
        Index
        """
        # TODO: SNOW-1458138 implement insert

    def is_boolean(self) -> bool:
        """
        Check if the Index only consists of booleans.

        .. deprecated:: 2.0.0
            Use `pandas.api.types.is_bool_dtype` instead.

        Returns
        -------
        bool
            Whether the Index only consists of booleans.

        See Also
        --------
        is_integer : Check if the Index only consists of integers (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_object : Check if the Index is of the object dtype (deprecated).
        is_categorical : Check if the Index holds categorical data.
        is_interval : Check if the Index holds Interval objects (deprecated).

        Examples
        --------
        >>> idx = pd.Index([True, False, True])
        >>> idx.is_boolean()
        True

        >>> idx = pd.Index(["True", "False", "True"])
        >>> idx.is_boolean()
        False

        >>> idx = pd.Index([True, False, "True"])
        >>> idx.is_boolean()
        False
        """
        return is_bool_dtype(self.dtype)

    def is_floating(self) -> bool:
        """
        Check if the Index is a floating type.

        .. deprecated:: 2.0.0
            Use `pandas.api.types.is_float_dtype` instead

        The Index may consist of only floats, NaNs, or a mix of floats,
        integers, or NaNs.

        Returns
        -------
        bool
            Whether the Index only consists of only consists of floats, NaNs, or
            a mix of floats, integers, or NaNs.

        See Also
        --------
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_integer : Check if the Index only consists of integers (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_object : Check if the Index is of the object dtype. (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).

        Examples
        --------
        >>> idx = pd.Index([1.0, 2.0, 3.0, 4.0])
        >>> idx.is_floating()
        True

        >>> idx = pd.Index([1.0, 2.0, np.nan, 4.0])
        >>> idx.is_floating()
        True

        >>> idx = pd.Index([1, 2, 3, 4, np.nan])
        >>> idx.is_floating()
        True

        >>> idx = pd.Index([1, 2, 3, 4])
        >>> idx.is_floating()
        False
        """
        return is_float_dtype(self.dtype)

    def is_integer(self) -> bool:
        """
        Check if the Index only consists of integers.

        .. deprecated:: 2.0.0
            Use `pandas.api.types.is_integer_dtype` instead.

        Returns
        -------
        bool
            Whether the Index only consists of integers.

        See Also
        --------
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_object : Check if the Index is of the object dtype. (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).

        Examples
        --------
        >>> idx = pd.Index([1, 2, 3, 4])
        >>> idx.is_integer()
        True

        >>> idx = pd.Index([1.0, 2.0, 3.0, 4.0])
        >>> idx.is_integer()
        False

        >>> idx = pd.Index(["Apple", "Mango", "Watermelon"])
        >>> idx.is_integer()
        False
        """
        return is_integer_dtype(self.dtype)

    @index_not_implemented()
    def is_interval(self) -> None:
        """
        Check if the Index holds Interval objects.

        .. deprecated:: 2.0.0
            Use `isinstance(index.dtype, pd.IntervalDtype)` instead.

        Returns
        -------
        bool
            Whether the Index holds Interval objects.

        See Also
        --------
        IntervalIndex : Index for Interval objects.
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_integer : Check if the Index only consists of integers (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_object : Check if the Index is of the object dtype. (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        """

    def is_numeric(self) -> bool:
        """
        Check if the Index only consists of numeric data.

        .. deprecated:: 2.0.0
            Use `pandas.api.types.is_numeric_dtype` instead.

        Returns
        -------
        bool
            Whether the Index only consists of numeric data.

        See Also
        --------
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_integer : Check if the Index only consists of integers (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_object : Check if the Index is of the object dtype. (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).

        Examples
        --------
        >>> idx = pd.Index([1.0, 2.0, 3.0, 4.0])
        >>> idx.is_numeric()
        True

        >>> idx = pd.Index([1, 2, 3, 4.0])
        >>> idx.is_numeric()
        True

        >>> idx = pd.Index([1, 2, 3, 4])
        >>> idx.is_numeric()
        True

        >>> idx = pd.Index([1, 2, 3, 4.0, np.nan])
        >>> idx.is_numeric()
        True

        >>> idx = pd.Index([1, 2, 3, 4.0, np.nan, "Apple"])
        >>> idx.is_numeric()
        False
        """
        return is_numeric_dtype(self.dtype) and not is_bool_dtype(self.dtype)

    def is_object(self) -> bool:
        """
        Check if the Index is of the object dtype.

        .. deprecated:: 2.0.0
           Use `pandas.api.types.is_object_dtype` instead.

        Returns
        -------
        bool
            Whether the Index is of the object dtype.

        See Also
        --------
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_integer : Check if the Index only consists of integers (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).

        Examples
        --------
        >>> idx = pd.Index(["Apple", "Mango", "Watermelon"])
        >>> idx.is_object()
        True

        >>> idx = pd.Index(["Apple", "Mango", 2.0])
        >>> idx.is_object()
        True

        >>> idx = pd.Index([1.0, 2.0, 3.0, 4.0])
        >>> idx.is_object()
        False
        """
        return is_object_dtype(self.dtype)

    def min(
        self, axis: int | None = None, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> Scalar:
        """
        Return the minimum value of the Index.

        Parameters
        ----------
        axis : {None}
            Dummy argument for consistency with Series.
        skipna : bool, default True
            Exclude NA/null values when showing the result.
        *args, **kwargs
            Additional arguments and keywords for compatibility with NumPy.

        Returns
        -------
        scalar
            Minimum value.

        See Also
        --------
        Index.max : Return the maximum value of the object.
        Series.min : Return the minimum value in a Series.
        DataFrame.min : Return the minimum values in a DataFrame.

        Examples
        --------
        >>> idx = pd.Index([3, 2, 1])
        >>> idx.min()
        1

        >>> idx = pd.Index(['c', 'b', 'a'])
        >>> idx.min()
        'a'
        """
        if axis:
            raise ValueError("Axis must be None or 0 for Index objects")
        return self.to_series().min(skipna=skipna, **kwargs)

    def max(
        self, axis: int | None = None, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> Scalar:
        """
        Return the maximum value of the Index.

        Parameters
        ----------
        axis : int, optional
            For compatibility with NumPy. Only 0 or None are allowed.
        skipna : bool, default True
            Exclude NA/null values when showing the result.
        *args, **kwargs
            Additional arguments and keywords for compatibility with NumPy.

        Returns
        -------
        scalar
            Maximum value.

        See Also
        --------
        Index.min : Return the minimum value in an Index.
        Series.max : Return the maximum value in a Series.
        DataFrame.max : Return the maximum values in a DataFrame.

        Examples
        --------
        >>> idx = pd.Index([3, 2, 1])
        >>> idx.max()
        3

        >>> idx = pd.Index(['c', 'b', 'a'])
        >>> idx.max()
        'c'
        """
        if axis:
            raise ValueError("Axis must be None or 0 for Index objects")
        return self.to_series().max(skipna=skipna, **kwargs)

    def reindex(
        self,
        target: Iterable,
        method: str | None = None,
        level: int | None = None,
        limit: int | None = None,
        tolerance: int | float | None = None,
    ) -> tuple[Index, np.ndarray]:
        """
        Create index with target's values.

        Parameters
        ----------
        target : an iterable
        method : {None, 'pad'/'ffill', 'backfill'/'bfill', 'nearest'}, optional
            * default: exact matches only.
            * pad / ffill: find the PREVIOUS index value if no exact match.
            * backfill / bfill: use NEXT index value if no exact match
            * nearest: use the NEAREST index value if no exact match. Tied
              distances are broken by preferring the larger index value.
        level : int, optional
            Level of multiindex.
        limit : int, optional
            Maximum number of consecutive labels in ``target`` to match for
            inexact matches.
        tolerance : int or float, optional
            Maximum distance between original and new labels for inexact
            matches. The values of the index at the matching locations must
            satisfy the equation ``abs(index[indexer] - target) <= tolerance``.

            Tolerance may be a scalar value, which applies the same tolerance
            to all values, or list-like, which applies variable tolerance per
            element. List-like includes list, tuple, array, Series, and must be
            the same size as the index and its dtype must exactly match the
            index's type.

        Returns
        -------
        new_index : pd.Index
            Resulting index.
        indexer : np.ndarray[np.intp] or None
            Indices of output values in original index.

        Raises
        ------
        TypeError
            If ``method`` passed along with ``level``.
        ValueError
            If non-unique multi-index
        ValueError
            If non-unique index and ``method`` or ``limit`` passed.

        Notes
        -----
        ``method=nearest`` is not supported.

        If duplicate values are present, they are ignored,
        and all duplicate values are present in the result.

        If the source and target indices have no overlap,
        monotonicity checks are skipped.

        Tuple-like index values are not supported.

        Examples
        --------
        >>> idx = pd.Index(['car', 'bike', 'train', 'tractor'])
        >>> idx
        Index(['car', 'bike', 'train', 'tractor'], dtype='object')

        >>> idx.reindex(['car', 'bike'])
        (Index(['car', 'bike'], dtype='object'), array([0, 1]))

        See Also
        --------
        Series.reindex : Conform Series to new index with optional filling logic.
        DataFrame.reindex : Conform DataFrame to new index with optional filling logic.
        """

        # This code path is only hit if our index is lazy (as an eager index would simply call
        # the method on its underlying pandas Index object and return the result of that wrapped
        # appropriately.) Therefore, we specify axis=0, since the QueryCompiler expects lazy indices
        # on axis=0, but eager indices on axis=1 (used for error checking).
        if limit is not None and method is None:
            raise ValueError(
                "limit argument only valid if doing pad, backfill or nearest reindexing"
            )
        kwargs = {
            "method": method,
            "level": level,
            "limit": limit,
            "tolerance": tolerance,
            "_is_index": True,
        }

        internal_index_column = (
            self._query_compiler._modin_frame.index_column_snowflake_quoted_identifiers[
                0
            ]
        )
        internal_index_type = self._query_compiler._modin_frame.get_snowflake_type(
            internal_index_column
        )
        if isinstance(internal_index_type, ArrayType):
            raise NotImplementedError(
                "Snowpark pandas does not support `reindex` with tuple-like Index values."
            )
        else:
            query_compiler, indices = self._query_compiler.reindex(
                axis=0, labels=target, **kwargs
            )
            return Index(query_compiler=query_compiler), indices

    def rename(self, name: Any, inplace: bool = False) -> None:
        """
        Alter Index or MultiIndex name.

        Able to set new names without level. Defaults to returning new index.
        Length of names must match number of levels in MultiIndex.

        Parameters
        ----------
        name : label or list of labels
            Name(s) to set.
        inplace : bool, default False
            Modifies the object directly, instead of creating a new Index or
            MultiIndex.

        Returns
        -------
        Index or None
            The same type as the caller or None if ``inplace=True``.

        See Also
        --------
        Index.set_names : Able to set new names partially and by level.

        Examples
        --------
        >>> idx = pd.Index(['A', 'C', 'A', 'B'], name='score')
        >>> idx.rename('grade', inplace=False)
        Index(['A', 'C', 'A', 'B'], dtype='object', name='grade')
        >>> idx.rename('grade', inplace=True)

        Note
        ----
        Native pandas only allows hashable types for names. Snowpark pandas allows
        name to be any scalar or list-like type. If a tuple is used for the name,
        the tuple itself will be the name.

        For instance,
        >>> idx = pd.Index([1, 2, 3])
        >>> idx.rename(('a', 'b', 'c'), inplace=True)
        >>> idx.name
        ('a', 'b', 'c')
        """
        if isinstance(name, tuple):
            name = [name]  # The entire tuple is the name
        return self.set_names(names=name, inplace=inplace)

    def nunique(self, dropna: bool = True) -> int:
        """
        Return number of unique elements in the object.

        Excludes NA values by default.

        Parameters
        ----------
        dropna : bool, default True
            Don't include NaN in the count.

        Returns
        -------
        int

        See Also
        --------
        DataFrame.nunique: Method nunique for DataFrame.
        Series.count: Count non-NA/null observations in the Series.

        Examples
        --------
        >>> s = pd.Series([1, 3, 5, 7, 7])
        >>> s
        0    1
        1    3
        2    5
        3    7
        4    7
        dtype: int64

        >>> s.nunique()
        4
        """
        return self._query_compiler.nunique_index(dropna=dropna)

    def value_counts(
        self,
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        bins: int | None = None,
        dropna: bool = True,
    ) -> Series:
        """
        Return a Series containing counts of unique values.

        The resulting object will be in descending order so that the
        first element is the most frequently occurring element.
        Excludes NA values by default.

        Parameters
        ----------
        normalize : bool, default False
            If True, then the object returned will contain the relative
            frequencies of the unique values.
        sort : bool, default True
            Sort by frequencies when True. Preserve the order of the data when False.
        ascending : bool, default False
            Sort in ascending order.
        bins : int, optional
            Rather than count values, group them into half-open bins,
            a convenience for ``pd.cut``, only works with numeric data.
            `bins` is not yet supported.
        dropna : bool, default True
            Don't include counts of NaN.

        Returns
        -------
        Series
            A Series containing counts of unique values.

        See Also
        --------
        Series.count: Number of non-NA elements in a Series.
        DataFrame.count: Number of non-NA elements in a DataFrame.
        DataFrame.value_counts: Equivalent method on DataFrames.

        Examples
        --------
        >>> index = pd.Index([3, 1, 2, 3, 4, np.nan])
        >>> index.value_counts()
        3.0    2
        1.0    1
        2.0    1
        4.0    1
        Name: count, dtype: int64

        With `normalize` set to `True`, returns the relative frequency by
        dividing all values by the sum of values.

        >>> ind = pd.Index([3, 1, 2, 3, 4, np.nan])
        >>> ind.value_counts(normalize=True)
        3.0    0.4
        1.0    0.2
        2.0    0.2
        4.0    0.2
        Name: proportion, dtype: float64

        **bins**

        Bins can be useful for going from a continuous variable to a
        categorical variable; instead of counting unique
        apparitions of values, divide the index in the specified
        number of half-open bins.
        """
        return Series(
            query_compiler=self._query_compiler.value_counts_index(
                normalize=normalize,
                sort=sort,
                ascending=ascending,
                bins=bins,
                dropna=dropna,
            ).set_index_names([self.name]),
            name="proportion" if normalize else "count",
        )

    def item(self) -> Hashable:
        """
        Return the first element of the underlying data as a Python scalar.

        Returns
        -------
        scalar
            The first element of Series or Index.

        Raises
        ------
        ValueError
            If the data is not length = 1.
        """
        # slice the first two elements of the index and materialize them
        item = self._query_compiler.take_2d_positional(
            index=slice(2), columns=[]
        ).index.to_pandas()

        # return the element as a scalar if the index is exacly one element large
        if len(item) == 1:
            return item[0]

        # otherwise raise the same value error as pandas
        raise ValueError("can only convert an array of size 1 to a Python scalar")

    def to_series(
        self, index: Index | None = None, name: Hashable | None = None
    ) -> Series:
        """
        Create a Series with both index and values equal to the index keys.

        Useful with map for returning an indexer based on an index.

        Parameters
        ----------
        index : Index, optional
            Index of resulting Series. If None, defaults to original index.
        name : str, optional
            Name of resulting Series. If None, defaults to name of original
            index.

        Returns
        -------
        Series
            The dtype will be based on the type of the Index values.

        See Also
        --------
        Index.to_frame : Convert an Index to a DataFrame.
        Series.to_frame : Convert Series to DataFrame.
        """
        # get the index name if the name is not given
        if name is None:
            name = self.name

        # convert self to a dataframe and get qc
        # this will give us a df where the index and data columns both have self
        new_qc = self.to_frame(name=name)._query_compiler

        # if we are given an index, join this index column into qc
        if index is not None:
            new_qc = new_qc.set_index_from_series(Series(index)._query_compiler)

        # create series and set the name
        ser = Series(query_compiler=new_qc)
        ser.name = name
        return ser

    def to_frame(
        self, index: bool = True, name: Hashable | None = lib.no_default
    ) -> modin.pandas.DataFrame:
        """
        Create a :class:`DataFrame` with a column containing the Index.

        Parameters
        ----------
        index : bool, default True
            Set the index of the returned DataFrame as the original Index.

        name : object, defaults to index.name
            The passed name should substitute for the index name (if it has
            one).

        Returns
        -------
        :class:`DataFrame`
            :class:`DataFrame` containing the original Index data.

        See Also
        --------
        Index.to_series : Convert an Index to a Series.
        Series.to_frame : Convert Series to DataFrame.

        Examples
        --------
        >>> idx = pd.Index(['Ant', 'Bear', 'Cow'], name='animal')
        >>> idx.to_frame()   # doctest: +NORMALIZE_WHITESPACE
               animal
        animal
        Ant       Ant
        Bear     Bear
        Cow       Cow

        By default, the original Index is reused. To enforce a new Index:

        >>> idx.to_frame(index=False)
          animal
        0    Ant
        1   Bear
        2    Cow

        To override the name of the resulting column, specify `name`:

        >>> idx.to_frame(index=False, name='zoo')
            zoo
        0   Ant
        1  Bear
        2   Cow
        """
        # Do a reset index to convert the index column to a data column,
        # the index column becomes the pandas default index of row position
        # Example:
        # before
        # index columns:    data columns (empty):
        #      100
        #      200
        #      300
        # after
        # index columns:    data columns (name=column_name):
        #       0               100
        #       1               200
        #       2               300
        new_qc = self._query_compiler.reset_index()
        # if index is true, we want self to be in the index and data columns of the df,
        # so set the index as the data column and set the name of the index
        if index:
            new_qc = new_qc.set_index([new_qc.columns[0]], drop=False).set_index_names(
                [self.name]
            )
        # If `name` is specified, use it as new column name; otherwise, set new column name to the original index name.
        # Note there is one exception case: when the original index name is None, the new column name should be 0.
        if name != lib.no_default:
            new_col_name = name
        else:
            new_col_name = self.name
            if new_col_name is None:
                new_col_name = 0
        new_qc = new_qc.set_columns([new_col_name])

        return DataFrame(query_compiler=new_qc)

    @index_not_implemented()
    def fillna(self) -> None:
        """
        Fill NA/NaN values with the specified value.

        Parameters
        ----------
        value : scalar
            Scalar value to use to fill holes (e.g. 0).
            This value cannot be a list-likes.
        downcast : dict, default is None
            A dict of item->dtype of what to downcast if possible,
            or the string 'infer' which will try to downcast to an appropriate
            equal type (e.g. float64 to int64 if possible).

            .. deprecated:: 2.1.0

        Returns
        -------
        Index

        See Also
        --------
        DataFrame.fillna : Fill NaN values of a DataFrame.
        Series.fillna : Fill NaN Values of a Series.
        """
        # TODO: SNOW-1458139 implement fillna

    @index_not_implemented()
    def dropna(self) -> None:
        """
        Return Index without NA/NaN values.

        Parameters
        ----------
        how : {'any', 'all'}, default 'any'
            If the Index is a MultiIndex, drop the value when any or all levels
            are NaN.

        Returns
        -------
        Index
        """
        # TODO: SNOW-1458139 implement dropna

    @index_not_implemented()
    def isna(self) -> None:
        """
        Detect missing values.

        Return a boolean same-sized object indicating if the values are NA.
        NA values, such as ``None``, :attr:`numpy.NaN` or :attr:`pd.NaT`, get
        mapped to ``True`` values.
        Everything else get mapped to ``False`` values. Characters such as
        empty strings `''` or :attr:`numpy.inf` are not considered NA values.

        Returns
        -------
        numpy.ndarray[bool]
            A boolean array of whether my values are NA.

        See Also
        --------
        Index.notna : Boolean inverse of isna.
        Index.dropna : Omit entries with missing values.
        isna : Top-level isna.
        Series.isna : Detect missing values in Series object.
        """
        # TODO: SNOW-1458139 implement isna

    @index_not_implemented()
    def notna(self) -> None:
        """
        Detect existing (non-missing) values.

        Return a boolean same-sized object indicating if the values are not NA.
        Non-missing values get mapped to ``True``. Characters such as empty
        strings ``''`` or :attr:`numpy.inf` are not considered NA values.
        NA values, such as None or :attr:`numpy.NaN`, get mapped to ``False``
        values.

        Returns
        -------
        numpy.ndarray[bool]
            Boolean array to indicate which entries are not NA.

        See Also
        --------
        Index.notnull : Alias of notna.
        Index.isna: Inverse of notna.
        notna : Top-level notna.
        """
        # TODO: SNOW-1458139 implement notna

    @index_not_implemented()
    def hasnans(self) -> None:
        """
        Return True if there are any NaNs.

        Enables various performance speedups.

        Returns
        -------
        bool

        See Also
        --------
        Index.isna : Detect missing values.
        Index.dropna : Return Index without NA/NaN values.
        Index.fillna : Fill NA/NaN values with the specified value.
        """
        # TODO: SNOW-1458139 implement hasnans

    def tolist(self) -> list:
        """
        Return a list of the values.

        These are each a scalar type, which is a Python scalar
        (for str, int, float) or a pandas scalar
        (for Timestamp/Timedelta/Interval/Period)

        Returns
        -------
        list
            The index values in list form

        See Also
        --------
        numpy.ndarray.tolist : Return the array as an a.ndim-levels deep
            nested list of Python scalars.

        Examples
        >>> idx = pd.Index([1, 2, 3])
        >>> idx
        Index([1, 2, 3], dtype='int64')

        >>> idx.to_list()
        [1, 2, 3]
        """
        return self.to_pandas().tolist()

    to_list = tolist

    def sort_values(
        self,
        return_indexer: bool = False,
        ascending: bool = True,
        na_position: NaPosition = "last",
        key: Callable | None = None,
    ) -> Index | tuple[Index, np.ndarray]:
        """
        Return a sorted copy of the index.

        Return a sorted copy of the index, and optionally return the indices
        that sorted the index itself.

        Parameters
        ----------
        return_indexer : bool, default False
            Should the indices that would sort the index be returned.
        ascending : bool, default True
            Should the index values be sorted in ascending order.
        na_position : {'first' or 'last'}, default 'last'
            Argument 'first' puts NaNs at the beginning, 'last' puts NaNs at
            the end.
        key : callable, optional
            If not None, apply the key function to the index values
            before sorting. This is similar to the `key` argument in the
            builtin :meth:`sorted` function, with the notable difference that
            this `key` function should be *vectorized*. It should expect an
            ``Index`` and return an ``Index`` of the same shape.
            This parameter is not yet supported.

        Returns
        -------
        Index, numpy.ndarray
            Index is returned in all cases as a sorted copy of the index.
            ndarray is returned when return_indexer is True, represents the indices
            that the index itself was sorted by.

        See Also
        --------
        Series.sort_values : Sort values of a Series.
        DataFrame.sort_values : Sort values in a DataFrame.

        Note
        ----
        The order of the indexer pandas returns is based on numpy's `argsort` which
        defaults to quicksort. However, currently Snowpark pandas does not support quicksort;
        instead, stable sort is performed. Therefore, the order of the indexer returned by
        `Index.sort_values` is not guaranteed to match pandas' result indexer.

        Examples
        --------
        >>> idx = pd.Index([10, 100, 1, 1000])
        >>> idx
        Index([10, 100, 1, 1000], dtype='int64')

        Sort values in ascending order (default behavior).

        >>> idx.sort_values()
        Index([1, 10, 100, 1000], dtype='int64')

        Sort values in descending order, and also get the indices `idx` was
        sorted by.

        >>> idx.sort_values(ascending=False, return_indexer=True)
        (Index([1000, 100, 10, 1], dtype='int64'), array([3, 1, 0, 2]))
        """
        res = self._query_compiler.sort_index(
            axis=0,
            level=None,
            ascending=ascending,
            kind="quicksort",
            na_position=na_position,
            sort_remaining=True,
            ignore_index=False,
            key=key,
            include_indexer=return_indexer,
        )
        index = self.__constructor__(query_compiler=res)
        if return_indexer:
            # When `return_indexer` is True, `res` is a query compiler with one index column
            # and one data column.
            # The resultant sorted Index is the index column and the indexer is the data column.
            # Therefore, performing Index(qc) and Series(qc).to_numpy() yields the required
            # objects to return.
            return index, Series(query_compiler=res).to_numpy()
        else:
            # When `return_indexer` is False, a query compiler with only one index column
            # is returned.
            return index

    @index_not_implemented()
    def append(self) -> None:
        """
        Append a collection of Index options together.

        Parameters
        ----------
        other : Index or list/tuple of indices

        Returns
        -------
        Index
        """
        # TODO: SNOW-1458149 implement append

    @index_not_implemented()
    def join(self) -> None:
        """
        Compute join_index and indexers to conform data structures to the new index.

        Parameters
        ----------
        other : Index
        how : {'left', 'right', 'inner', 'outer'}
        level : int or level name, default None
        return_indexers : bool, default False
        sort : bool, default False
            Sort the join keys lexicographically in the result Index. If False,
            the order of the join keys depends on the join type (how keyword).

        Returns
        -------
        join_index, (left_indexer, right_indexer)
        """
        # TODO: SNOW-1458150 implement join

    def intersection(self, other: Any, sort: bool = False) -> Index:
        """
        Form the intersection of two Index objects.

        This returns a new Index with elements common to the index and `other`.

        Parameters
        ----------
        other : Index or array-like
        sort : True, False or None, default False
            Whether to sort the resulting index.

            * None : sort the result, except when `self` and `other` are equal
              or when the values cannot be compared.
            * False : do not sort the result.
            * True : Sort the result (which may raise TypeError).

        Returns
        -------
        Index
            A new Index with elements common to the index and `other`.

        Examples
        --------
        >>> idx1 = pd.Index([1, 2, 3, 4])
        >>> idx2 = pd.Index([3, 4, 5, 6])
        >>> idx1.intersection(idx2)
        Index([3, 4], dtype='int64')
        """
        # TODO: SNOW-1458151 implement intersection
        WarningMessage.index_to_pandas_warning("intersection")
        return self.__constructor__(
            self.to_pandas().intersection(
                other=try_convert_index_to_native(other), sort=sort
            )
        )

    @index_not_implemented()
    def union(self, other: Any, sort: bool = False) -> Index:
        """
        Form the union of two Index objects.

        If the Index objects are incompatible, both Index objects will be
        cast to dtype('object') first.

        Parameters
        ----------
        other : Index or array-like
        sort : bool or None, default None
            Whether to sort the resulting Index.

            * None : Sort the result, except when

              1. `self` and `other` are equal.
              2. `self` or `other` has length 0.
              3. Some values in `self` or `other` cannot be compared.
                 A RuntimeWarning is issued in this case.

            * False : do not sort the result.
            * True : Sort the result (which may raise TypeError).

        Returns
        -------
        Index
            The Index that represents the union between the two indexes
        """
        # TODO: SNOW-1458149 implement union w/o sort
        # TODO: SNOW-1468240 implement union w/ sort

    @index_not_implemented()
    def difference(self, other: Any, sort: Any = None) -> Index:
        """
        Return a new Index with elements of index not in `other`.

        This is the set difference of two Index objects.

        Parameters
        ----------
        other : Index or array-like
        sort : bool or None, default None
            Whether to sort the resulting index. By default, the
            values are attempted to be sorted, but any TypeError from
            incomparable elements is caught by pandas.

            * None : Attempt to sort the result, but catch any TypeErrors
              from comparing incomparable elements.
            * False : Do not sort the result.
            * True : Sort the result (which may raise TypeError).

        Returns
        -------
        Index
            An index object that represents the difference between the two indexes.
        """
        # TODO: SNOW-1458152 implement difference

    @index_not_implemented()
    def get_indexer_for(self, target: Any) -> Any:
        """
        Guaranteed return of an indexer even when non-unique.

        This dispatches to get_indexer or get_indexer_non_unique
        as appropriate.

        Returns
        -------
        np.ndarray[np.intp]
            List of indices.
        """
        WarningMessage.index_to_pandas_warning("get_indexer_for")
        return self.to_pandas().get_indexer_for(target=target)

    def _get_indexer_strict(self, key: Any, axis_name: str) -> tuple[Index, np.ndarray]:
        """
        Analogue to pandas.Index.get_indexer that raises if any elements are missing.
        """
        WarningMessage.index_to_pandas_warning("_get_indexer_strict")
        tup = self.to_pandas()._get_indexer_strict(key=key, axis_name=axis_name)
        return self.__constructor__(tup[0]), tup[1]

    @index_not_implemented()
    def get_level_values(self, level: int | str) -> Index:
        """
        Return an Index of values for requested level.

        This is primarily useful to get an individual level of values from a
        MultiIndex, but is provided on Index as well for compatibility.

        Parameters
        ----------
        level : int or str
            It is either the integer position or the name of the level.

        Returns
        -------
        Index
            self, since self only has one level

        Notes
        -----
        For Index, level should be 0, since there are no multiple levels.
        """
        WarningMessage.index_to_pandas_warning("get_level_values")
        return self.__constructor__(self.to_pandas().get_level_values(level=level))

    @index_not_implemented()
    def isin(self) -> None:
        """
        Return a boolean array where the index values are in `values`.

        Compute boolean array of whether each index value is found in the
        passed set of values. The length of the returned boolean array matches
        the length of the index.

        Parameters
        ----------
        values : set or list-like
            Sought values.
        level : str or int, optional
            Name or position of the index level to use (if the index is a
            `MultiIndex`).

        Returns
        -------
        np.ndarray[bool]
            NumPy array of boolean values.

        See Also
        --------
        Series.isin : Same for Series.
        DataFrame.isin : Same method for DataFrames.

        Notes
        -----
        In the case of `MultiIndex` you must either specify `values` as a
        list-like object containing tuples that are the same length as the
        number of levels, or specify `level`. Otherwise it will raise a
        ``ValueError``.

        If `level` is specified:

        - if it is the name of one *and only one* index level, use that level;
        - otherwise it should be a number indicating level position.
        """
        # TODO: SNOW-1458153 implement isin

    @index_not_implemented()
    def slice_indexer(
        self,
        start: Hashable | None = None,
        end: Hashable | None = None,
        step: int | None = None,
    ) -> slice:
        """
        Compute the slice indexer for input labels and step.

        Index needs to be ordered and unique.

        Parameters
        ----------
        start : label, default None
            If None, defaults to the beginning.
        end : label, default None
            If None, defaults to the end.
        step : int, default None

        Returns
        -------
        slice
            The slice of indices

        Raises
        ------
        KeyError
            If key does not exist, or key is not unique and index is not ordered.

        Notes
        -----
        This function assumes that the data is sorted, so use at your own peril
        """
        WarningMessage.index_to_pandas_warning("slice_indexer")
        return self.to_pandas().slice_indexer(start=start, end=end, step=step)

    @property
    def array(self) -> ExtensionArray:
        """
        return the array of values
        """
        return self.to_pandas().array

    def _summary(self, name: Any = None) -> str:
        """
        Return a summarized representation.

        Parameters
        ----------
        name : str
            name to use in the summary representation

        Returns
        -------
        str
            String with a summarized representation of the index
        """
        WarningMessage.index_to_pandas_warning("_summary")
        return self.to_pandas()._summary(name=name)

    def __array__(self, dtype: Any = None) -> np.ndarray:
        """
        The array interface, return the values.
        """
        return self.to_pandas().__array__(dtype=dtype)

    def __repr__(self) -> str:
        """
        Return a string representation for this object.
        """
        # Create the representation for each field in the index and then join them.
        # First, create the data representation.
        # When the number of elements in the Index is greater than the number of
        # elements to display, display only the first and last 10 elements.
        max_seq_items = get_option("display.max_seq_items") or 100
        length_of_index, _, temp_df = self.to_series()._query_compiler.build_repr_df(
            max_seq_items, 1
        )
        if isinstance(temp_df, native_pd.DataFrame) and not temp_df.empty:
            local_index = temp_df.iloc[:, 0].to_list()
        else:
            local_index = []
        too_many_elem = max_seq_items < length_of_index

        # The representation begins with class name followed by parentheses; the data representation is enclosed in
        # square brackets. For example, "DatetimeIndex([" or "Index([".
        class_name = self.__class__.__name__

        # In the case of DatetimeIndex, if the data is timezone-aware, the timezone is displayed
        # within the dtype field. This is not directly supported in Snowpark pandas.
        native_pd_idx = native_pd.Index(local_index)
        dtype = native_pd_idx.dtype if "DatetimeIndex" in class_name else self.dtype

        # _format_data() correctly indents the data and places newlines where necessary.
        # It also accounts for the comma, newline, and indentation for the next field (dtype).
        data_repr = native_pd_idx._format_data()

        # Next, creating the representation for each field with their respective labels.
        # The index always displays the data and datatype, and optionally the name, length, and freq.
        dtype_repr = f"dtype='{dtype}'"
        name_repr = f", name='{self.name}'" if self.name else ""
        # Length is displayed only when the number of elements is greater than the number of elements to display.
        length_repr = f", length={length_of_index}" if too_many_elem else ""
        # The frequency is displayed for DatetimeIndex and TimedeltaIndex
        # TODO: SNOW-1625233 update freq_repr; replace None with the correct value.
        freq_repr = (
            ", freq=None" if class_name in ("DatetimeIndex", "TimedeltaIndex") else ""
        )

        repr = (
            class_name
            + "("
            + data_repr
            + dtype_repr
            + name_repr
            + length_repr
            + freq_repr
            + ")"
        )
        return repr

    def __iter__(self) -> Iterator:
        """
        Return an iterator of the values.

        These are each a scalar type, which is a Python scalar
        (for str, int, float) or a pandas scalar
        (for Timestamp/Timedelta/Interval/Period)

        Returns
        -------
        Iterator
            Iterator of the index values

        Examples
        --------
        >>> i = pd.Index([1, 2, 3])
        >>> for x in i:
        ...     print(x)
        1
        2
        3
        """
        WarningMessage.index_to_pandas_warning("__iter__")
        return self.to_pandas().__iter__()

    def __contains__(self, key: Any) -> bool:
        """
        Return a boolean indicating whether the provided key is in the index.

        Parameters
        ----------
        key : label
            The key to check if it is present in the index.

        Returns
        -------
        bool
            True if the key is in the index, False otherwise.

        Raises
        ------
        TypeError
            If the key is not hashable.

        Examples
        --------
        >>> idx = pd.Index([1, 2, 3, 4])
        >>> idx
        Index([1, 2, 3, 4], dtype='int64')

        >>> 2 in idx
        True
        >>> 6 in idx
        False
        """
        WarningMessage.index_to_pandas_warning("__contains__")
        return self.to_pandas().__contains__(key=key)

    def __len__(self) -> int:
        """
        Return the length of the Index as an int.
        """
        return self._query_compiler.get_axis_len(0)

    def __getitem__(self, key: Any) -> np.ndarray | None | Index:
        """
        Reuse series iloc to implement getitem for index.
        """
        try:
            res = self.to_series().iloc[key]
            if isinstance(res, Series):
                res = res.index
            return res
        except IndexError as ie:
            raise IndexError(
                "only integers, slices (`:`), ellipsis (`...`), numpy.newaxis (`None`) and integer or "
                "boolean arrays are valid indices"
            ) from ie

    def __setitem__(self, key: Any, value: Any) -> None:
        """
        Override numpy.ndarray's __setitem__ method to work as desired.

        We raise a TypeError because the Index values are not mutable
        """
        raise TypeError("Index does not support mutable operations")

    @property
    def str(self) -> native_pd.core.strings.accessor.StringMethods:
        """
        Vectorized string functions for Series and Index.

        NAs stay NA unless handled otherwise by a particular method.
        Patterned after Python's string methods, with some inspiration from
        R's stringr package.

        Examples
        --------
        >>> s = pd.Series(["A_Str_Series"])
        >>> s
        0    A_Str_Series
        dtype: object

        >>> s.str.split("_")
        0    [A, Str, Series]
        dtype: object

        >>> s.str.replace("_", "")
        0    AStrSeries
        dtype: object
        """
        return self.to_pandas().str

    def _to_datetime(
        self,
        errors: DateTimeErrorChoices = "raise",
        dayfirst: bool = False,
        yearfirst: bool = False,
        utc: bool = False,
        format: str = None,
        exact: bool | lib.NoDefault = lib.no_default,
        unit: str = None,
        infer_datetime_format: bool | lib.NoDefault = lib.no_default,
        origin: DateTimeOrigin = "unix",
    ) -> Index:
        """
        Convert index to DatetimeIndex.
        Args:
            errors: {'ignore', 'raise', 'coerce'}, default 'raise'
              If 'raise', then invalid parsing will raise an exception.
              If 'coerce', then invalid parsing will be set as NaT.
              If 'ignore', then invalid parsing will return the input.
            dayfirst: bool, default False
              Specify a date parse order if arg is str or is list-like.
            yearfirst: bool, default False
              Specify a date parse order if arg is str or is list-like.
            utc: bool, default False
              Control timezone-related parsing, localization and conversion.
            format: str, default None
              The strftime to parse time
            exact: bool, default True
              Control how format is used:
              True: require an exact format match.
              False: allow the format to match anywhere in the target string.
            unit: str, default 'ns'
              The unit of the arg (D,s,ms,us,ns) denote the unit, which is an integer
              or float number.
            infer_datetime_format: bool, default False
              If True and no format is given, attempt to infer the format of the \
              datetime strings based on the first non-NaN element.
            origin: scalar, default 'unix'
              Define the reference date. The numeric values would be parsed as number
              of units (defined by unit) since this reference date.

        Returns:
            DatetimeIndex
        """
        from snowflake.snowpark.modin.plugin.extensions.datetime_index import (
            DatetimeIndex,
        )

        new_qc = self._query_compiler.series_to_datetime(
            errors,
            dayfirst,
            yearfirst,
            utc,
            format,
            exact,
            unit,
            infer_datetime_format,
            origin,
            include_index=True,
        )
        return DatetimeIndex(query_compiler=new_qc)
