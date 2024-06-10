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

from typing import Any, Callable, Hashable, Iterator, Literal

import numpy as np
import pandas as native_pd
from pandas._typing import ArrayLike, DtypeObj, NaPosition, Self
from pandas.core.arrays import ExtensionArray
from pandas.core.dtypes.base import ExtensionDtype
from pandas.core.indexes.frozen import FrozenList

from snowflake.snowpark.modin.pandas.utils import try_convert_index_to_native
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    index_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage


class Index:
    """
    Immutable sequence used for indexing and alignment.

    The basic object storing axis labels for all pandas objects.

    Parameters
    ----------
    data : array-like (1-dimensional)
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

    Notes
    -----
    An Index instance can **only** contain hashable objects.
    An Index instance *can not* hold numpy float16 dtype.

    Examples
    --------
    >>> pd.Index([1, 2, 3])
    Index([1, 2, 3], dtype='int64')

    >>> pd.Index(list('abc'))
    Index(['a', 'b', 'c'], dtype='object')

    >>> pd.Index([1, 2, 3], dtype="uint8")
    Index([1, 2, 3], dtype='uint8')
    """

    # same fields as native pandas index constructor
    def __init__(
        self,
        # Any should be replaced with SnowflakeQueryCompiler when possible (linter won't allow it now)
        data: ArrayLike | Any = None,
        dtype: str | np.dtype | ExtensionDtype | None = None,
        copy: bool = False,
        name: object = None,
        tupleize_cols: bool = True,
    ) -> None:
        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
        )

        # TODO: SNOW-1359041: Switch to lazy index implementation
        if isinstance(data, native_pd.Index):
            self._index = data
        elif isinstance(data, Index):
            self._index = data.to_pandas()
        elif isinstance(data, SnowflakeQueryCompiler):
            self._index = data._modin_frame.index_columns_index
        else:
            self._index = native_pd.Index(
                data=data,
                dtype=dtype,
                copy=copy,
                name=name,
                tupleize_cols=tupleize_cols,
            )

    def to_pandas(self) -> native_pd.Index:
        """
        Convert Snowpark pandas Index to pandas Index

        Returns
        -------
        pandas Index
            A native pandas Index representation of self
        """
        return self._index

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
    @index_not_implemented()
    def is_monotonic_increasing(self) -> None:
        """
        Return a boolean if the values are equal or increasing.

        Returns
        -------
        bool
            Whether the values are equal or increasing

        See Also
        --------
        Index.is_monotonic_decreasing : Check if the values are equal or decreasing
        """
        # TODO: SNOW-1458134 implement is_monotonic_increasing

    @property
    @index_not_implemented()
    def is_monotonic_decreasing(self) -> None:
        """
        Return a boolean if the values are equal or decreasing.

        Returns
        -------
        bool
            Whether the values are equal or decreasing

        See Also
        --------
        Index.is_monotonic_increasing : Check if the values are equal or increasing
        """
        # TODO: SNOW-1458134 implement is_monotonic_decreasing

    @property
    @index_not_implemented()
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
        >>> idx.is_unique  # doctest: +SKIP
        False

        >>> idx = pd.Index([1, 5, 7])
        >>> idx.is_unique  # doctest: +SKIP
        True

        >>> idx = pd.Index(["Watermelon", "Orange", "Apple",
        ...                 "Watermelon"]).astype("category")
        >>> idx.is_unique  # doctest: +SKIP
        False

        >>> idx = pd.Index(["Orange", "Apple",
        ...                 "Watermelon"]).astype("category")
        >>> idx.is_unique  # doctest: +SKIP
        True
        """
        # TODO: SNOW-1458131 implement is_unique
        self.to_pandas_warning()
        return self.to_pandas().is_unique

    @property
    @index_not_implemented()
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
        >>> idx.has_duplicates  # doctest: +SKIP
        True

        >>> idx = pd.Index([1, 5, 7])
        >>> idx.has_duplicates  # doctest: +SKIP
        False

        >>> idx = pd.Index(["Watermelon", "Orange", "Apple",
        ...                 "Watermelon"]).astype("category")
        >>> idx.has_duplicates  # doctest: +SKIP
        True

        >>> idx = pd.Index(["Orange", "Apple",
        ...                 "Watermelon"]).astype("category")
        >>> idx.has_duplicates  # doctest: +SKIP
        False
        """
        # TODO: SNOW-1458131 implement has_duplicates
        return not self.is_unique

    @property
    @index_not_implemented()
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
        >>> idx.dtype  # doctest: +SKIP
        dtype('int64')
        """
        # TODO: SNOW-1458123 implement dtype
        self.to_pandas_warning()
        return self.to_pandas().dtype

    @property
    @index_not_implemented()
    def inferred_type(self) -> None:
        """
        Return a string of the type inferred from the values.
        """

    @property
    @index_not_implemented()
    def shape(self) -> None:
        """
        Return a tuple of the shape of the underlying data.
        """
        # TODO: SNOW-1458118 implement shape

    @property
    @index_not_implemented()
    def name(self) -> Hashable:
        """
        Get the index name.

        Returns
        -------
        Hashable
            name of this index

        Examples
        --------
        >>> idx = pd.Index([1, 2, 3], name='x')
        >>> idx
        Index([1, 2, 3], dtype='int64', name='x')
        >>> idx.name
        'x'
        """
        # TODO: SNOW-1458122 implement name
        self.to_pandas_warning()
        return self.to_pandas().name

    @name.setter
    def name(self, value: Hashable) -> None:
        """
        Set Index name.
        """
        self.to_pandas_warning()
        self.to_pandas().name = value

    def _get_names(self) -> FrozenList:
        """
        Get names of index
        """
        raise ErrorMessage.NotImplementedError("Index.names is not yet implemented")
        self.to_pandas_warning()
        return self.to_pandas()._get_names()

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
        raise ErrorMessage.NotImplementedError("Index.names is not yet implemented")
        self.to_pandas_warning()
        self.to_pandas()._set_names(values)

    # TODO: SNOW-1458122 implement names
    names = property(fset=_set_names, fget=_get_names)

    @property
    @index_not_implemented()
    def nbytes(self) -> None:
        """
        Return the number of bytes in the underlying data.
        """

    @property
    @index_not_implemented()
    def ndim(self) -> None:
        """
        Number of dimensions of the underlying data, by definition 1.
        """

    @property
    @index_not_implemented()
    def size(self) -> None:
        """
        Return the number of elements in the underlying data.
        """
        # TODO: SNOW-1458118 implement size

    @property
    @index_not_implemented()
    def empty(self) -> None:
        """
        Whether the index is empty.
        """
        # TODO: SNOW-1458118 implement empty

    @property
    @index_not_implemented()
    def T(self) -> Index:
        """
        Return the transpose, which is by definition self.
        """
        # TODO: SNOW-1458140 implement T
        return self

    @property
    @index_not_implemented()
    def memory_usage(self, deep: bool = False) -> None:
        """
        Memory usage of the values.

        Parameters
        ----------
        deep : bool, default False
            Introspect the data deeply, interrogate object dtypes for system-level memory consumption.

        Returns
        -------
        bytes
            The number of bytes used.

        See Also
        --------
        numpy.ndarray.nbytes : Total bytes consumed by the elements of the array.

        Notes
        -----
        Memory usage does not include memory consumed by elements that are not components of the array if deep=False or
        if used on PyPy.
        """

    @index_not_implemented()
    def all(self, *args: Any, **kwargs: Any) -> None:
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
        """
        # TODO: SNOW-1458141 implement all

    @index_not_implemented()
    def any(self, *args: Any, **kwargs: Any) -> None:
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
        """
        # TODO: SNOW-1458141 implement any

    @index_not_implemented()
    def argmin(
        self, axis: str | None, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> None:
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
        """
        # TODO: SNOW-1458142 implement argmin

    @index_not_implemented()
    def argmax(
        self, axis: str | None, skipna: bool = True, *args: Any, **kwargs: Any
    ) -> None:
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
        """
        # TODO: SNOW-1458142 implement argmax

    @index_not_implemented()
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
        >>> new_idx = idx.copy()  # doctest: +SKIP
        >>> idx is new_idx  # doctest: +SKIP
        False
        """
        # TODO: SNOW-1458120 implement copy
        self.to_pandas_warning()
        return Index(self.to_pandas().copy(deep=deep, name=name))

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

        Examples
        --------
        >>> idx = pd.Index(['a', 'b', 'c'])
        >>> idx.drop(['a'])  # doctest: +SKIP
        Index(['b', 'c'], dtype='object')
        """
        # TODO: SNOW-1458146 implement drop
        self.to_pandas_warning()
        return Index(self.to_pandas().drop(labels=labels, errors=errors))

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
    def duplicated(self, keep: Literal["first", "last", False] = "first") -> Any:
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

        Examples
        --------
        By default, for each set of duplicated values, the first occurrence is
        set to False and all others to True:

        >>> idx = pd.Index(['lama', 'cow', 'lama', 'beetle', 'lama'])
        >>> idx.duplicated()  # doctest: +SKIP
        array([False, False,  True, False,  True])

        which is equivalent to

        >>> idx.duplicated(keep='first')  # doctest: +SKIP
        array([False, False,  True, False,  True])

        By using 'last', the last occurrence of each set of duplicated values
        is set on False and all others on True:

        >>> idx.duplicated(keep='last')  # doctest: +SKIP
        array([ True, False,  True, False, False])

        By setting keep on ``False``, all duplicates are True:

        >>> idx.duplicated(keep=False)  # doctest: +SKIP
        array([ True, False,  True, False,  True])
        """
        # TODO: SNOW-1458147 implement duplicated
        self.to_pandas_warning()
        return self.to_pandas().duplicated(keep=keep)

    @index_not_implemented()
    def equals(self, other: Any) -> bool:
        """
        Determine if two Index object are equal.

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
        False

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
        >>> uint64_idx = pd.Index([1, 2, 3], dtype='uint64')
        >>> uint64_idx
        Index([1, 2, 3], dtype='uint64')
        >>> int64_idx.equals(uint64_idx)
        True
        """
        # TODO: SNOW-1458148 implement equals
        self.to_pandas_warning()
        return self.to_pandas().equals(try_convert_index_to_native(other))

    @index_not_implemented()
    def factorize(self) -> None:
        """
        Encode the object as an enumerated type or categorical variable.

        This method is useful for obtaining a numeric representation of an
        array when all that matters is identifying distinct values.
        factorize is available as both a top-level function pandas.factorize(),
        and as a method Series.factorize() and Index.factorize().

        Parameters
        ----------
        sort : bool, default False
            Sort uniques and shuffle codes to maintain the relationship.
        use_na_sentinel: bool, default True
            If True, the sentinel -1 will be used for NaN values.
            If False, NaN values will be encoded as non-negative integers
            and will not drop the NaN from the uniques of the values.

        Returns
        -------
        A tuple of the form (codes, uniques).
        codes : ndarray
            An integer ndarray that’s an indexer into uniques.
            uniques.take(codes) will have the same values as values.

        uniques : ndarray, Index, or Categorical
            The unique valid values. When values is Categorical, uniques is a Categorical.
            When values is some other pandas object, an Index is returned.
            Otherwise, a 1-D ndarray is returned.

        See Also
        --------
        cut : Discretize a continuous-valued array.
        unique : Find the unique value in an array.
        """

    @index_not_implemented()
    def identical(self, other: Index) -> None:
        """
        Similar to equals, but checks that object attributes and types are also equal.

        Returns
        -------
        bool
            If two Index objects have equal elements and the same type True,
            otherwise False.
        """
        # TODO: SNOW-1458148 implement identical

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

    @index_not_implemented()
    def is_(self) -> None:
        """
        More flexible, faster check like ``is`` but that works through views.

        Note: this is *not* the same as ``Index.identical()``, which checks
        that metadata is also the same.

        Parameters
        ----------
        other : object
            Other object to compare against.

        Returns
        -------
        bool
            True if both have same underlying data, False otherwise.

        See Also
        --------
        Index.identical : Works like ``Index.is_`` but also checks metadata.
        """

    @index_not_implemented()
    def is_boolean(self) -> None:
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
        """
        # TODO: SNOW-1458123 implement is_boolean

    @index_not_implemented()
    def is_categorical(self) -> None:
        """
        Check if the Index holds categorical data.

        .. deprecated:: 2.0.0
              Use `isinstance(index.dtype, pd.CategoricalDtype)` instead.

        Returns
        -------
        bool
            True if the Index is categorical.

        See Also
        --------
        CategoricalIndex : Index for categorical data.
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_integer : Check if the Index only consists of integers (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_object : Check if the Index is of the object dtype. (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).
        """

    @index_not_implemented()
    def is_floating(self) -> None:
        """
        Check if the Index is a floating type.

        .. deprecated:: 2.0.0
            Use `pandas.api.types.is_float_dtype` instead

        The Index may consist of only floats, NaNs, or a mix of floats,
        integers, or NaNs.

        Returns
        -------
        bool
            Whether or not the Index only consists of only consists of floats, NaNs, or
            a mix of floats, integers, or NaNs.

        See Also
        --------
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_integer : Check if the Index only consists of integers (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_object : Check if the Index is of the object dtype. (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).
        """

    # TODO: SNOW-1458123 implement is_floating

    @index_not_implemented()
    def is_integer(self) -> None:
        """
        Check if the Index only consists of integers.

        .. deprecated:: 2.0.0
            Use `pandas.api.types.is_integer_dtype` instead.

        Returns
        -------
        bool
            Whether or not the Index only consists of integers.

        See Also
        --------
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_object : Check if the Index is of the object dtype. (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).
        """
        # TODO: SNOW-1458123 implement is_integer

    @index_not_implemented()
    def is_interval(self) -> None:
        """
        Check if the Index holds Interval objects.

        .. deprecated:: 2.0.0
            Use `isinstance(index.dtype, pd.IntervalDtype)` instead.

        Returns
        -------
        bool
            Whether or not the Index holds Interval objects.

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
        # TODO: SNOW-1458123 implement is_interval

    @index_not_implemented()
    def is_numeric(self) -> None:
        """
        Check if the Index only consists of numeric data.

        .. deprecated:: 2.0.0
            Use `pandas.api.types.is_numeric_dtype` instead.

        Returns
        -------
        bool
            Whether or not the Index only consists of numeric data.

        See Also
        --------
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_integer : Check if the Index only consists of integers (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_object : Check if the Index is of the object dtype. (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).
        """
        # TODO: SNOW-1458123 implement is_numeric

    @index_not_implemented()
    def is_object(self) -> None:
        """
        Check if the Index is of the object dtype.

        .. deprecated:: 2.0.0
           Use `pandas.api.types.is_object_dtype` instead.

        Returns
        -------
        bool
            Whether or not the Index is of the object dtype.

        See Also
        --------
        is_boolean : Check if the Index only consists of booleans (deprecated).
        is_integer : Check if the Index only consists of integers (deprecated).
        is_floating : Check if the Index is a floating type (deprecated).
        is_numeric : Check if the Index only consists of numeric data (deprecated).
        is_categorical : Check if the Index holds categorical data (deprecated).
        is_interval : Check if the Index holds Interval objects (deprecated).
        """
        # TODO: SNOW-1458123 implement is_object

    @index_not_implemented()
    def min(self) -> None:
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
        """

    @index_not_implemented()
    def max(self) -> None:
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
        """

    @index_not_implemented()
    def reindex(self) -> None:
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

        See Also
        --------
        Series.reindex : Conform Series to new index with optional filling logic.
        DataFrame.reindex : Conform DataFrame to new index with optional filling logic.
        """

    @index_not_implemented()
    def rename(self) -> None:
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
        """

    @index_not_implemented()
    def repeat(self) -> None:
        """
        Repeat elements of a Index.

        Returns a new Index where each element of the current Index
        is repeated consecutively a given number of times.

        Parameters
        ----------
        repeats : int or array or ints
            The number of repetitions for each element.
            This should be a non-negative integer.
            Repeating 0 times will return an empty Index.
        axis : None
            Must be None. It has no effect but is accepted
            for compatibility with numpy.
        """

    @index_not_implemented()
    def where(self) -> None:
        """
        Replace values where the condition is False.

        The replacement is taken from other.

        Parameters
        ----------
        cond : bool array-like with the same length as self
            Condition to select the values on.
        other : scalar, or array-like, default None
            Replacement if the condition is False.

        Returns
        -------
        pandas.Index
            A copy of self with values replaced from other
            where the condition is False.

        See Also
        --------
        Series.where : Same method for Series.
        DataFrame.where : Same method for DataFrame.
        """

    @index_not_implemented()
    def take(self) -> None:
        pass

    @index_not_implemented()
    def putmask(self) -> None:
        pass

    @index_not_implemented()
    def unique(self) -> None:
        pass

    @index_not_implemented()
    def nunique(self) -> None:
        pass

    @index_not_implemented()
    def value_counts(
        self,
        normalize: bool = False,
        sort: bool = True,
        ascending: bool = False,
        bins: Any = None,
        dropna: bool = True,
    ) -> native_pd.Series:
        # how to change the above return type to modin pandas series?
        """
        Return a Series containing counts of unique values.

        The resulting object will be in descending order so that the
        first element is the most frequently-occurring element.
        Excludes NA values by default.

        Parameters
        ----------
        normalize : bool, default False
            If True then the object returned will contain the relative
            frequencies of the unique values.
        sort : bool, default True
            Sort by frequencies when True. Preserve the order of the data when False.
        ascending : bool, default False
            Sort in ascending order.
        bins : int, optional
            Rather than count values, group them into half-open bins,
            a convenience for ``pd.cut``, only works with numeric data.
        dropna : bool, default True
            Don't include counts of NaN.

        Returns
        -------
        Series
            A series containing counts of unique values.

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
        self.to_pandas_warning()
        return self.to_pandas().value_counts(
            normalize=normalize,
            sort=sort,
            ascending=ascending,
            bins=bins,
            dropna=dropna,
        )

    @index_not_implemented()
    def set_names(
        self, names: Any, level: Any = None, inplace: bool = False
    ) -> Self | None:
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
        self.to_pandas_warning()
        if not inplace:
            return Index(
                self.to_pandas().set_names(names, level=level, inplace=inplace)
            )
        return self.to_pandas().set_names(names, level=level, inplace=inplace)

    @index_not_implemented()
    def droplevel(self) -> None:
        pass

    @index_not_implemented()
    def fillna(self) -> None:
        pass

    @index_not_implemented()
    def dropna(self) -> None:
        pass

    @index_not_implemented()
    def isna(self) -> None:
        pass

    @index_not_implemented()
    def notna(self) -> None:
        pass

    @index_not_implemented()
    def astype(self, dtype: Any, copy: bool = True) -> Index:
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
        self.to_pandas_warning()
        return Index(self.to_pandas().astype(dtype=dtype, copy=copy))

    @index_not_implemented()
    def item(self) -> None:
        pass

    @index_not_implemented()
    def map(self) -> None:
        pass

    @index_not_implemented()
    def ravel(self) -> None:
        pass

    @index_not_implemented()
    def to_series(self) -> None:
        pass

    @index_not_implemented()
    def to_frame(self) -> None:
        pass

    @index_not_implemented()
    def view(self) -> None:
        pass

    @index_not_implemented()
    def argsort(self) -> None:
        pass

    @index_not_implemented()
    def searchsorted(self) -> None:
        pass

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

    @index_not_implemented()
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

        Returns
        -------
        Index, numpy.ndarray
            Index is returned in all cases as a sorted copy of the index.
            ndarray is returned when return_indexer is True, represents the indices that the index itself was sorted by.

        See Also
        --------
        Series.sort_values : Sort values of a Series.
        DataFrame.sort_values : Sort values in a DataFrame.

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
        self.to_pandas_warning()
        ret = self.to_pandas().sort_values(
            return_indexer=return_indexer,
            ascending=ascending,
            na_position=na_position,
            key=key,
        )
        if return_indexer:
            return Index(ret[0]), ret[1]
        else:
            return Index(ret)

    @index_not_implemented()
    def shift(self) -> None:
        pass

    @index_not_implemented()
    def append(self) -> None:
        pass

    @index_not_implemented()
    def join(self) -> None:
        pass

    @index_not_implemented()
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
        self.to_pandas_warning()
        return Index(
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

        Examples
        --------
        Union matching dtypes

        >>> idx1 = pd.Index([1, 2, 3, 4])
        >>> idx2 = pd.Index([3, 4, 5, 6])
        >>> idx1.union(idx2)
        Index([1, 2, 3, 4, 5, 6], dtype='int64')

        Union mismatched dtypes

        >>> idx1 = pd.Index(['a', 'b', 'c', 'd'])
        >>> idx2 = pd.Index([1, 2, 3, 4])
        >>> idx1.union(idx2)
        Index(['a', 'b', 'c', 'd', 1, 2, 3, 4], dtype='object')
        """
        self.to_pandas_warning()
        return Index(
            self.to_pandas().union(other=try_convert_index_to_native(other), sort=sort)
        )

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

        Examples
        --------
        >>> idx1 = pd.Index([2, 1, 3, 4])
        >>> idx2 = pd.Index([3, 4, 5, 6])
        >>> idx1.difference(idx2)
        Index([1, 2], dtype='int64')
        >>> idx1.difference(idx2, sort=False)
        Index([2, 1], dtype='int64')
        """
        self.to_pandas_warning()
        return Index(
            self.to_pandas().difference(try_convert_index_to_native(other), sort=sort)
        )

    @index_not_implemented()
    def symmetric_difference(self) -> None:
        pass

    @index_not_implemented()
    def asof(self) -> None:
        pass

    @index_not_implemented()
    def asof_locs(self) -> None:
        pass

    @index_not_implemented()
    def get_indexer(self) -> None:
        pass

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

        Examples
        --------
        >>> idx = pd.Index([np.nan, 'var1', np.nan])
        >>> idx.get_indexer_for([np.nan])
        array([0, 2])
        """
        self.to_pandas_warning()
        return self.to_pandas().get_indexer_for(target=target)

    @index_not_implemented()
    def get_indexer_non_unique(self) -> None:
        pass

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

        Examples
        --------
        >>> idx = pd.Index(list('abc'))
        >>> idx
        Index(['a', 'b', 'c'], dtype='object')

        Get level values by supplying `level` as integer:

        >>> idx.get_level_values(0)
        Index(['a', 'b', 'c'], dtype='object')
        """
        self.to_pandas_warning()
        return Index(self.to_pandas().get_level_values(level=level))

    @index_not_implemented()
    def get_loc(self) -> None:
        pass

    @index_not_implemented()
    def get_slice_bound(self) -> None:
        pass

    @index_not_implemented()
    def isin(self) -> None:
        pass

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

        Examples
        --------
        This is a method on all index types. For example you can do:

        >>> idx = pd.Index(list('abcd'))
        >>> idx.slice_indexer(start='b', end='c')
        slice(1, 3, None)
        """
        self.to_pandas_warning()
        return self.to_pandas().slice_indexer(start=start, end=end, step=step)

    @index_not_implemented()
    def slice_locs(self) -> None:
        pass

    def _get_indexer_strict(self, key: Any, axis_name: str) -> tuple[Index, np.ndarray]:
        """
        Analogue to pandas.Index.get_indexer that raises if any elements are missing.
        """
        self.to_pandas_warning()
        tup = self.to_pandas()._get_indexer_strict(key=key, axis_name=axis_name)
        return Index(tup[0]), tup[1]

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
        self.to_pandas_warning()
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
        return self.to_pandas().__repr__()

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
        self.to_pandas_warning()
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
        self.to_pandas_warning()
        return self.to_pandas().__contains__(key=key)

    def __len__(self) -> int:
        """
        Return the length of the Index as an int.
        """
        self.to_pandas_warning()
        return self.to_pandas().__len__()

    def __getitem__(self, key: Any) -> np.ndarray | None | Index:
        """
        Override numpy.ndarray's __getitem__ method to work as desired.

        This function adds lists and Series as valid boolean indexers
        (ndarrays only supports ndarray with dtype=bool).

        If resulting ndim != 1, plain ndarray is returned instead of
        corresponding `Index` subclass.
        """
        self.to_pandas_warning()
        return self.to_pandas().__getitem__(key=key)

    def __setitem__(self, key: Any, value: Any) -> None:
        """
        Override numpy.ndarray's __setitem__ method to work as desired.

        We raise a TypeError because the Index values are not mutable
        """
        raise TypeError("Index does not support mutable operations")

    @property
    def str(self) -> str:
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
        self.to_pandas_warning()
        return self.to_pandas().str

    def to_pandas_warning(self) -> None:
        """
        Helper method to notify users if they are using a method that currently calls to_pandas()
        """
        WarningMessage.single_warning(
            "This method currently calls to_pandas() and materializes data. In future updates, this method will be lazily evaluated"
        )
