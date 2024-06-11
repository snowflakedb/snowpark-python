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
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage


class Index:
    def __init__(
        self,
        # Any should be replaced with SnowflakeQueryCompiler when possible (linter won't allow it now)
        data: ArrayLike | Any = None,
        dtype: str | np.dtype | ExtensionDtype | None = None,
        copy: bool = False,
        name: object = None,
        tupleize_cols: bool = True,
        convert_to_lazy: bool = True,
    ) -> None:
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
        convert_to_lazy : bool (default: True)
            When True, create a lazy index object, otherwise, create an index object that locally saves a pandas index
            We set convert_to_lazy as False when calling df.columns

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
        from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
            SnowflakeQueryCompiler,
        )

        self.is_lazy = convert_to_lazy

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

    def is_lazy_check(func: Any) -> Any:
        """
        Decorator method for separating function calls for lazy indexes and non-lazy (column) indexes
        """

        def check_lazy(*args: Any, **kwargs: Any) -> Any:
            func_name = func.__name__

            # If the index is lazy, call the method and return
            if args[0].is_lazy:
                returned_value = func(*args, **kwargs)
                return returned_value
            else:
                # If the index is not lazy, get the cached native index and call the function
                native_index = args[0]._index
                native_func = getattr(native_index, func_name)

                # If the function is a property, we will get a non-callable, so we just return it
                # Examples of this are values or dtype
                if not callable(native_func):
                    return native_func

                # Remove the first argument in args, because it is self and we don't need it
                args = args[1:]
                returned_value = native_func(*args, **kwargs)

                # If we return a native Index, we need to convert this to a modin index
                # Examples of this are astype and copy
                if isinstance(returned_value, native_pd.Index):
                    returned_value = Index(returned_value, convert_to_lazy=False)
                # Some methods also return a tuple with an Index, so convert that tuples first item to an index
                # Examples of this are _get_indexer_strict and sort_values
                elif isinstance(returned_value, tuple) and isinstance(
                    returned_value[0], native_pd.Index
                ):
                    returned_value = (
                        Index(returned_value[0], convert_to_lazy=False),
                        returned_value[1],
                    )
                return returned_value

        return check_lazy

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
    @is_lazy_check
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
    @is_lazy_check
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
        ...                 "Watermelon"]).astype("category")
        >>> idx.is_unique
        False

        >>> idx = pd.Index(["Orange", "Apple",
        ...                 "Watermelon"]).astype("category")
        >>> idx.is_unique
        True
        """
        WarningMessage.index_to_pandas_warning("is_unique")
        return self.to_pandas().is_unique

    @property
    @is_lazy_check
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
        ...                 "Watermelon"]).astype("category")
        >>> idx.has_duplicates
        True

        >>> idx = pd.Index(["Orange", "Apple",
        ...                 "Watermelon"]).astype("category")
        >>> idx.has_duplicates
        False
        """
        return not self.is_unique

    @is_lazy_check
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
        Series.unique : Return unique values of Series object.

        Examples
        --------
        >>> idx = pd.Index([1, 1, 2, 3, 3])
        >>> idx.unique()
        Index([1, 2, 3], dtype='int64')
        """
        WarningMessage.index_to_pandas_warning("unique")
        return Index(self.to_pandas().unique(level=level))

    @property
    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("dtype")
        return self.to_pandas().dtype

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("astype")
        return Index(self.to_pandas().astype(dtype=dtype, copy=copy))

    @property
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
        WarningMessage.index_to_pandas_warning("name")
        return self.to_pandas().name

    @name.setter
    def name(self, value: Hashable) -> None:
        """
        Set Index name.
        """
        WarningMessage.index_to_pandas_warning("name")
        self.to_pandas().name = value

    def _get_names(self) -> FrozenList:
        """
        Get names of index
        """
        WarningMessage.index_to_pandas_warning("_get_names")
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
        WarningMessage.index_to_pandas_warning("_set_names")
        self.to_pandas()._set_names(values)

    names = property(fset=_set_names, fget=_get_names)

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
        WarningMessage.index_to_pandas_warning("set_names")
        if not inplace:
            return Index(
                self.to_pandas().set_names(names, level=level, inplace=inplace)
            )
        return self.to_pandas().set_names(names, level=level, inplace=inplace)

    @property
    @is_lazy_check
    def nlevels(self) -> int:
        """
        Number of levels.
        """
        return 1

    @is_lazy_check
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
            Index refer to new object which is a copy of this object.

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
        WarningMessage.index_to_pandas_warning("copy")
        return Index(self.to_pandas().copy(deep=deep, name=name))

    @is_lazy_check
    def drop(
        self,
        labels: Any,
        errors: Literal["ignore", "raise"] = "raise",
    ) -> Index:
        """
        Make new Index with passed list of labels deleted.

        Parameters
        ----------
        labels : array-like or scalar
        errors : {'ignore', 'raise'}, default 'raise'
            If 'ignore', suppress error and existing labels are dropped.

        Returns
        -------
        Index
            The index created will have the same type as self.

        Raises
        ------
        KeyError
            If not all of the labels are found in the selected axis

        Examples
        --------
        >>> idx = pd.Index(['a', 'b', 'c'])
        >>> idx.drop(['a'])
        Index(['b', 'c'], dtype='object')
        """
        WarningMessage.index_to_pandas_warning("drop")
        return Index(self.to_pandas().drop(labels=labels, errors=errors))

    @is_lazy_check
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
        >>> idx.duplicated()
        array([False, False,  True, False,  True])

        which is equivalent to

        >>> idx.duplicated(keep='first')
        array([False, False,  True, False,  True])

        By using 'last', the last occurrence of each set of duplicated values
        is set on False and all others on True:

        >>> idx.duplicated(keep='last')
        array([ True, False,  True, False, False])

        By setting keep on ``False``, all duplicates are True:

        >>> idx.duplicated(keep=False)
        array([ True, False,  True, False,  True])
        """
        WarningMessage.index_to_pandas_warning("duplicated")
        return self.to_pandas().duplicated(keep=keep)

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("equals")
        return self.to_pandas().equals(try_convert_index_to_native(other))

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("value_counts")
        return self.to_pandas().value_counts(
            normalize=normalize,
            sort=sort,
            ascending=ascending,
            bins=bins,
            dropna=dropna,
        )

    @is_lazy_check
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

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("sort_values")
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

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("intersection")
        return Index(
            self.to_pandas().intersection(
                other=try_convert_index_to_native(other), sort=sort
            )
        )

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("union")
        return Index(
            self.to_pandas().union(other=try_convert_index_to_native(other), sort=sort)
        )

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("difference")
        return Index(
            self.to_pandas().difference(try_convert_index_to_native(other), sort=sort)
        )

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("get_indexer_for")
        return self.to_pandas().get_indexer_for(target=target)

    @is_lazy_check
    def _get_indexer_strict(self, key: Any, axis_name: str) -> tuple[Index, np.ndarray]:
        """
        Analogue to pandas.Index.get_indexer that raises if any elements are missing.
        """
        WarningMessage.index_to_pandas_warning("_get_indexer_strict")
        tup = self.to_pandas()._get_indexer_strict(key=key, axis_name=axis_name)
        return Index(tup[0]), tup[1]

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("get_level_values")
        return Index(self.to_pandas().get_level_values(level=level))

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("slice_indexer")
        return self.to_pandas().slice_indexer(start=start, end=end, step=step)

    @property
    @is_lazy_check
    def array(self) -> ExtensionArray:
        """
        return the array of values
        """
        return self.to_pandas().array

    @is_lazy_check
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

    @is_lazy_check
    def __array__(self, dtype: Any = None) -> np.ndarray:
        """
        The array interface, return the values.
        """
        return self.to_pandas().__array__(dtype=dtype)

    @is_lazy_check
    def __repr__(self) -> str:
        """
        Return a string representation for this object.
        """
        WarningMessage.index_to_pandas_warning("__repr__")
        return self.to_pandas().__repr__()

    @is_lazy_check
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

    @is_lazy_check
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

    @is_lazy_check
    def __len__(self) -> int:
        """
        Return the length of the Index as an int.
        """
        WarningMessage.index_to_pandas_warning("__len__")
        return self.to_pandas().__len__()

    @is_lazy_check
    def __getitem__(self, key: Any) -> np.ndarray | None | Index:
        """
        Override numpy.ndarray's __getitem__ method to work as desired.

        This function adds lists and Series as valid boolean indexers
        (ndarrays only supports ndarray with dtype=bool).

        If resulting ndim != 1, plain ndarray is returned instead of
        corresponding `Index` subclass.
        """
        WarningMessage.index_to_pandas_warning("__getitem__")
        return self.to_pandas().__getitem__(key=key)

    @is_lazy_check
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
        WarningMessage.index_to_pandas_warning("str")
        return self.to_pandas().str
