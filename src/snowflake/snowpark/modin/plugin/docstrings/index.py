#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""This module contains Index docstrings that override modin's docstrings."""

from __future__ import annotations

from functools import cached_property


class Index:
    def __new__():
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

    def __init__() -> None:
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

    def __getattr__():
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

    def _set_parent():
        """
        Set the parent object and its query compiler.

        Parameters
        ----------
        parent : Series or DataFrame
            The parent object that the Index is a part of.
        """

    def __add__():
        pass

    def __radd__():
        pass

    def __mul__():
        pass

    def __rmul__():
        pass

    def __neg__():
        pass

    def __sub__():
        pass

    def __rsub__():
        pass

    def __truediv__():
        pass

    def __rtruediv__():
        pass

    def __floordiv__():
        pass

    def __rfloordiv__():
        pass

    def __pow__():
        pass

    def __rpow__():
        pass

    def __mod__():
        pass

    def __rmod__():
        pass

    def __eq__():
        pass

    def __ne__():
        pass

    def __ge__():
        pass

    def __gt__():
        pass

    def __le__():
        pass

    def __lt__():
        pass

    def __or__():
        pass

    def __and__():
        pass

    def __xor__():
        pass

    def __lshift__():
        pass

    def __rshift__():
        pass

    def __rand__():
        pass

    def __ror__():
        pass

    def __rxor__():
        pass

    def __rlshift__():
        pass

    def __rrshift__():
        pass

    def to_pandas():
        """
        Convert Snowpark pandas Index to pandas Index.

        Args:
        statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
            pandas Index
                A native pandas Index representation of self
        """

    @cached_property
    def __constructor__():
        """
        Returns: Type of the instance.
        """

    @property
    def values():
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

    @property
    def is_monotonic_increasing():
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
        >>> pd.Index([1, 2, 3]).is_monotonic_increasing  # doctest: +SKIP
        True
        >>> pd.Index([1, 2, 2]).is_monotonic_increasing  # doctest: +SKIP
        True
        >>> pd.Index([1, 3, 2]).is_monotonic_increasing  # doctest: +SKIP
        False
        """

    @property
    def is_monotonic_decreasing():
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
        >>> pd.Index([3, 2, 1]).is_monotonic_decreasing  # doctest: +SKIP
        True
        >>> pd.Index([3, 2, 2]).is_monotonic_decreasing  # doctest: +SKIP
        True
        >>> pd.Index([3, 1, 2]).is_monotonic_decreasing  # doctest: +SKIP
        False
        """

    @property
    def is_unique():
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

    @property
    def has_duplicates():
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

    def unique():
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

    @property
    def dtype():
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

    @property
    def shape():
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

    def astype():
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

    @property
    def name():
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

    def _get_names():
        """
        Get names of index
        """

    def _set_names():
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

    names = property(fset=_set_names, fget=_get_names)

    def set_names():
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

    @property
    def ndim():
        """
        Number of dimensions of the underlying data, by definition 1.
        """

    @property
    def size():
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

    @property
    def nlevels():
        """
        Number of levels.
        """

    @property
    def empty():
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

    @property
    def T():
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

    def all():
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

        >>> pd.Index([1, 2, 3]).all()  # doctest: +SKIP
        True

        False, because 0 is considered False.

        >>> pd.Index([0, 1, 2]).all()  # doctest: +SKIP
        False
        """

    def any():
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
        >>> index.any()  # doctest: +SKIP
        True

        >>> index = pd.Index([0, 0, 0])
        >>> index.any()  # doctest: +SKIP
        False
        """

    def argmin():
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

    def argmax():
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

    def copy():
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

    def delete():
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

    def drop():
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

    def drop_duplicates():
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

    def duplicated():
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

    def equals():
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

    def identical():
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

    def insert():
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

    def is_boolean():
        """
        Check if the Index only consists of booleans.

        Deprecated: Use `pandas.api.types.is_bool_dtype` instead.

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

    def is_floating():
        """
        Check if the Index is a floating type.

        Deprecated: Use `pandas.api.types.is_float_dtype` instead.

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

    def is_integer():
        """
        Check if the Index only consists of integers.

        Deprecated: Use `pandas.api.types.is_integer_dtype` instead.

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

    def is_interval():
        """
        Check if the Index holds Interval objects.

        Deprecated: Use `isinstance(index.dtype, pd.IntervalDtype)` instead.

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

    def is_numeric():
        """
        Check if the Index only consists of numeric data.

        Deprecated: Use `pandas.api.types.is_numeric_dtype` instead.

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

    def is_object():
        """
        Check if the Index is of the object dtype.

        Deprecated: Use `pandas.api.types.is_object_dtype` instead.

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

    def min():
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
        >>> idx.min()  # doctest: +SKIP
        1

        >>> idx = pd.Index(['c', 'b', 'a'])
        >>> idx.min()
        'a'
        """

    def max():
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
        >>> idx.max()  # doctest: +SKIP
        3

        >>> idx = pd.Index(['c', 'b', 'a'])
        >>> idx.max()
        'c'
        """

    def reindex():
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

    def rename():
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

    def nunique():
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

        >>> s.nunique()  # doctest: +SKIP
        4
        """

    def value_counts():
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

    def item():
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

    def to_series():
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

    def to_frame():
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

    def to_numpy():
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

        See Also
        --------
        Series.array
            Get the actual data stored within.
        Index.array
            Get the actual data stored within.
        DataFrame.to_numpy
            Similar method for DataFrame.

        Notes
        -----
        The returned array will be the same up to equality (values equal in self will be equal in the returned array; likewise for values that are not equal). When self contains an ExtensionArray, the dtype may be different. For example, for a category-dtype Series, to_numpy() will return a NumPy array and the categorical dtype will be lost.

        This table lays out the different dtypes and default return types of to_numpy() for various dtypes within pandas.

        +--------------------+----------------------------------+
        | dtype              | array type                       |
        +--------------------+----------------------------------+
        | category[T]        | ndarray[T] (same dtype as input) |
        +--------------------+----------------------------------+
        | period             | ndarray[object] (Periods)        |
        +--------------------+----------------------------------+
        | interval           | ndarray[object] (Intervals)      |
        +--------------------+----------------------------------+
        | IntegerNA          | ndarray[object]                  |
        +--------------------+----------------------------------+
        | datetime64[ns]     | datetime64[ns]                   |
        +--------------------+----------------------------------+
        | datetime64[ns, tz] | ndarray[object] (Timestamps)     |
        +--------------------+----------------------------------+

        Examples
        --------
        >>> ser = pd.Series(pd.Categorical(['a', 'b', 'a']))  # doctest: +SKIP
        >>> ser.to_numpy()  # doctest: +SKIP
        array(['a', 'b', 'a'], dtype=object)

        Specify the dtype to control how datetime-aware data is represented. Use dtype=object to return an ndarray of pandas Timestamp objects, each with the correct tz.

        >>> ser = pd.Series(pd.date_range('2000', periods=2, tz="CET"))
        >>> ser.to_numpy(dtype=object)
        array([Timestamp('2000-01-01 00:00:00+0100', tz='UTC+01:00'),
               Timestamp('2000-01-02 00:00:00+0100', tz='UTC+01:00')],
              dtype=object)

        Or dtype='datetime64[ns]' to return an ndarray of native datetime64 values. The values are converted to UTC and the timezone info is dropped.

        >>> ser.to_numpy(dtype="datetime64[ns]")
        array(['1999-12-31T23:00:00.000000000', '2000-01-01T23:00:00...'],
              dtype='datetime64[ns]')
        """

    def fillna():
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

            Deprecated parameter.

        Returns
        -------
        Index

        See Also
        --------
        DataFrame.fillna : Fill NaN values of a DataFrame.
        Series.fillna : Fill NaN Values of a Series.
        """

    def dropna():
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

    def isna():
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

    def notna():
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

    def hasnans():
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

    def tolist():
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

    to_list = tolist

    def sort_values():
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

    def append():
        """
        Append a collection of Index options together.

        Parameters
        ----------
        other : Index or list/tuple of indices

        Returns
        -------
        Index
        """

    def join():
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

    def intersection():
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

    def union():
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

    def difference():
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

    def get_indexer_for():
        """
        Guaranteed return of an indexer even when non-unique.

        This dispatches to get_indexer or get_indexer_non_unique
        as appropriate.

        Returns
        -------
        np.ndarray[np.intp]
            List of indices.
        """

    def _get_indexer_strict():
        """
        Analogue to pandas.Index.get_indexer that raises if any elements are missing.
        """

    def get_level_values():
        """
        Return an Index of values for requested level.

        This is primarily useful to get an individual level of values from a
        MultiIndex, but is provided on Index as well for compatibility.

        Parameters
        ----------
        level : Any
            It is either the integer position or the name of the level.

        Returns
        -------
        Index
            self, since self only has one level

        Notes
        -----
        For Index, level should be 0, -1, or the name of the index, since there
        is only one level.

        Examples
        --------
        >>> idx = pd.Index(['a', 'b', 'c'], name='index')
        >>> idx.get_level_values(0)
        Index(['a', 'b', 'c'], dtype='object', name='index')
        >>> idx.get_level_values('index')
        Index(['a', 'b', 'c'], dtype='object', name='index')
        """

    def isin():
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

    def slice_indexer():
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

    @property
    def array():
        """
        return the array of values
        """

    def _summary():
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

    def __array__():
        """
        The array interface, return the values.
        """

    def __repr__():
        """
        Return a string representation for this object.
        """

    def __iter__():
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

    def __contains__():
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

    def __len__():
        """
        Return the length of the Index as an int.
        """

    def __getitem__():
        """
        Reuse series iloc to implement getitem for index.
        """

    def __setitem__():
        """
        Override numpy.ndarray's __setitem__ method to work as desired.

        We raise a TypeError because the Index values are not mutable
        """

    @property
    def str():
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

    def _to_datetime():
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
