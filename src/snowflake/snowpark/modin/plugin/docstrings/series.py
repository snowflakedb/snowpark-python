#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This module contains Series docstrings that override modin's docstrings."""

from textwrap import dedent

import pandas
from pandas.util._decorators import doc

from snowflake.snowpark.modin.plugin.docstrings.shared_docs import (
    _doc_binary_op,
    _shared_docs,
)
from snowflake.snowpark.modin.utils import _create_operator_docstring

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


class Series:
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

    @property
    def name():
        """
        Return the name of the Series.

        The name of a Series becomes its index or column name if it is used to form a DataFrame.
        It is also used whenever displaying the Series using the interpreter.
        """

    @_doc_binary_op(operation="addition", bin_op="add")
    def __add__():
        pass

    @_doc_binary_op(operation="addition", bin_op="radd", right="left")
    def __radd__():
        pass

    @_doc_binary_op(operation="union", bin_op="and", right="other")
    def __and__():
        pass

    @_doc_binary_op(operation="union", bin_op="and", right="other")
    def __rand__():
        pass

    def __array__():
        """
        Return the values as a NumPy array.
        """

    def __contains__():
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

    def __copy__():
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

    def __deepcopy__():
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

    def __delitem__():
        """
        Delete item identified by `key` label.

        Parameters
        ----------
        key : hashable
            Key to delete.
        """

    def __divmod__():
        pass

    @_doc_binary_op(
        operation="integer division and modulo",
        bin_op="divmod",
        right="left",
        returns="tuple of two Series",
    )
    def __rdivmod__():
        pass

    @_doc_binary_op(operation="integer division", bin_op="floordiv")
    def __floordiv__():
        pass

    @_doc_binary_op(operation="integer division", bin_op="floordiv")
    def __rfloordiv__():
        pass

    def __getattr__():
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

    def abs():
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

    def __neg__():
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

    def __iter__():
        """
        Return an iterator of the values.

        Returns
        -------
        iterable
        """

    @_doc_binary_op(operation="modulo", bin_op="mod")
    def __mod__():
        pass

    @_doc_binary_op(operation="modulo", bin_op="mod", right="left")
    def __rmod__():
        pass

    @_doc_binary_op(operation="multiplication", bin_op="mul")
    def __mul__():
        pass

    @_doc_binary_op(operation="multiplication", bin_op="mul", right="left")
    def __rmul__():
        pass

    @_doc_binary_op(operation="disjunction", bin_op="or", right="other")
    def __or__():
        pass

    @_doc_binary_op(operation="disjunction", bin_op="or", right="other")
    def __ror__():
        pass

    @_doc_binary_op(operation="exclusive or", bin_op="xor", right="other")
    def __xor__():
        pass

    @_doc_binary_op(operation="exclusive or", bin_op="xor", right="other")
    def __rxor__():
        pass

    @_doc_binary_op(operation="exponential power", bin_op="pow")
    def __pow__():
        pass

    @_doc_binary_op(operation="exponential power", bin_op="pow", right="left")
    def __rpow__():
        pass

    def __repr__():
        """
        Return a string representation for a particular Series.

        Returns
        -------
        str
        """

    def __round__():
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

    def __setitem__():
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

    @_doc_binary_op(operation="subtraction", bin_op="sub")
    def __sub__():
        pass

    @_doc_binary_op(operation="subtraction", bin_op="sub", right="left")
    def __rsub__():
        pass

    @_doc_binary_op(operation="floating division", bin_op="truediv")
    def __truediv__():
        pass

    @_doc_binary_op(operation="floating division", bin_op="truediv", right="left")
    def __rtruediv__():
        pass

    __iadd__ = __add__
    __imul__ = __add__
    __ipow__ = __pow__
    __isub__ = __sub__
    __itruediv__ = __truediv__

    @_create_operator_docstring(pandas.core.series.Series.add, overwrite_existing=True)
    def add():
        pass

    @_create_operator_docstring(pandas.core.series.Series.radd, overwrite_existing=True)
    def radd():
        pass

    def add_prefix():
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

    def add_suffix():
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

    def drop():
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
    def aggregate():
        pass

    agg = aggregate

    def apply():
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
        1. When ``func`` has a type annotation for its return value, the result will be cast
        to the corresponding dtype. When no type annotation is provided, data will be converted
        to VARIANT type in Snowflake, and the result will have ``dtype=object``. In this case, the return value must
        be JSON-serializable, which can be a valid input to ``json.dumps`` (e.g., ``dict`` and
        ``list`` objects are JSON-serializable, but ``bytes`` and ``datetime.datetime`` objects
        are not). The return type hint is used only when ``func`` is a series-to-scalar function.


        2. Under the hood, we use `Snowflake Vectorized Python UDFs <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-batch>`_.
        to implement apply() method. You can find type mappings from Snowflake SQL types to pandas
        dtypes `here <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-batch#type-support>`_.

        3. Snowflake supports two types of NULL values in variant data: `JSON NULL and SQL NULL <https://docs.snowflake.com/en/user-guide/semistructured-considerations#null-values>`_.
        When no type annotation is provided and Variant data is returned, Python ``None`` is translated to
        JSON NULL, and all other pandas missing values (np.nan, pd.NA, pd.NaT) are translated to SQL NULL.

        4. For working with 3rd-party-packages see :func:`DataFrame.apply <snowflake.snowpark.modin.pandas.DataFrame.apply>`.
        """

    def argmax():
        """
        Return int position of the largest value in the Series.
        """

    def argmin():
        """
        Return int position of the smallest value in the Series.
        """

    def argsort():
        """
        Return the integer indices that would sort the Series values.
        """

    def autocorr():
        """
        Compute the lag-N autocorrelation.
        """

    def between():
        """
        Return boolean Series equivalent to left <= series <= right.
        """

    def compare():
        """
        Compare to another Series and show the differences.
        """

    def corr():
        """
        Compute correlation with `other` Series, excluding missing values.
        """

    def count():
        """
        Return number of non-NA/null observations in the Series.

        Returns
        -------
        int
            Number of non-null values in the Series.

        See Also
        --------
        DataFrame.count : Count non-NA cells for each column or row.

        Examples
        --------
        >>> s = pd.Series([0.0, 1.0, np.nan])
        >>> s.count()
        2
        """

    def cov():
        """
        Compute covariance with Series, excluding missing values.
        """

    def describe():
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

    def diff():
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

    def divmod():
        """
        Return Integer division and modulo of series and `other`, element-wise (binary operator `divmod`).
        Not implemented
        """

    def dot():
        """
        Compute the dot product between the Series and the columns of `other`.
        """

    def drop_duplicates():
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

    def dropna():
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

        >>> ser = pd.Series([np.nan, 2, pd.NaT, '', None, 'I stay'])
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

    def duplicated():
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

    @_create_operator_docstring(pandas.core.series.Series.eq, overwrite_existing=True)
    def eq():
        pass

    def equals():
        """
        Test whether two series contain the same elements.

        This function allows two Series to be compared against
        each other to see if they have the same shape and elements. NaNs in
        the same location are considered equal.

        The row/column index do not need to have the same type, as long
        as the values are considered equal. Corresponding columns and
        index must be of the same dtype. Note: int variants (int8, int16 etc) are
        considered equal dtype i.e int8 == int16. Similarly, float variants (float32,
        float64 etc) are considered equal dtype.

        Parameters
        ----------
        other : Series
            The other Series to be compared with the first.

        Returns
        -------
        bool
            True if all elements are the same in both series, False
            otherwise.

        See Also
        --------
        Series.eq : Compare two Series objects of the same length
            and return a Series where each element is True if the element
            in each Series is equal, False otherwise.
        DataFrame.eq : Compare two DataFrame objects of the same shape and
            return a DataFrame where each element is True if the respective
            element in each DataFrame is equal, False otherwise.
        testing.assert_series_equal : Raises an AssertionError if left and
            right are not equal. Provides an easy interface to ignore
            inequality in dtypes, indexes and precision among others.
        testing.assert_frame_equal : Like assert_series_equal, but targets
            DataFrames.
        numpy.array_equal : Return True if two arrays have the same shape
            and elements, False otherwise.

        Examples
        --------
        >>> series = pd.Series([1, 2, 3], name=99)
        >>> series
        0    1
        1    2
        2    3
        Name: 99, dtype: int64

        Series 'series' and 'exactly_equal' have the same types and values for
        their elements and names, which will return True.

        >>> exactly_equal = pd.Series([1, 2, 3], name=99)
        >>> exactly_equal
        0    1
        1    2
        2    3
        Name: 99, dtype: int64
        >>> series.equals(exactly_equal)
        True

        Series 'series' and 'different_column_type' have the same element
        types and values, but have different types for names,
        which will still return True.

        >>> different_column_type = pd.Series([1, 2, 3], name=99.0)
        >>> different_column_type
        0    1
        1    2
        2    3
        Name: 99.0, dtype: int64
        >>> series.equals(different_column_type)
        True

        Series 'series' and 'different_data_type' have different types for the
        same values for their elements, and will return False even though
        their names are the same values and types.

        >>> different_data_type = pd.Series([1.0, 2.0, 3.0], name=99)
        >>> different_data_type
        0    1.0
        1    2.0
        2    3.0
        Name: 99, dtype: float64
        >>> series.equals(different_data_type)
        False
        """

    def explode():
        """
        Transform each element of a list-like to a row.
        """

    def factorize():
        """
        Encode the object as an enumerated type or categorical variable.
        """

    def case_when():
        """
        Replace values where the conditions are True.

        Parameters
        ----------
        caselist : A list of tuples of conditions and expected replacements
            Takes the form:  ``(condition0, replacement0)``,
            ``(condition1, replacement1)``, ... .
            ``condition`` should be a 1-D boolean array-like object
            or a callable. If ``condition`` is a callable,
            it is computed on the Series
            and should return a boolean Series or array.
            The callable must not change the input Series
            (though pandas doesn`t check it). ``replacement`` should be a
            1-D array-like object, a scalar or a callable.
            If ``replacement`` is a callable, it is computed on the Series
            and should return a scalar or Series. The callable
            must not change the input Series
            (though pandas doesn`t check it).

            .. versionadded:: 2.2.0

        Returns
        -------
        Series

        See Also
        --------
        Series.mask : Replace values where the condition is True.

        Examples
        --------
        >>> c = pd.Series([6, 7, 8, 9], name='c')
        >>> a = pd.Series([0, 0, 1, 2])
        >>> b = pd.Series([0, 3, 4, 5])

        >>> c.case_when(caselist=[(a.gt(0), a),  # condition, replacement
        ...                       (b.gt(0), b)])
        0    6
        1    3
        2    1
        3    2
        Name: c, dtype: int64
        """

    def fillna():
        """
        Fill NA/NaN values using the specified method.

        Parameters
        ----------
        value : scalar, dict, Series, or DataFrame
            Value to use to fill holes (e.g. 0), alternately a
            dict/Series/DataFrame of values specifying which value to use for
            each index (for a Series) or column (for a DataFrame).  Values not
            in the dict/Series/DataFrame will not be filled. This value cannot
            be a list.
        method : {{'backfill', 'bfill', 'ffill', None}}, default None
            Method to use for filling holes in reindexed Series:

            * ffill: propagate last valid observation forward to next valid.
            * backfill / bfill: use next valid observation to fill gap.

            .. deprecated:: 2.1.0
                Use ffill or bfill instead.

        axis : {axes_single_arg}
            Axis along which to fill missing values. For `Series`
            this parameter is unused and defaults to 0.
        inplace : bool, default False
            If True, fill in-place. Note: this will modify any
            other views on this object (e.g., a no-copy slice for a column in a
            DataFrame).
        limit : int, default None
            If method is specified, this is the maximum number of consecutive
            NaN values to forward/backward fill. In other words, if there is
            a gap with more than this number of consecutive NaNs, it will only
            be partially filled. If method is not specified, this is the
            maximum number of entries along the entire axis where NaNs will be
            filled. Must be greater than 0 if not None.
        downcast : dict, default is None
            A dict of item->dtype of what to downcast if possible,
            or the string 'infer' which will try to downcast to an appropriate
            equal type (e.g. float64 to int64 if possible).

            .. deprecated:: 2.2.0

        Returns
        -------
        {klass} or None
            Object with missing values filled or None if ``inplace=True``.

        See Also
        --------
        ffill : Fill values by propagating the last valid observation to next valid.
        bfill : Fill values by using the next valid observation to fill the gap.
        interpolate : Fill NaN values using interpolation.
        reindex : Conform object to new index.
        asfreq : Convert TimeSeries to specified frequency.

        Examples
        --------
        >>> df = pd.DataFrame([[np.nan, 2, np.nan, 0],
        ...                    [3, 4, np.nan, 1],
        ...                    [np.nan, np.nan, np.nan, np.nan],
        ...                    [np.nan, 3, np.nan, 4]],
        ...                   columns=list("ABCD"))
        >>> df
             A    B   C    D
        0  NaN  2.0 NaN  0.0
        1  3.0  4.0 NaN  1.0
        2  NaN  NaN NaN  NaN
        3  NaN  3.0 NaN  4.0

        Replace all NaN elements with 0s.

        >>> df.fillna(0)
             A    B    C    D
        0  0.0  2.0  0.0  0.0
        1  3.0  4.0  0.0  1.0
        2  0.0  0.0  0.0  0.0
        3  0.0  3.0  0.0  4.0

        Replace all NaN elements in column 'A', 'B', 'C', and 'D', with 0, 1,
        2, and 3 respectively.

        >>> values = {"A": 0, "B": 1, "C": 2, "D": 3}
        >>> df.fillna(value=values)
             A    B    C    D
        0  0.0  2.0  2.0  0.0
        1  3.0  4.0  2.0  1.0
        2  0.0  1.0  2.0  3.0
        3  0.0  3.0  2.0  4.0

        Only replace the first NaN element.

        >>> df.fillna(method="ffill", limit=1)
             A    B   C    D
        0  NaN  2.0 NaN  0.0
        1  3.0  4.0 NaN  1.0
        2  3.0  4.0 NaN  1.0
        3  NaN  3.0 NaN  4.0

        When filling using a DataFrame, replacement happens along
        the same column names and same indices

        >>> df2 = pd.DataFrame(np.zeros((4, 4)), columns=list("ABCE"))
        >>> df.fillna(df2)
             A    B    C    D
        0  0.0  2.0  0.0  0.0
        1  3.0  4.0  0.0  1.0
        2  0.0  0.0  0.0  NaN
        3  0.0  3.0  0.0  4.0

        Note that column D is not affected since it is not present in df2.

        Notes
        -----
        `limit` parameter is only supported when using `method` parameter.
        """

    @_create_operator_docstring(
        pandas.core.series.Series.floordiv, overwrite_existing=True
    )
    def floordiv():
        pass

    @_create_operator_docstring(pandas.core.series.Series.ge, overwrite_existing=True)
    def ge():
        pass

    def groupby():
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
        """

    @_create_operator_docstring(pandas.core.series.Series.gt, overwrite_existing=True)
    def gt():
        pass

    def hist():
        """
        Draw histogram of the input series using matplotlib.
        """

    def idxmax():
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

    def idxmin():
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

    def info():
        pass

    def interpolate():
        """
        Fill NaN values using an interpolation method.
        """

    def item():
        """
        Return the first element of the underlying data as a Python scalar.
        """

    def items():
        """
        Lazily iterate over (index, value) tuples.
        """

    def keys():
        """
        Return alias for index.
        """

    def kurt():
        """
        Return unbiased kurtosis over requested axis.
        """

    kurtosis = kurt

    @_create_operator_docstring(pandas.core.series.Series.le, overwrite_existing=True)
    def le():
        pass

    @_create_operator_docstring(pandas.core.series.Series.lt, overwrite_existing=True)
    def lt():
        pass

    def map():
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

        >>> s.map({'cat': 'kitten', 'dog': 'puppy'})  # doctest: +SKIP
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

        >>> s.map('I am a {}'.format, na_action='ignore')  # doctest: +SKIP
        0       I am a cat
        1       I am a dog
        2             None
        3    I am a rabbit
        dtype: object

        Note that in the above example, the missing value in Snowflake is NULL,
        it is mapped to ``None`` in a string/object column.
        """

    def mask():
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

    def memory_usage():
        """
        Return the memory usage of the Series.
        """

    @_create_operator_docstring(pandas.core.series.Series.mod, overwrite_existing=True)
    def mod():
        pass

    def mode():
        """
        Return the mode(s) of the Series.
        """

    @_create_operator_docstring(pandas.core.series.Series.mul, overwrite_existing=True)
    def mul():
        pass

    @_create_operator_docstring(pandas.core.series.Series.rmul, overwrite_existing=True)
    def rmul():
        pass

    @_create_operator_docstring(pandas.core.series.Series.ne, overwrite_existing=True)
    def ne():
        pass

    def nlargest():
        """
        Return the largest `n` elements.

        Parameters
        ----------
        n : int, default 5
            Return this many descending sorted values.
        keep : {'first', 'last', 'all'}, default 'first'
            When there are duplicate values that cannot all fit in a
            Series of `n` elements:

            - ``first`` : return the first `n` occurrences in order
              of appearance.
            - ``last`` : return the last `n` occurrences in reverse
              order of appearance.
            - ``all`` : keep all occurrences. This can result in a Series of
              size larger than `n`.

        Returns
        -------
        Series
            The `n` largest values in the Series, sorted in decreasing order.

        See Also
        --------
        Series.nsmallest: Get the `n` smallest elements.
        Series.sort_values: Sort Series by values.
        Series.head: Return the first `n` rows.

        Examples
        --------
        >>> countries_population = {"Italy": 59000000, "France": 65000000,
        ...                         "Malta": 434000, "Maldives": 434000,
        ...                         "Brunei": 434000, "Iceland": 337000,
        ...                         "Nauru": 11300, "Tuvalu": 11300,
        ...                         "Anguilla": 11300, "Montserrat": 5200}
        >>> s = pd.Series(countries_population)
        >>> s
        Italy         59000000
        France        65000000
        Malta           434000
        Maldives        434000
        Brunei          434000
        Iceland         337000
        Nauru            11300
        Tuvalu           11300
        Anguilla         11300
        Montserrat        5200
        dtype: int64

        The `n` largest elements where ``n=5`` by default.

        >>> s.nlargest()
        France      65000000
        Italy       59000000
        Malta         434000
        Maldives      434000
        Brunei        434000
        dtype: int64

        The `n` largest elements where ``n=3``. Default `keep` value is 'first'
        so Malta will be kept.

        >>> s.nlargest(3)
        France    65000000
        Italy     59000000
        Malta       434000
        dtype: int64

        The `n` largest elements where ``n=3`` and keeping the last duplicates.
        Brunei will be kept since it is the last with value 434000 based on
        the index order.

        >>> s.nlargest(3, keep='last')
        France    65000000
        Italy     59000000
        Brunei      434000
        dtype: int64

        The `n` largest elements where ``n=3`` with all duplicates kept. Note
        that the returned Series has five elements due to the three duplicates.

        >>> s.nlargest(3, keep='all')  # doctest: +SKIP
        France      65000000
        Italy       59000000
        Malta         434000
        Maldives      434000
        Brunei        434000
        dtype: int64
        """

    def nsmallest():
        """
        Return the smallest `n` elements.

        Parameters
        ----------
        n : int, default 5
            Return this many ascending sorted values.
        keep : {'first', 'last', 'all'}, default 'first'
            When there are duplicate values that cannot all fit in a
            Series of `n` elements:

            - ``first`` : return the first `n` occurrences in order
              of appearance.
            - ``last`` : return the last `n` occurrences in reverse
              order of appearance.
            - ``all`` : keep all occurrences. This can result in a Series of
              size larger than `n`.

        Returns
        -------
        Series
            The `n` smallest values in the Series, sorted in increasing order.

        See Also
        --------
        Series.nlargest: Get the `n` largest elements.
        Series.sort_values: Sort Series by values.
        Series.head: Return the first `n` rows.

        Examples
        --------
        >>> countries_population = {"Italy": 59000000, "France": 65000000,
        ...                         "Brunei": 434000, "Malta": 434000,
        ...                         "Maldives": 434000, "Iceland": 337000,
        ...                         "Nauru": 11300, "Tuvalu": 11300,
        ...                         "Anguilla": 11300, "Montserrat": 5200}
        >>> s = pd.Series(countries_population)
        >>> s
        Italy         59000000
        France        65000000
        Brunei          434000
        Malta           434000
        Maldives        434000
        Iceland         337000
        Nauru            11300
        Tuvalu           11300
        Anguilla         11300
        Montserrat        5200
        dtype: int64

        The `n` smallest elements where ``n=5`` by default.

        >>> s.nsmallest()
        Montserrat      5200
        Nauru          11300
        Tuvalu         11300
        Anguilla       11300
        Iceland       337000
        dtype: int64

        The `n` smallest elements where ``n=3``. Default `keep` value is
        'first' so Nauru and Tuvalu will be kept.

        >>> s.nsmallest(3)
        Montserrat     5200
        Nauru         11300
        Tuvalu        11300
        dtype: int64

        The `n` smallest elements where ``n=3`` and keeping the last
        duplicates. Anguilla and Tuvalu will be kept since they are the last
        with value 11300 based on the index order.

        >>> s.nsmallest(3, keep='last')
        Montserrat     5200
        Anguilla      11300
        Tuvalu        11300
        dtype: int64

        The `n` smallest elements where ``n=3`` with all duplicates kept. Note
        that the returned Series has four elements due to the three duplicates.

        >>> s.nsmallest(3, keep='all')  # doctest: +SKIP
        Montserrat     5200
        Nauru         11300
        Tuvalu        11300
        Anguilla      11300
        dtype: int64
        """

    def set_axis():
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

    def unstack():
        """
        Unstack, also known as pivot, Series with MultiIndex to produce DataFrame.
        """

    @property
    def plot():
        """
        Make plot of Series.
        """

    @_create_operator_docstring(pandas.core.series.Series.pow, overwrite_existing=True)
    def pow():
        pass

    def prod():
        pass

    product = prod

    def ravel():
        """
        Return the flattened underlying data as an ndarray.
        """

    def reindex():
        """
        Conform Series to new index with optional filling logic.

        Places NA/NaN in locations having no value in the previous index. A new object is produced
        unless the new index is equivalent to the current one and copy=False.

        Parameters
        ----------
        index : array-like, optional
            New labels for the index.
        axis : int or str, optional
            Unused.
        method :  {None, "backfill"/"bfill", "pad"/"ffill", "nearest"}, default: None
            Method to use for filling holes in reindexed DataFrame.

            * None (default): don't fill gaps
            * pad / ffill: Propagate last valid observation forward to next valid.
            * backfill / bfill: Use next valid observation to fill gap.
            * nearest: Use nearest valid observations to fill gap. Unsupported by Snowpark pandas.

        copy : bool, default True
            Return a new object, even if the passed indexes are the same.

        level : int or name
            Broadcast across a level, matching Index values on the passed MultiIndex level.

        fill_value : scalar, default np.nan
            Value to use for missing values. Defaults to NaN, but can be any “compatible” value.

        limit : int, default None
            Maximum number of consecutive elements to forward or backward fill.

        tolerance : optional
            Maximum distance between original and new labels for inexact matches.
            The values of the index at the matching locations most satisfy the
            equation abs(index[indexer] - target) <= tolerance. Unsupported by
            Snowpark pandas.

        Returns
        -------
        Series
            Series with changed index.

        Notes
        -----
        For axis 0, Snowpark pandas' behaviour diverges from vanilla pandas in order
        to maintain Snowpark's lazy execution paradigm. The behaviour changes are as follows:

            * Snowpark pandas does not error if the existing index is not monotonically increasing
              or decreasing when `method` is specified for filling. It instead assumes that
              the index is monotonically increasing, performs the reindex, and fills the values
              as though the index is sorted (which involves sorting internally).
            * Snowpark pandas does not error out if there are duplicates - they are included in the
              output.
            * Snowpark pandas does not error if a `limit` value is passed and the new index is not
              monotonically increasing or decreasing - instead, it reindexes, sorts the new index,
              fills using limit, and then reorders the data to be in the correct order (the order
              of the target labels passed in to the method).

        For axis 1, Snowpark pandas' error checking remains the same as vanilla pandas.

        MultiIndex is currently unsupported.

        ``method="nearest"`` is currently unsupported.

        Examples
        --------
        Create a dataframe with some fictional data.

        >>> index = ['Firefox', 'Chrome', 'Safari', 'IE10', 'Konqueror']
        >>> df = pd.DataFrame({'http_status': [200, 200, 404, 404, 301],
        ...             'response_time': [0.04, 0.02, 0.07, 0.08, 1.0]},
        ...             index=index)
        >>> df
                   http_status  response_time
        Firefox            200           0.04
        Chrome             200           0.02
        Safari             404           0.07
        IE10               404           0.08
        Konqueror          301           1.00

        Create a new index and reindex the dataframe. By default, values in the new index
        that do not have corresponding records in the dataframe are assigned NaN.

        >>> new_index = ['Safari', 'Iceweasel', 'Comodo Dragon', 'IE10',
        ...              'Chrome']
        >>> df.reindex(new_index)
                       http_status  response_time
        Safari               404.0           0.07
        Iceweasel              NaN            NaN
        Comodo Dragon          NaN            NaN
        IE10                 404.0           0.08
        Chrome               200.0           0.02

        We can fill in the missing values by passing a value to the keyword fill_value.

        >>> df.reindex(new_index, fill_value=0)
                       http_status  response_time
        Safari                 404           0.07
        Iceweasel                0           0.00
        Comodo Dragon            0           0.00
        IE10                   404           0.08
        Chrome                 200           0.02

        >>> df.reindex(new_index, fill_value=-1)  # doctest: +NORMALIZE_WHITESPACE
                       http_status    response_time
        Safari                 404             0.07
        Iceweasel               -1            -1.00
        Comodo Dragon           -1            -1.00
        IE10                   404             0.08
        Chrome                 200             0.02

        We can also reindex the columns.

        >>> df.reindex(columns=['http_status', 'user_agent']) # doctest: +NORMALIZE_WHITESPACE
                   http_status   user_agent
        Firefox            200         None
        Chrome             200         None
        Safari             404         None
        IE10               404         None
        Konqueror          301         None

        Or we can use “axis-style” keyword arguments

        >>> df.reindex(['http_status', 'user_agent'], axis="columns")  # doctest: +NORMALIZE_WHITESPACE
                   http_status   user_agent
        Firefox            200         None
        Chrome             200         None
        Safari             404         None
        IE10               404         None
        Konqueror          301         None

        To further illustrate the filling functionality in reindex, we will create a dataframe
        with a monotonically increasing index (for example, a sequence of dates).

        >>> date_index = pd.date_range('1/1/2010', periods=6, freq='D')
        >>> df2 = pd.DataFrame({"prices": [100, 101, np.nan, 100, 89, 88]},
        ...                    index=date_index)
        >>> df2
                    prices
        2010-01-01   100.0
        2010-01-02   101.0
        2010-01-03     NaN
        2010-01-04   100.0
        2010-01-05    89.0
        2010-01-06    88.0

        Suppose we decide to expand the dataframe to cover a wider date range.

        >>> date_index2 = pd.date_range('12/29/2009', periods=10, freq='D')
        >>> df2.reindex(date_index2)
                    prices
        2009-12-29     NaN
        2009-12-30     NaN
        2009-12-31     NaN
        2010-01-01   100.0
        2010-01-02   101.0
        2010-01-03     NaN
        2010-01-04   100.0
        2010-01-05    89.0
        2010-01-06    88.0
        2010-01-07     NaN

        The index entries that did not have a value in the original data frame (for example,
        ``2009-12-29``) are by default filled with NaN. If desired, we can fill in the missing
        values using one of several options.

        For example, to back-propagate the last valid value to fill the NaN values, pass bfill as
        an argument to the method keyword.

        >>> df2.reindex(date_index2, method='bfill')
                    prices
        2009-12-29   100.0
        2009-12-30   100.0
        2009-12-31   100.0
        2010-01-01   100.0
        2010-01-02   101.0
        2010-01-03     NaN
        2010-01-04   100.0
        2010-01-05    89.0
        2010-01-06    88.0
        2010-01-07     NaN

        Please note that the NaN value present in the original dataframe (at index value 2010-01-03) will
        not be filled by any of the value propagation schemes. This is because filling while reindexing
        does not look at dataframe values, but only compares the original and desired indexes. If you do
        want to fill in the NaN values present in the original dataframe, use the fillna() method.

        An example illustrating Snowpark pandas' behavior when dealing with non-monotonic indices.
        >>> unordered_dataframe = pd.DataFrame([[5]*3, [8]*3, [6]*3], columns=list("ABC"), index=[5, 8, 6])
        >>> unordered_dataframe
           A  B  C
        5  5  5  5
        8  8  8  8
        6  6  6  6
        >>> unordered_dataframe.reindex([6, 8, 7], method="ffill")
           A  B  C
        6  6  6  6
        8  8  8  8
        7  6  6  6

        In the example above, index value ``7`` is forward filled from index value ``6``, since that
        is the previous index value when the data is sorted.
        """

    def rename_axis():
        """
        Set the name of the axis for the index or columns.

        Parameters
        ----------
        mapper : scalar, list-like, optional
            Value to set the axis name attribute.

        index : scalar, list-like, dict-like or function, optional
            A scalar, list-like, dict-like or functions transformations to apply to that axis' values.

            Use either ``mapper`` and ``axis`` to specify the axis to target with ``mapper``, or ``index``.

        axis : {0 or 'index', 1 or 'columns'}, default 0
            The axis to rename. For Series this parameter is unused and defaults to 0.

        copy : bool, default None
            Also copy underlying data. This parameter is ignored in Snowpark pandas.

        inplace : bool, default False
            Modifies the object directly, instead of creating a new Series.

        Returns
        -------
        Series or None
            Series, or None if ``inplace=True``.
        """

    def rename():
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
        >>> s.rename({1: 3, 2: 5})  # mapping, changes labels
        0    1
        3    2
        5    3
        dtype: int64
        """

    def repeat():
        """
        Repeat elements of a Series.
        """

    def reset_index():
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

    def rdivmod():
        """
        Return integer division and modulo of series and `other`, element-wise (binary operator `rdivmod`).

        not yet implemented
        """

    @_create_operator_docstring(
        pandas.core.series.Series.rfloordiv, overwrite_existing=True
    )
    def rfloordiv():
        pass

    @_create_operator_docstring(pandas.core.series.Series.rmod, overwrite_existing=True)
    def rmod():
        pass

    def round():
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

    @_create_operator_docstring(pandas.core.series.Series.rpow, overwrite_existing=True)
    def rpow():
        pass

    @_create_operator_docstring(pandas.core.series.Series.rsub, overwrite_existing=True)
    def rsub():
        pass

    @_create_operator_docstring(
        pandas.core.series.Series.rtruediv, overwrite_existing=True
    )
    def rtruediv():
        pass

    rdiv = rtruediv

    def quantile():
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

            Snowpark pandas currently only supports "linear" and "nearest".

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

    def reorder_levels():
        """
        Rearrange index levels using input order.
        """

    def replace():
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

    def searchsorted():
        """
        Find indices where elements should be inserted to maintain order.
        """

    def sort_values():
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
        """

    def squeeze():
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
        # TODO: SNOW-1372242: Remove instances of to_pandas when lazy index is implemented

        >>> df_0a = df.loc[df.index.to_pandas() < 1, ['a']]
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

    @_create_operator_docstring(pandas.core.series.Series.sub, overwrite_existing=True)
    def sub():
        pass

    subtract = sub

    def swaplevel():
        """
        Swap levels `i` and `j` in a `MultiIndex`.
        """

    def take():
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

    def to_dict():
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

    def to_frame():
        """
        Convert Series to {label -> value} dict or dict-like object.
        """

    def to_list():
        """
        Return a list of the values.
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
        """

    tolist = to_list

    def to_period():
        """
        Cast to PeriodArray/Index at a particular frequency.
        """

    def to_string():
        """
        Render a string representation of the Series.
        """

    def to_timestamp():
        """
        Cast to DatetimeIndex of Timestamps, at beginning of period.
        """

    def transpose():
        """
        Return the transpose, which is by definition `self`.
        """

    T = property(transpose)

    @_create_operator_docstring(
        pandas.core.series.Series.truediv, overwrite_existing=True
    )
    def truediv():
        pass

    div = divide = truediv

    def truncate():
        """
        Truncate a Series before and after some index value.
        """

    def unique():
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

    def update():
        """
        Modify Series in place using values from passed Series.
        """

    def value_counts():
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
            When there is a tie between counts, the order is still deterministic where
            the order in the original data is preserved, but may be different from the
            result from native pandas. Snowpark pandas will always respect the order of
            insertion during ties. Native pandas is not deterministic when `sort=True`
            since the original order/order of insertion is based on the Python hashmap
            which may produce different results on different versions.
            Refer to: https://github.com/pandas-dev/pandas/issues/15833
        ascending : bool, default False
            Whether to sort the frequencies in ascending order or descending order.
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

    def view():
        """
        Create a new view of the Series.
        """

    def where():
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

    def xs():
        """
        Return cross-section from the Series/DataFrame.
        """

    @property
    def attrs():
        """
        Return dictionary of global attributes of this dataset.
        """

    @property
    def array():
        """
        Return the ExtensionArray of the data backing this Series or Index.
        """

    @property
    def axes():
        """
        Return a list of the row axis labels.
        """

    @property
    def cat():
        """
        Accessor object for categorical properties of the Series values.
        """

    @property
    def dt():
        """
        Accessor object for datetimelike properties of the Series values.
        """

    @property
    def dtype():
        """
        Return the dtype object of the underlying data.
        See :func:`DataFrame.dtypes` for exact behavior.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3])
        >>> s.dtype
        dtype('int64')
        """

    dtypes = dtype

    @property
    def empty():
        """
        Indicator whether the Series is empty.

        True if the Series is entirely empty (no items), meaning it is of length 0.
        """

    @property
    def hasnans():
        """
        Return True if there are any NaNs.
        """

    def isna():
        """
        Detect missing values.

        Return a boolean same-sized object indicating if the values are NA. NA values, such as None
        or `numpy.NaN`, gets mapped to True values. Everything else gets mapped to False values.
        Characters such as empty strings `''` or `numpy.inf` are not considered NA values.

        Returns
        -------
        Series
            Mask of bool values for each element in Series that indicates whether an element is an NA value.

        Examples
        --------
        >>> ser = pd.Series([5, 6, np.nan])
        >>> ser
        0    5.0
        1    6.0
        2    NaN
        dtype: float64

        >>> ser.isna()
        0    False
        1    False
        2     True
        dtype: bool
        """

    def isnull():
        """
        `Series.isnull` is an alias for `Series.isna`.

        Detect missing values.

        Return a boolean same-sized object indicating if the values are NA. NA values, such as None
        or `numpy.NaN`, gets mapped to True values. Everything else gets mapped to False values.
        Characters such as empty strings `''` or `numpy.inf` are not considered NA values.

        Returns
        -------
        Series
            Mask of bool values for each element in Series that indicates whether an element is an NA value.

        Examples
        --------
        >>> ser = pd.Series([5, 6, np.nan])
        >>> ser
        0    5.0
        1    6.0
        2    NaN
        dtype: float64

        >>> ser.isna()
        0    False
        1    False
        2     True
        dtype: bool
        """

    @property
    def is_monotonic_increasing():
        """
        Return True if values in the Series are monotonic_increasing.
        """

    @property
    def is_monotonic_decreasing():
        """
        Return True if values in the Series are monotonic_decreasing.
        """

    @property
    def is_unique():
        """
        Return True if values in the Series are unique.
        """

    @property
    def nbytes():
        """
        Return the number of bytes in the underlying data.
        """

    @property
    def ndim(self) -> int:
        """
        Number of dimensions of the underlying data, by definition 1.
        """

    def nunique():
        """
        Return number of unique elements in the series.

        Excludes NA values by default.
        Snowpark pandas API does not distinguish between different NaN types like None,
        pd.NA, and np.nan, and treats them as the same.

        Parameters
        ----------
        dropna : bool, default True
            Don't include NaN in the count.

        Returns
        -------
        int

        Examples
        --------
        >>> import snowflake.snowpark.modin.pandas as pd
        >>> import numpy as np
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

        >>> s = pd.Series([pd.NaT, np.nan, pd.NA, None, 1])
        >>> s.nunique()
        1

        >>> s.nunique(dropna=False)
        2
        """

    @property
    def shape():
        """
        Return a tuple of the shape of the underlying data.

        Examples
        --------
        >>> s = pd.Series([1, 2, 3])
        >>> s.shape
        (3,)
        """

    def shift():
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

    @property
    def str():
        """
        Vectorized string functions for Series and Index.
        """

    def to_csv():
        """
        Write object to a comma-separated values (csv) file. This can write csv file
        either to local filesystem or to snowflake stage. Filepath staring with `@` is
        treated as snowflake stage location.

        Note: Writing to local filesystem supports all parameters but writing to
        snowflake stage does not support float_format, mode, encoding, quoting,
        quotechar, lineterminator, doublequote and decimal parameters. Also when
        writing to snowflake stage chucksize, errors and storage_options parameters
        are ignored.

        Parameters
        ----------
        path_or_buf : str, path object, file-like object, or None, default None
            String, path object (implementing os.PathLike[str]), or file-like
            object implementing a write() function. If None, the result is
            returned as a string. If a non-binary file object is passed, it should
            be opened with `newline=''`, disabling universal newlines. If a binary
            file object is passed, `mode` might need to contain a `'b'`.
        sep : str, default ','
            String of length 1. Field delimiter for the output file.
        na_rep : str, default ''
            Missing data representation.
        float_format : str, Callable, default None
            Format string for floating point numbers. If a Callable is given, it takes
            precedence over other numeric formatting parameters, like decimal.
        header : bool or list of str, default True
            Write out the column names. If a list of strings is given it is
            assumed to be aliases for the column names.
        index : bool, default True
            Write row names (index).
        index_label : str or sequence, or False, default None
            Column label for index column(s) if desired. If None is given, and
            `header` and `index` are True, then the index names are used. A
            sequence should be given if the object uses MultiIndex. If
            False do not print fields for index names. Use index_label=False
            for easier importing in R.
        mode : {{'w', 'x', 'a'}}, default 'w'
            Forwarded to either `open(mode=)` or `fsspec.open(mode=)` to control
            the file opening. Typical values include:

            - 'w', truncate the file first.
            - 'x', exclusive creation, failing if the file already exists.
            - 'a', append to the end of file if it exists.
        encoding : str, optional
            A string representing the encoding to use in the output file,
            defaults to 'utf-8'. `encoding` is not supported if `path_or_buf`
            is a non-binary file object.
        compression : str or dict, default 'infer'
            For on-the-fly compression of the output data. If 'infer' and '%s' is
            path-like, then detect compression from the following extensions: '.gz',
            '.bz2', '.zip', '.xz', '.zst', '.tar', '.tar.gz', '.tar.xz' or '.tar.bz2'
            (otherwise no compression).
            Set to ``None`` for no compression.
            Can also be a dict with key ``'method'`` set
            to one of {``'zip'``, ``'gzip'``, ``'bz2'``, ``'zstd'``, ``'xz'``, ``'tar'``} and
            other key-value pairs are forwarded to
            ``zipfile.ZipFile``, ``gzip.GzipFile``,
            ``bz2.BZ2File``, ``zstandard.ZstdCompressor``, ``lzma.LZMAFile`` or
            ``tarfile.TarFile``, respectively.
            As an example, the following could be passed for faster compression and to create
            a reproducible gzip archive:
            ``compression={'method': 'gzip', 'compresslevel': 1, 'mtime': 1}``.

            Note: Supported compression algorithms are different when writing to
            snowflake stage.
            Please refer to https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#type-csv
            for supported compression algorithms.
        quoting : optional constant from csv module
            Defaults to csv.QUOTE_MINIMAL. If you have set a `float_format`
            then floats are converted to strings and thus csv.QUOTE_NONNUMERIC
            will treat them as non-numeric.
        quotechar : str, default '\"'
            String of length 1. Character used to quote fields.
        lineterminator : str, optional
            The newline character or character sequence to use in the output
            file. Defaults to `os.linesep`, which depends on the OS in which
            this method is called ('\\n' for linux, '\\r\\n' for Windows, i.e.).
        chunksize : int or None
            Rows to write at a time.
        date_format : str, default None
            Format string for datetime objects.
        doublequote : bool, default True
            Control quoting of `quotechar` inside a field.
        escapechar : str, default None
            String of length 1. Character used to escape `sep` and `quotechar`
            when appropriate.
        decimal : str, default '.'
            Character recognized as decimal separator. E.g. use ',' for
            European data.
        errors : str, default 'strict'
            Specifies how encoding and decoding errors are to be handled.
            See the errors argument for :func:`open` for a full list
            of options.
        storage_options : dict, optional
            Extra options that make sense for a particular storage connection, e.g.
            host, port, username, password, etc. For HTTP(S) URLs the key-value pairs
            are forwarded to ``urllib.request.Request`` as header options. For other
            URLs (e.g. starting with "s3://", and "gcs://") the key-value pairs are
            forwarded to ``fsspec.open``. Please see ``fsspec`` and ``urllib`` for more
            details, and for more examples on storage options refer `here
            <https://pandas.pydata.org/docs/user_guide/io.html?
            highlight=storage_options#reading-writing-remote-files>`_.

        Returns
        -------
        None or str
            If path_or_buf is None, returns the resulting csv format as a
            string. Otherwise returns None.

        See Also
        --------
        read_csv : Load a CSV file into a DataFrame.
        to_excel : Write DataFrame to an Excel file.

        Examples
        --------
        Create 'out.csv' containing 'series' without indices

        >>> series = pd.Series(['red', 'green', 'blue'], name='color')
        >>> series.to_csv('out.csv', index=False)  # doctest: +SKIP

        Create 'out.zip' containing 'out.csv'

        >>> series.to_csv(index=False)  # doctest: +SKIP
        >>> compression_opts = dict(method='zip',
        ...                         archive_name='out.csv')  # doctest: +SKIP
        >>> series.to_csv('out.zip', index=False,
        ...           compression=compression_opts)  # doctest: +SKIP

        To write a csv file to a new folder or nested folder you will first
        need to create it using either Pathlib or os:

        >>> from pathlib import Path  # doctest: +SKIP
        >>> filepath = Path('folder/subfolder/out.csv')  # doctest: +SKIP
        >>> filepath.parent.mkdir(parents=True, exist_ok=True)  # doctest: +SKIP
        >>> df.to_csv(filepath)  # doctest: +SKIP

        >>> import os  # doctest: +SKIP
        >>> os.makedirs('folder/subfolder', exist_ok=True)  # doctest: +SKIP
        >>> df.to_csv('folder/subfolder/out.csv')  # doctest: +SKIP
        """
