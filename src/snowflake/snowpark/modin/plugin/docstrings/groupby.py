#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This module contains groupby docstrings that override modin's docstrings."""

from textwrap import dedent

from pandas.util._decorators import doc

_groupby_agg_method_engine_template = """
Compute {fname} of group values.

Parameters
----------
numeric_only : bool, default {no}
    Include only float, int, boolean columns.

min_count : int, default {mc}
    The required number of valid values to perform the operation. If fewer
    than ``min_count`` non-NA values are present the result will be NA.

engine : str, default None {e}
    * ``'cython'`` : Runs rolling apply through C-extensions from cython.
    * ``'numba'`` : Runs rolling apply through JIT compiled code from numba.
        Only available when ``raw`` is set to ``True``.
    * ``None`` : Defaults to ``'cython'`` or globally setting ``compute.use_numba``

    **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

engine_kwargs : dict, default None {ek}
    * For ``'cython'`` engine, there are no accepted ``engine_kwargs``
    * For ``'numba'`` engine, the engine can accept ``nopython``, ``nogil``
        and ``parallel`` dictionary keys. The values must either be ``True`` or
        ``False``. The default ``engine_kwargs`` for the ``'numba'`` engine is
        ``{{'nopython': True, 'nogil': False, 'parallel': False}}`` and will be
        applied to both the ``func`` and the ``apply`` groupby aggregation.

    **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

Returns
-------
:class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
    Computed {fname} of values within each group.

Examples
--------
{example}
"""

_agg_template = """
Aggregate using one or more operations over the specified axis.

Parameters
----------
func : function, str, list, or dict
    Function to use for aggregating the data. If a function, must either
    work when passed a {klass} or when passed to {klass}.apply.

    Accepted combinations are:

    - function
    - string function name
    - list of functions and/or function names, e.g. ``[np.sum, 'mean']``
    - dict of axis labels -> functions, function names or list of such.

*args
    Positional arguments to pass to func.

engine : str, default None
    * ``'cython'`` : Runs the function through C-extensions from cython.
    * ``'numba'`` : Runs the function through JIT compiled code from numba.
    * ``None`` : Defaults to ``'cython'`` or globally setting ``compute.use_numba``

    **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

engine_kwargs : dict, default None
    * For ``'cython'`` engine, there are no accepted ``engine_kwargs``
    * For ``'numba'`` engine, the engine can accept ``nopython``, ``nogil``
      and ``parallel`` dictionary keys. The values must either be ``True`` or
      ``False``. The default ``engine_kwargs`` for the ``'numba'`` engine is
      ``{{'nopython': True, 'nogil': False, 'parallel': False}}`` and will be
      applied to the function

    **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

**kwargs
    keyword arguments to be passed into func.

Returns
-------
{klass}

{examples}"""

_agg_series_examples_doc = dedent(
    """
Examples
--------
>>> s = pd.Series([1, 2, 3, 4], index=pd.Index([1, 2, 1, 2]))

>>> s
1    1
2    2
1    3
2    4
dtype: int64

>>> s.groupby(level=0).agg('min')
1    1
2    2
dtype: int64

>>> s.groupby(level=0).agg(['min', 'max'])
   min  max
1    1    3
2    2    4
"""
)

_agg_examples_dataframe_doc = dedent(
    """
Examples
--------
>>> df = pd.DataFrame(
...     {
...         "A": [1, 1, 2, 2],
...         "B": [1, 2, 3, 4],
...         "C": [0.362838, 0.227877, 1.267767, -0.562860],
...     }
... )

>>> df
   A  B         C
0  1  1  0.362838
1  1  2  0.227877
2  2  3  1.267767
3  2  4 -0.562860

Apply a single aggregation to all columns:

>>> df.groupby('A').agg('min')  # doctest: +NORMALIZE_WHITESPACE
    B         C
A
1  1  0.227877
2  3 -0.562860

Apply multiple aggregations to all columns:

>>> df.groupby('A').agg(['min', 'max']) # doctest: +NORMALIZE_WHITESPACE
    B             C
    min max       min       max
A
1   1   2  0.227877  0.362838
2   3   4 -0.562860  1.267767

Select a single column and apply aggregations:

>>> df.groupby('A').B.agg(['min', 'max'])   # doctest: +NORMALIZE_WHITESPACE
    min  max
A
1    1    2
2    3    4

Apply different aggregations to specific columns:

>>> df.groupby('A').agg({'B': ['min', 'max'], 'C': 'sum'})  # doctest: +NORMALIZE_WHITESPACE
    B             C
    min max       sum
A
1   1   2  0.590715
2   3   4  0.704907
"""
)


class DataFrameGroupBy:
    def __getattr__():
        """
        Alter regular attribute access, looks up the name in the columns.

        Parameters
        ----------
        key : str
            Attribute name.

        Returns
        -------
        The value of the attribute.
        """

    @property
    def ngroups():
        pass

    def skew():
        pass

    def ffill():
        pass

    def sem():
        pass

    def value_counts():
        pass

    def mean():
        """
        Compute mean of groups, excluding missing values.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        engine : str, default None
            * ``'cython'`` : Runs the operation through C-extensions from cython.
            * ``'numba'`` : Runs the operation through JIT compiled code from numba.
            * ``None`` : Defaults to ``'cython'`` or globally setting ``compute.use_numba``

            This parameter is ignored in Snowpark pandas, as the execution is always performed in Snowflake.

        engine_kwargs : dict, default None
            * For ``'cython'`` engine, there are no accepted ``engine_kwargs``
            * For ``'numba'`` engine, the engine can accept ``nopython``, ``nogil``
                and ``parallel`` dictionary keys. The values must either be ``True`` or
                ``False``. The default ``engine_kwargs`` for the ``'numba'`` engine is
                ``{{'nopython': True, 'nogil': False, 'parallel': False}}``

            This parameter is ignored in Snowpark pandas, as the execution is always performed in Snowflake.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`

        Examples
        --------
        >>> df = pd.DataFrame({'A': [1, 1, 2, 1, 2],
        ...                    'B': [np.nan, 2, 3, 4, 5],
        ...                    'C': [1, 2, 1, 1, 2]}, columns=['A', 'B', 'C'])

        Groupby one column and return the mean of the remaining columns in
        each group.

        >>> df.groupby('A').mean()      # doctest: +NORMALIZE_WHITESPACE
             B         C
        A
        1  3.0  1.333333
        2  4.0  1.500000

        Groupby two columns and return the mean of the remaining column.

        >>> df.groupby(['A', 'B']).mean()   # doctest: +NORMALIZE_WHITESPACE
                 C
        A B
        1 2.0  2.0
          4.0  1.0
        2 3.0  1.0
          5.0  2.0

        Groupby one column and return the mean of only one particular column in
        the group.

        >>> df.groupby('A')['B'].mean()
        A
        1    3.0
        2    4.0
        Name: B, dtype: float64
        """

    @property
    def plot():
        pass

    def ohlc():
        pass

    def __bytes__():
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

    @property
    def groups():
        """
        Get a dictionary mapping group key to row labels.

        Returns
        -------
        pandas.io.formats.printing.PrettyDict[Hashable, pandas.Index]
            Dict {group name -> group labels}.

        Examples
        --------
        >>> df = pd.DataFrame({'A': [1, 1, 2, 1, 2],
        ...                    'B': [np.nan, 2, 3, 4, 5],
        ...                    'C': [1, 2, 1, 1, 2]}, columns=['A', 'B', 'C'])

        Groupby one column and get the label of each member of each group.

        >>> df.groupby('A').groups
        {1: [0, 1, 3], 2: [2, 4]}

        Group a dataframe with a custom index by two columns.

        >>> df.set_index('A', inplace=True)
        >>> df.groupby(['B', 'C']).groups
        {(2.0, 2): [1], (3.0, 1): [2], (4.0, 1): [1], (5.0, 2): [2]}

        Notes
        -----
        Beware that the return value is a python dictionary, so evaluating this
        property will trigger eager evaluation of the pandas dataframe and will
        materialize data that could be as large as the size of the grouping
        columns plus the size of the index.
        """

    @doc(
        _groupby_agg_method_engine_template,
        fname="min",
        no=False,
        mc=-1,
        e=None,
        ek=None,
        example=dedent(
            """\
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'b', 'b']
        >>> ser = pd.Series([1, 2, 3, 4], index=lst)
        >>> ser
        a    1
        a    2
        b    3
        b    4
        dtype: int64
        >>> ser.groupby(level=0).min()
        a    1
        b    3
        dtype: int64

        For DataFrameGroupBy:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["tiger", "leopard", "cheetah", "lion"])
        >>> df
                 a  b  c
        tiger    1  8  2
        leopard  1  2  5
        cheetah  2  5  8
        lion     2  6  9
        >>> df.groupby("a").min()  # doctest: +NORMALIZE_WHITESPACE
           b  c
        a
        1  2  2
        2  5  8"""
        ),
    )
    def min():
        pass

    def idxmax():
        """
        Return the index of the first occurrence of maximum over requested axis.

        NA/null values are excluded based on `skipna`.

        Parameters
        ----------
        axis : {{0 or 'index', 1 or 'columns'}}, default None
            The axis to use. 0 or 'index' for row-wise, 1 or 'columns' for column-wise.
            If axis is not provided, grouper's axis is used.

            Snowpark pandas does not support axis=1, since it is deprecated in pandas.

            .. deprecated:: 2.1.0
                For axis=1, operate on the underlying object instead. Otherwise,
                the axis keyword is not necessary.

        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result
            will be NA.

        numeric_only : bool, default False
            Include only `float`, `int` or `boolean` data.

        Returns
        -------
        Series
            Indexes of maxima along the specified axis.

        Raises
        ------
        ValueError
            If the row/column is empty

        See Also
        --------
        Series.idxmax : Return index of the maximum element.

        Notes
        -----
        This method is the DataFrame version of ``ndarray.argmax``.

        Examples
        --------
        >>> small_df_data = [
        ...        ["lion", 78, 50, 50, 50],
        ...        ["tiger", -35, 12, -378, 1246],
        ...        ["giraffe", 54, -9, 67, -256],
        ...        ["hippopotamus", np.nan, -537, -47, -789],
        ...        ["tiger", 89, 2, 256, 246],
        ...        ["tiger", -325, 2, 2, 5],
        ...        ["tiger", 367, -367, 3, -6],
        ...        ["giraffe", 25, 6, 312, 6],
        ...        ["lion", -5, -5, -3, -4],
        ...        ["lion", 15, np.nan, 2, 12],
        ...        ["giraffe", 100, 200, 300, 400],
        ...        ["hippopotamus", -100, -300, -600, -200],
        ...        ["rhino", 26, 2, -45, 14],
        ...        ["rhino", -7, 63, 257, -257],
        ...        ["lion", 1, 2, 3, 4],
        ...        ["giraffe", -5, -6, -7, 8],
        ...        ["lion", 1234, 456, 78, np.nan],
        ... ]

        >>> df = pd.DataFrame(
        ...     data=small_df_data,
        ...     columns=("species", "speed", "age", "weight", "height"),
        ...     index=list("abcdefghijklmnopq"),
        ... )

        Group by axis=0, apply idxmax on axis=0

        >>> df.groupby("species").idxmax(axis=0, skipna=True)  # doctest: +NORMALIZE_WHITESPACE
                     speed age weight height
        species
        giraffe          k   k      h      k
        hippopotamus     l   l      d      l
        lion             q   q      q      a
        rhino            m   n      n      m
        tiger            g   b      e      b

        >>> df.groupby("species").idxmax(axis=0, skipna=False)  # doctest: +NORMALIZE_WHITESPACE
                     speed   age weight height
        species
        giraffe          k     k      h      k
        hippopotamus  None     l      d      l
        lion             q  None      q   None
        rhino            m     n      n      m
        tiger            g     b      e      b

        """

    def idxmin():
        """
        Return the index of the first occurrence of minimum over requested axis.

        NA/null values are excluded based on `skipna`.

        Parameters
        ----------
        axis : {{0 or 'index', 1 or 'columns'}}, default None
            The axis to use. 0 or 'index' for row-wise, 1 or 'columns' for column-wise.
            If axis is not provided, grouper's axis is used.

            Snowpark pandas does not support axis=1, since it is deprecated in pandas.

            .. deprecated:: 2.1.0
                For axis=1, operate on the underlying object instead. Otherwise,
                the axis keyword is not necessary.

        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result
            will be NA.

        numeric_only : bool, default False
            Include only `float`, `int` or `boolean` data.

        Returns
        -------
        Series
            Indexes of minima along the specified axis.

        Raises
        ------
        ValueError
            If the row/column is empty

        See Also
        --------
        Series.idxmin : Return index of the minimum element.

        Notes
        -----
        This method is the DataFrame version of ``ndarray.argmin``.

        Examples
        --------
        >>> small_df_data = [
        ...        ["lion", 78, 50, 50, 50],
        ...        ["tiger", -35, 12, -378, 1246],
        ...        ["giraffe", 54, -9, 67, -256],
        ...        ["hippopotamus", np.nan, -537, -47, -789],
        ...        ["tiger", 89, 2, 256, 246],
        ...        ["tiger", -325, 2, 2, 5],
        ...        ["tiger", 367, -367, 3, -6],
        ...        ["giraffe", 25, 6, 312, 6],
        ...        ["lion", -5, -5, -3, -4],
        ...        ["lion", 15, np.nan, 2, 12],
        ...        ["giraffe", 100, 200, 300, 400],
        ...        ["hippopotamus", -100, -300, -600, -200],
        ...        ["rhino", 26, 2, -45, 14],
        ...        ["rhino", -7, 63, 257, -257],
        ...        ["lion", 1, 2, 3, 4],
        ...        ["giraffe", -5, -6, -7, 8],
        ...        ["lion", 1234, 456, 78, np.nan],
        ... ]

        >>> df = pd.DataFrame(
        ...     data=small_df_data,
        ...     columns=("species", "speed", "age", "weight", "height"),
        ...     index=list("abcdefghijklmnopq"),
        ... )

        Group by axis=0, apply idxmax on axis=0

        >>> df.groupby("species").idxmin(axis=0, skipna=True)  # doctest: +NORMALIZE_WHITESPACE
                     speed age weight height
        species
        giraffe          p   c      p      c
        hippopotamus     l   d      l      d
        lion             i   i      i      i
        rhino            n   m      m      n
        tiger            f   g      b      g

        >>> df.groupby("species").idxmin(axis=0, skipna=False)  # doctest: +NORMALIZE_WHITESPACE
                     speed   age weight height
        species
        giraffe          p     c      p      c
        hippopotamus  None     d      l      d
        lion             i  None      i   None
        rhino            n     m      m      n
        tiger            f     g      b      g

        """

    @property
    def ndim():
        """
        Return 2.

        Returns
        -------
        int
            Returns 2.

        Notes
        -----
        Deprecated and removed in pandas and will be likely removed in Modin.
        """

    def shift():
        """
        Shift each group by `periods` observations.

        If freq is passed, the index will be increased using the periods and the freq.

        Parameters
        ----------
        periods : int | Sequence[int], default 1
            Number of periods to shift. Can be positive or negative. If an iterable of ints,
            the data will be shifted once by each int. This is equivalent to shifting by one
            value at a time and concatenating all resulting frames. The resulting columns
            will have the shift suffixed to their column names. For multiple periods, axis must not be 1.

            Snowpark pandas does not currently support sequences of int for `periods`.

        freq : DateOffset, tseries.offsets, timedelta, or str, optional
            Offset to use from the tseries module or time rule (e.g. ‘EOM’).

            Snowpark pandas does not yet support this parameter.

        axis : axis to shift, default 0
            Shift direction. Snowpark pandas does not yet support axis=1.

        fill_value : optional
            The scalar value to use for newly introduced missing values.

        suffix : str, optional
            If str is specified and periods is an iterable, this is added after the column name
            and before the shift value for each shifted column name.

            Snowpark pandas does not yet support this parameter.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Object shifted within each group.

        Examples
        --------
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'b', 'b']
        >>> ser = pd.Series([1, 2, 3, 4], index=lst)

        >>> ser
        a    1
        a    2
        b    3
        b    4
        dtype: int64


        >>> ser.groupby(level=0).shift(1)
        a    NaN
        a    1.0
        b    NaN
        b    3.0
        dtype: float64

        For DataFrameGroupBy:

        >>> data = [[1, 2, 3], [1, 5, 6], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["tuna", "salmon", "catfish", "goldfish"])

        >>> df
                  a  b  c
        tuna      1  2  3
        salmon    1  5  6
        catfish   2  5  8
        goldfish  2  6  9

        >>> df.groupby("a").shift(1)
                    b    c
        tuna      NaN  NaN
        salmon    2.0  3.0
        catfish   NaN  NaN
        goldfish  5.0  8.0
        """

    def nth():
        pass

    def cumsum():
        """
        Cumulative sum for each group.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`

        See also
        --------
        Series.groupby
            Apply a function groupby to a Series.
        DataFrame.groupby
            Apply a function groupby to each row or column of a DataFrame.

        Examples
        --------
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'b']
        >>> ser = pd.Series([6, 2, 0], index=lst)
        >>> ser
        a    6
        a    2
        b    0
        dtype: int64

        >>> ser.groupby(level=0).cumsum()
        a    6
        a    8
        b    0
        dtype: int64

        For DataFrameGroupBy:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 6, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["fox", "gorilla", "lion"])
        >>> df
                 a  b  c
        fox      1  8  2
        gorilla  1  2  5
        lion     2  6  9

        >>> df.groupby("a").groups
        {1: ['fox', 'gorilla'], 2: ['lion']}

        >>> df.groupby("a").cumsum()
                  b  c
        fox       8  2
        gorilla  10  7
        lion      6  9
        """

    @property
    def indices():
        """
        Get a dictionary mapping group key to row positions.

        Returns
        -------
        Dict[Any, np.array]
            Dict {group name -> group positions}.

        Examples
        --------
        >>> df = pd.DataFrame({'A': [1, 1, 2, 1, 2],
        ...                    'B': [np.nan, 2, 3, 4, 5],
        ...                    'C': [1, 2, 1, 1, 2]})

        Groupby one column and get the positions of each member of each group.

        >>> df.groupby('A').indices
        {1: array([0, 1, 3]), 2: array([2, 4])}

        Group the same dataframe with a different index. The result is the same
        because the row positions for each group are the same.

        >>> df.set_index('B').groupby('A').indices
        {1: array([0, 1, 3]), 2: array([2, 4])}

        Notes
        -----
        Beware that the return value is a python dictionary, so evaluating this
        property will trigger evaluation of the pandas dataframe and will
        materialize data that could be as large as the size of the grouping
        columns.
        """

    def pct_change():
        pass

    def filter():
        pass

    def cummax():
        """
        Cumulative max for each group.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`

        See also
        --------
        Series.groupby
            Apply a function groupby to a Series.
        DataFrame.groupby
            Apply a function groupby to each row or column of a DataFrame.

        Examples
        --------
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'a', 'b', 'b', 'b']
        >>> ser = pd.Series([1, 6, 2, 3, 1, 4], index=lst)
        >>> ser
        a    1
        a    6
        a    2
        b    3
        b    1
        b    4
        dtype: int64
        >>> ser.groupby(level=0).cummax()
        a    1
        a    6
        a    6
        b    3
        b    3
        b    4
        dtype: int64

        For DataFrameGroupBy:

        >>> data = [[1, 8, 2], [1, 1, 0], [2, 6, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["cow", "horse", "bull"])
        >>> df
               a  b  c
        cow    1  8  2
        horse  1  1  0
        bull   2  6  9
        >>> df.groupby("a").groups
        {1: ['cow', 'horse'], 2: ['bull']}
        >>> df.groupby("a").cummax()
               b  c
        cow    8  2
        horse  8  2
        bull   6  9
        """

    def apply():
        """
        Apply function ``func`` group-wise and combine the results together.

        The function passed to ``apply`` must take a dataframe or series as its first
        argument and return a DataFrame, Series or scalar. ``apply`` will
        then take care of combining the results back together into a single
        dataframe or series. ``apply`` is therefore a highly flexible
        grouping method.

        While ``apply`` is a very flexible method, its downside is that
        using it can be quite a bit slower than using more specific methods
        like ``agg`` or ``transform``. pandas offers a wide range of methods that will
        be much faster than using ``apply`` for their specific purposes, so try to
        use them before reaching for ``apply``.

        Parameters
        ----------
        func : callable
            A callable that takes a dataframe or series as its first argument, and
            returns a dataframe, a series or a scalar. In addition the
            callable may take positional and keyword arguments.
        args, kwargs : tuple and dict
            Optional positional and keyword arguments to pass to ``func``.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`

        See Also
        --------
        pipe : Apply function to the full GroupBy object instead of to each
            group.
        aggregate : Apply aggregate function to the GroupBy object.
        transform : Apply function column-by-column to the GroupBy object.
        Series.apply : Apply a function to a Series.
        DataFrame.apply : Apply a function to each row or column of a DataFrame.

        Notes
        -----
        Functions that mutate the passed object can produce unexpected
        behavior or errors and are not supported.

        Returning a Series or scalar in ``func`` is not yet supported in Snowpark pandas.

        Examples
        --------

        >>> df = pd.DataFrame({'A': 'a a b'.split(),
        ...                    'B': [1,2,3],
        ...                    'C': [4,6,5]})
        >>> g1 = df.groupby('A', group_keys=False)
        >>> g2 = df.groupby('A', group_keys=True)

        Notice that ``g1`` have ``g2`` have two groups, ``a`` and ``b``, and only
        differ in their ``group_keys`` argument. Calling `apply` in various ways,
        we can get different grouping results:

        Example 1: below the function passed to `apply` takes a DataFrame as
        its argument and returns a DataFrame. `apply` combines the result for
        each group together into a new DataFrame:

        >>> g1[['B', 'C']].apply(lambda x: x.select_dtypes('number') / x.select_dtypes('number').sum()) # doctest: +NORMALIZE_WHITESPACE
                    B    C
        0.0  0.333333  0.4
        1.0  0.666667  0.6
        2.0  1.000000  1.0

        In the above, the groups are not part of the index. We can have them included
        by using ``g2`` where ``group_keys=True``:

        >>> g2[['B', 'C']].apply(lambda x: x.select_dtypes('number') / x.select_dtypes('number').sum()) # doctest: +NORMALIZE_WHITESPACE
                    B    C
        A
        a 0.0  0.333333  0.4
          1.0  0.666667  0.6
        b 2.0  1.000000  1.0
        """

    @property
    def dtypes():
        pass

    def first():
        pass

    def __getitem__():
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
            Column lookups on GroupBy when selected column overlaps with the by columns.

            we currently do not support select data columns that overlaps with by columns, like
            df.groupby("A")["A", "C"], where column "A" occurs in both the groupby and column selection.
            This is because in regular groupby, one by column cannot be mapped to multiple columns,
            for example with a dataframe have columns=['A', 'B', 'A'], where 'A' corresponds to two columns,
            df.groupby('A') will raise an error. However, with getitem, the new columns selected
            is treated differently and they can be duplicate of the by column. For example: it is valid to
            have df.groupby("A")["A", "A", "C"] even though the result dataframe after colum select have
            multiple column "A".
            In order to handle this correctly, we need to record the columns selected and move the actual column
            selection to query backend. Proper fallback with column selection is also required.
            Since there is no such usage in our current known usage pattern, and Modin does not support this case.
            We raise a NotImplementedError, and deffer the support to later.
            TODO (SNOW-894942): Handle getitem overlap with groupby column
        """

    def cummin():
        """
        Cumulative min for each group.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`

        See also
        --------
        Series.groupby
            Apply a function groupby to a Series.
        DataFrame.groupby
            Apply a function groupby to each row or column of a DataFrame.

        Examples
        --------
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'a', 'b', 'b', 'b']
        >>> ser = pd.Series([1, 6, 2, 3, 0, 4], index=lst)
        >>> ser
        a    1
        a    6
        a    2
        b    3
        b    0
        b    4
        dtype: int64
        >>> ser.groupby(level=0).cummin()
        a    1
        a    1
        a    1
        b    3
        b    0
        b    0
        dtype: int64

        For DataFrameGroupBy:

        >>> data = [[1, 0, 2], [1, 1, 5], [6, 6, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["snake", "rabbit", "turtle"])
        >>> df
                a  b  c
        snake   1  0  2
        rabbit  1  1  5
        turtle  6  6  9
        >>> df.groupby("a").groups
        {1: ['snake', 'rabbit'], 6: ['turtle']}
        >>> df.groupby("a").cummin()
                b  c
        snake   0  2
        rabbit  0  2
        turtle  6  9
        """

    def bfill():
        pass

    def prod():
        pass

    def std():
        """
        Compute standard deviation of groups, excluding missing values.

        For multiple groupings, the result index will be a MultiIndex.

        Parameters
        ----------
        ddof : int, default 1.
            Degrees of freedom.

            Snowpark pandas currently only supports ddof=0 and ddof=1.

        engine : str, default None
            In pandas, engine can be configured as ``'cython'`` or ``'numba'``, and ``None`` defaults to
            ``'cython'`` or globally setting ``compute.use_numba``.

            This parameter is ignored in Snowpark pandas, as the execution is always performed in Snowflake.

        engine_kwargs : dict, default None
            Configuration keywords for the configured execution egine.

            This parameter is ignored in Snowpark pandas, as the execution is always performed in Snowflake.

        numeric_only : bool, default False
            Include only `float`, `int` or `boolean` data columns.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Standard deviation of values within each group.

        Examples
        --------
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'a', 'b', 'b', 'b', 'c']
        >>> ser = pd.Series([7, 2, 8, 4, 3, 3, 1], index=lst)
        >>> ser
        a    7
        a    2
        a    8
        b    4
        b    3
        b    3
        c    1
        dtype: int64
        >>> ser.groupby(level=0).std()
        a    3.21455
        b    0.57735
        c        NaN
        dtype: float64
        >>> ser.groupby(level=0).std(ddof=0)
        a    2.624669
        b    0.471404
        c    0.000000
        dtype: float64

        Note that if the number of elements in a group is less or equal to the ddof, the result for the
        group will be NaN/None. For example, the value for group c is NaN when we call ser.groupby(level=0).std(),
        and the default ddof is 1.

        For DataFrameGroupBy:

        >>> data = {'a': [1, 3, 5, 7, 7, 8, 3], 'b': [1, 4, 8, 4, 4, 2, 1]}
        >>> df = pd.DataFrame(data, index=pd.Index(['dog', 'dog', 'dog',
        ...                   'mouse', 'mouse', 'mouse', 'mouse'], name='c'))
        >>> df      # doctest: +NORMALIZE_WHITESPACE
               a  b
        c
        dog    1  1
        dog    3  4
        dog    5  8
        mouse  7  4
        mouse  7  4
        mouse  8  2
        mouse  3  1
        >>> df.groupby('c').std()       # doctest: +NORMALIZE_WHITESPACE
                      a         b
        c
        dog    2.000000  3.511885
        mouse  2.217356  1.500000
        >>> data = {'a': [1, 3, 5, 7, 7, 8, 3], 'b': ['c', 'e', 'd', 'a', 'a', 'b', 'e']}
        >>> df = pd.DataFrame(data, index=pd.Index(['dog', 'dog', 'dog',
        ...                   'mouse', 'mouse', 'mouse', 'mouse'], name='c'))
        >>> df      # doctest: +NORMALIZE_WHITESPACE
               a  b
        c
        dog    1  c
        dog    3  e
        dog    5  d
        mouse  7  a
        mouse  7  a
        mouse  8  b
        mouse  3  e
        >>> df.groupby('c').std(numeric_only=True)       # doctest: +NORMALIZE_WHITESPACE
                      a
        c
        dog    2.000000
        mouse  2.217356
        """

    @doc(
        _agg_template,
        examples=_agg_examples_dataframe_doc,
        klass="DataFrame",
    )
    def aggregate():
        pass

    agg = aggregate

    def last():
        pass

    def rank():
        """
        Provide the rank of values within each group.

        Parameters
        ----------
        method: {"average", "min", "max", "first", "dense"}
            How to rank the group of records that have the same value (i.e. break ties):
            - average: average rank of the group
            - min: lowest rank in the group
            - max: highest rank in the group
            - first: ranks assigned in order they appear in the array
            - dense: like 'min', but rank always increases by 1 between groups.
        ascending: bool
            Whether the elements should be ranked in ascending order.
        na_option: {"keep", "top", "bottom"}
            How to rank NaN values:
            - keep: assign NaN rank to NaN values
            - top: assign the lowest rank to NaN values
            - bottom: assign the highest rank to NaN values
        pct: bool
            Whether to display the returned rankings in percentile form.
        axis : {{0 or 'index', 1 or 'columns'}}, default None
            The axis to use. 0 or 'index' for row-wise, 1 or 'columns' for column-wise.
            If axis is not provided, grouper's axis is used.

            Snowpark pandas does not currently support axis=1, since it is deprecated in pandas.

            .. deprecated:: 2.1.0
                For axis=1, operate on the underlying object instead. Otherwise,
                the axis keyword is not necessary.


        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame` with ranking of values within each group

        Examples
        --------
        >>> df = pd.DataFrame({"group": ["a", "a", "a", "b", "b", "b", "b"], "value": [2, 4, 2, 3, 5, 1, 2]})
        >>> df
          group  value
        0     a      2
        1     a      4
        2     a      2
        3     b      3
        4     b      5
        5     b      1
        6     b      2
        >>> df = df.groupby("group").rank(method='min')
        >>> df
           value
        0      1
        1      3
        2      1
        3      3
        4      4
        5      1
        6      2
        """

    def corrwith():
        pass

    @doc(
        _groupby_agg_method_engine_template,
        fname="max",
        no=False,
        mc=-1,
        e=None,
        ek=None,
        example=dedent(
            """\
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'b', 'b']
        >>> ser = pd.Series([1, 2, 3, 4], index=lst)
        >>> ser
        a    1
        a    2
        b    3
        b    4
        dtype: int64
        >>> ser.groupby(level=0).max()
        a    2
        b    4
        dtype: int64

        For DataFrameGroupBy:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["tiger", "leopard", "cheetah", "lion"])
        >>> df
                 a  b  c
        tiger    1  8  2
        leopard  1  2  5
        cheetah  2  5  8
        lion     2  6  9
        >>> df.groupby("a").max()  # doctest: +NORMALIZE_WHITESPACE
           b  c
        a
        1  8  5
        2  6  9"""
        ),
    )
    def max():
        pass

    def var():
        """
        Compute variance of groups, excluding missing values.

        For multiple groupings, the result index will be a MultiIndex.

        Parameters
        ----------
        ddof : int, default 1
            Degrees of freedom.
            When ddof is 0/1, the operation is executed with Snowflake. Otherwise, it is not yet supported.

        engine : str, default None
            In pandas, engine can be configured as ``'cython'`` or ``'numba'``, and ``None`` defaults to
            ``'cython'`` or globally setting ``compute.use_numba``.

            This parameter is ignored in Snowpark pandas, as the execution is always performed in Snowflake.

        engine_kwargs : dict, default None
            Configuration keywords for the configured execution egine.

            This parameter is ignored in Snowpark pandas, as the execution is always performed in Snowflake.

        numeric_only : bool, default False
            Include only `float`, `int` or `boolean` data columns.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Variance of values within each group.

        Examples
        --------
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'a', 'b', 'b', 'b', 'c']
        >>> ser = pd.Series([7, 2, 8, 4, 3, 3, 1], index=lst)
        >>> ser
        a    7
        a    2
        a    8
        b    4
        b    3
        b    3
        c    1
        dtype: int64
        >>> ser.groupby(level=0).var()
        a    10.333333
        b     0.333333
        c          NaN
        dtype: float64
        >>> ser.groupby(level=0).var(ddof=0)
        a    6.888889
        b    0.222222
        c    0.000000
        dtype: float64

        Note that if the number of elements in a group is less or equal to the ddof, the result for the
        group will be NaN/None. For example, the value for group c is NaN when we call ser.groupby(level=0).var(),
        and the default ddof is 1.

        For DataFrameGroupBy:

        >>> data = {'a': [1, 3, 5, 7, 7, 8, 3], 'b': [1, 4, 8, 4, 4, 2, 1]}
        >>> df = pd.DataFrame(data, index=pd.Index(['dog', 'dog', 'dog',
        ...                   'mouse', 'mouse', 'mouse', 'mouse'], name='c'))
        >>> df      # doctest: +NORMALIZE_WHITESPACE
               a  b
        c
        dog    1  1
        dog    3  4
        dog    5  8
        mouse  7  4
        mouse  7  4
        mouse  8  2
        mouse  3  1
        >>> df.groupby('c').var()       # doctest: +NORMALIZE_WHITESPACE
                      a          b
        c
        dog    4.000000  12.333333
        mouse  4.916667   2.250000
        >>> data = {'a': [1, 3, 5, 7, 7, 8, 3], 'b': ['c', 'e', 'd', 'a', 'a', 'b', 'e']}
        >>> df = pd.DataFrame(data, index=pd.Index(['dog', 'dog', 'dog',
        ...                   'mouse', 'mouse', 'mouse', 'mouse'], name='c'))
        >>> df      # doctest: +NORMALIZE_WHITESPACE
               a  b
        c
        dog    1  c
        dog    3  e
        dog    5  d
        mouse  7  a
        mouse  7  a
        mouse  8  b
        mouse  3  e
        >>> df.groupby('c').var(numeric_only=True)       # doctest: +NORMALIZE_WHITESPACE
                      a
        c
        dog    4.000000
        mouse  4.916667
        """

    def get_group():
        pass

    def __len__():
        pass

    def all():
        """
        Return True if all values in the group are truthful, else False.

        Parameters
        ----------
        skipna : bool, default True
            Flag to ignore nan values during truth testing.

        Returns
        -------
        Series or DataFrame
            DataFrame or Series of boolean values, where a value is True if all elements
            are True within its respective group, False otherwise.

        Examples
        --------

        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'b']
        >>> ser = pd.Series([1, 2, 0], index=lst)
        >>> ser  # doctest: +NORMALIZE_WHITESPACE
        a    1
        a    2
        b    0
        dtype: int64
        >>> ser.groupby(level=0).all()  # doctest: +NORMALIZE_WHITESPACE
        a     True
        b    False
        dtype: bool

        For DataFrameGroupBy:

        >>> data = [[1, 0, 3], [1, 5, 6], [7, 8, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["ostrich", "penguin", "parrot"])
        >>> df  # doctest: +NORMALIZE_WHITESPACE
                 a  b  c
        ostrich  1  0  3
        penguin  1  5  6
        parrot   7  8  9
        >>> df.groupby(by=["a"]).all()  # doctest: +NORMALIZE_WHITESPACE
               b      c
        a
        1  False   True
        7   True   True
        """

    def any():
        """
        Return True if any value in the group is truthful, else False.

        Parameters
        ----------
        skipna : bool, default True
            Flag to ignore nan values during truth testing.

        Returns
        -------
        Series or DataFrame
            DataFrame or Series of boolean values, where a value is True if any element
            is True within its respective group, False otherwise.

        Examples
        --------
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'b']
        >>> ser = pd.Series([1, 2, 0], index=lst)
        >>> ser  # doctest: +NORMALIZE_WHITESPACE
        a    1
        a    2
        b    0
        dtype: int64
        >>> ser.groupby(level=0).any()  # doctest: +NORMALIZE_WHITESPACE
        a     True
        b    False
        dtype: bool

        For DataFrameGroupBy:

        >>> data = [[1, 0, 3], [1, 0, 6], [7, 1, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["ostrich", "penguin", "parrot"])
        >>> df  # doctest: +NORMALIZE_WHITESPACE
                 a  b  c
        ostrich  1  0  3
        penguin  1  0  6
        parrot   7  1  9
        >>> df.groupby(by=["a"]).any()  # doctest: +NORMALIZE_WHITESPACE
               b      c
        a
        1  False   True
        7   True   True
        """

    def size():
        """
        Compute group sizes.

        Returns
        -------
        DataFrame or Series
            Number of rows in each group as a Series if as_index is True
            or a DataFrame if as_index is False.

        Examples
        --------

        >>> data = [[1, 2, 3], [1, 5, 6], [7, 8, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["owl", "toucan", "eagle"])
        >>> df
                a  b  c
        owl     1  2  3
        toucan  1  5  6
        eagle   7  8  9
        >>> df.groupby("a").size()
        a
        1    2
        7    1
        dtype: int64

        For SeriesGroupBy:

        >>> data = [[1, 2, 3], [1, 5, 6], [7, 8, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["owl", "toucan", "eagle"])
        >>> df
                a  b  c
        owl     1  2  3
        toucan  1  5  6
        eagle   7  8  9
        >>> df.groupby("a")["b"].size()
        a
        1    2
        7    1
        Name: b, dtype: int64
        """
        pass

    @doc(
        _groupby_agg_method_engine_template,
        fname="sum",
        no=False,
        mc=0,
        e=None,
        ek=None,
        example=dedent(
            """\
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'b', 'b']
        >>> ser = pd.Series([1, 2, 3, 4], index=lst)
        >>> ser
        a    1
        a    2
        b    3
        b    4
        dtype: int64
        >>> ser.groupby(level=0).sum()
        a    3
        b    7
        dtype: int64

        For DataFrameGroupBy:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["tiger", "leopard", "cheetah", "lion"])
        >>> df
                 a  b  c
        tiger    1  8  2
        leopard  1  2  5
        cheetah  2  5  8
        lion     2  6  9
        >>> df.groupby("a").sum()  # doctest: +NORMALIZE_WHITESPACE
            b   c
        a
        1  10   7
        2  11  17"""
        ),
    )
    def sum():
        pass

    def describe():
        pass

    def boxplot():
        pass

    def ngroup():
        pass

    def nunique():
        """
        Return DataFrame with counts of unique elements in each position.

        Parameters
        ----------
        dropna : bool, default True
            Whether to exclude NaN in the counts.

        Returns
        -------
        DataFrame
        """

    def resample():
        pass

    def sample():
        pass

    def median():
        """
        Compute median of groups, excluding missing values.

        For multiple groupings, the result index will be a MultiIndex.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Median of values within each group.

        Examples
        --------
        For SeriesGroupBy:

        >>> lst = ['a', 'a', 'a', 'b', 'b', 'b']
        >>> ser = pd.Series([7, 2, 8, 4, 3, 3], index=lst)
        >>> ser
        a    7
        a    2
        a    8
        b    4
        b    3
        b    3
        dtype: int64
        >>> ser.groupby(level=0).median()
        a    7.0
        b    3.0
        dtype: float64

        For DataFrameGroupBy:

        >>> data = {'a': [1, 3, 5, 7, 7, 8, 3], 'b': [1, 4, 8, 4, 4, 2, 1]}
        >>> df = pd.DataFrame(data, index=['dog', 'dog', 'dog',
        ...                   'mouse', 'mouse', 'mouse', 'mouse'])
        >>> df
               a  b
        dog    1  1
        dog    3  4
        dog    5  8
        mouse  7  4
        mouse  7  4
        mouse  8  2
        mouse  3  1
        >>> df.groupby(level=0).median()
                 a    b
        dog    3.0  4.0
        mouse  7.0  3.0
        """

    def head():
        """
        Return first n rows of each group.

        Similar to ``.apply(lambda x: x.head(n))``, but it returns a subset of rows
        from the original DataFrame with original index and order preserved
        (``as_index`` flag is ignored).

        Parameters
        ----------
        n : int
            If positive: number of entries to include from the start of each group.
            If negative: number of entries to exclude from the end of each group.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Subset of the original Series or DataFrame as determined by n.

        See also
        --------
        Series.groupby
            Apply a function groupby to a Series.

        DataFrame.groupby
            Apply a function groupby to each row or column of a DataFrame.

        Examples
        --------
        >>> df = pd.DataFrame([[1, 2], [1, 4], [5, 6]],
        ...                   columns=['A', 'B'])
        >>> df.groupby('A').head(1)
           A  B
        0  1  2
        2  5  6
        >>> df.groupby('A').head(-1)
           A  B
        0  1  2
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": ["Z", None, "X", "Z", "Y", "X", "X", None, "X", "Y"],
        ...         "col2": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        ...         "col3": [40, 50, 60, 10, 20, 30, 40, 80, 90, 10],
        ...         "col4": [-1, -2, -3, -4, -5, -6, -7, -8, -9, -10],
        ...     },
        ...     index=list("abcdefghij"),
        ... )
        >>> df
           col1  col2  col3  col4
        a     Z     1    40    -1
        b  None     2    50    -2
        c     X     3    60    -3
        d     Z     4    10    -4
        e     Y     5    20    -5
        f     X     6    30    -6
        g     X     7    40    -7
        h  None     8    80    -8
        i     X     9    90    -9
        j     Y    10    10   -10
        >>> df.groupby("col1", dropna=False).head(2)
           col1  col2  col3  col4
        a     Z     1    40    -1
        b  None     2    50    -2
        c     X     3    60    -3
        d     Z     4    10    -4
        e     Y     5    20    -5
        f     X     6    30    -6
        h  None     8    80    -8
        j     Y    10    10   -10
        >>> df.groupby("col1", dropna=False).head(-2)
          col1  col2  col3  col4
        c    X     3    60    -3
        f    X     6    30    -6
        """

    def cumprod():
        pass

    def __iter__():
        """
        GroupBy iterator.

        Returns
        -------
        Generator
            A generator yielding a sequence of (name, subsetted object) for each group.
        """

    def cov():
        pass

    def transform():
        """
        Call function producing a same-indexed DataFrame on each group.

        Returns a DataFrame having the same indexes as the original object
        filled with the transformed values.

        Parameters
        ----------
        func : function, str
            Function to apply to each group. See the Notes section below for requirements.

            Accepted inputs are:

            - String (needs to be the name of groupby method you want to use)
            - Python function

        *args : Any
            Positional arguments to pass to func.
        engine : str, default None
            * ``'cython'`` : Runs the function through C-extensions from cython.
            * ``'numba'`` : Runs the function through JIT compiled code from numba.
            * ``None`` : Defaults to ``'cython'`` or the global setting ``compute.use_numba``

            This parameter is ignored in Snowpark pandas, as the execution is always performed in Snowflake.

        engine_kwargs : dict, default None
            * For ``'cython'`` engine, there are no accepted ``engine_kwargs``
            * For ``'numba'`` engine, the engine can accept ``nopython``, ``nogil``
              and ``parallel`` dictionary keys. The values must either be ``True`` or
              ``False``. The default ``engine_kwargs`` for the ``'numba'`` engine is
              ``{'nopython': True, 'nogil': False, 'parallel': False}`` and will be
              applied to the function

            This parameter is ignored in Snowpark pandas, as the execution is always performed in Snowflake.

        **kwargs : Any
            Keyword arguments to be passed into func.

        Notes
        -----
        Functions that mutate the passed object can produce unexpected
        behavior or errors and are not supported.

        Returning a Series or scalar in ``func`` is not yet supported in Snowpark pandas.

        Examples
        --------
        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": ["Z", None, "X", "Z", "Y", "X", "X", None, "X", "Y"],
        ...         "col2": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        ...         "col3": [40, 50, 60, 10, 20, 30, 40, 80, 90, 10],
        ...         "col4": [-1, -2, -3, -4, -5, -6, -7, -8, -9, -10],
        ...     },
        ...     index=list("abcdefghij")
        ... )
        >>> df
           col1  col2  col3  col4
        a     Z     1    40    -1
        b  None     2    50    -2
        c     X     3    60    -3
        d     Z     4    10    -4
        e     Y     5    20    -5
        f     X     6    30    -6
        g     X     7    40    -7
        h  None     8    80    -8
        i     X     9    90    -9
        j     Y    10    10   -10

        >>> df.groupby("col1", dropna=True).transform(lambda df, n: df.head(n), n=2)
           col2  col3  col4
        a   1.0  40.0  -1.0
        b   NaN   NaN   NaN
        c   3.0  60.0  -3.0
        d   4.0  10.0  -4.0
        e   5.0  20.0  -5.0
        f   6.0  30.0  -6.0
        g   NaN   NaN   NaN
        h   NaN   NaN   NaN
        i   NaN   NaN   NaN
        j  10.0  10.0 -10.0

        >>> df.groupby("col1", dropna=False).transform("mean")
           col2  col3  col4
        a  2.50  25.0 -2.50
        b  5.00  65.0 -5.00
        c  6.25  55.0 -6.25
        d  2.50  25.0 -2.50
        e  7.50  15.0 -7.50
        f  6.25  55.0 -6.25
        g  6.25  55.0 -6.25
        h  5.00  65.0 -5.00
        i  6.25  55.0 -6.25
        j  7.50  15.0 -7.50
        """

    def corr():
        pass

    def fillna():
        pass

    def count():
        """
        Compute count of group, excluding missing values.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Count of values within each group.

        Examples
        --------
        For SeriesGroupBy:

            >>> lst = ['a', 'a', 'b']
            >>> ser = pd.Series([1, 2, np.nan], index=lst)
            >>> ser
            a    1.0
            a    2.0
            b    NaN
            dtype: float64
            >>> ser.groupby(level=0).count()
            a    2
            b    0
            dtype: int64

        For DataFrameGroupBy:

            >>> data = [[1, np.nan, 3], [1, np.nan, 6], [7, 8, 9]]
            >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
            ...                   index=["cow", "horse", "bull"])
            >>> df
                   a    b  c
            cow    1  NaN  3
            horse  1  NaN  6
            bull   7  8.0  9
            >>> df.groupby("a").count()     # doctest: +NORMALIZE_WHITESPACE
               b  c
            a
            1  0  2
            7  1  1
        """

    def cumcount():
        """
        Number each item in each group from 0 to the length of that group - 1.

        Essentially this is equivalent to

        .. code-block:: python

            self.apply(lambda x: pd.Series(np.arange(len(x)), x.index))

        Parameters
        ----------
        ascending : bool, default True
            If False, number in reverse, from length of group - 1 to 0.

        Returns
        -------
        Series
            Sequence number of each element within each group.

        See also
        --------
        ngroup
            Number the groups themselves.

        Examples
        --------
        >>> df = pd.DataFrame([['a'], ['a'], ['a'], ['b'], ['b'], ['a']],
        ...                   columns=['A'])
        >>> df
           A
        0  a
        1  a
        2  a
        3  b
        4  b
        5  a

        >>> df.groupby('A').cumcount()
        0    0
        1    1
        2    2
        3    0
        4    1
        5    3
        dtype: int64

        >>> df.groupby('A').cumcount(ascending=False)
        0    3
        1    2
        2    1
        3    1
        4    0
        5    0
        dtype: int64
        """

    def tail():
        """
        Return last n rows of each group.

        Similar to ``.apply(lambda x: x.tail(n))``, but it returns a subset of rows
        from the original DataFrame with original index and order preserved
        (``as_index`` flag is ignored).

        Parameters
        ----------
        n : int
            If positive: number of entries to include from the end of each group.
            If negative: number of entries to exclude from the start of each group.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Subset of the original Series or DataFrame as determined by n.

        See also
        --------
        Series.groupby
            Apply a function groupby to a Series.

        DataFrame.groupby
            Apply a function groupby to each row or column of a DataFrame.

        Examples
        --------

        >>> df = pd.DataFrame([['a', 1], ['a', 2], ['b', 1], ['b', 2]],
        ...                   columns=['A', 'B'])
        >>> df.groupby('A').tail(1)
           A  B
        1  a  2
        3  b  2
        >>> df.groupby('A').tail(-1)
           A  B
        1  a  2
        3  b  2

        >>> df = pd.DataFrame(
        ...     {
        ...         "col1": ["Z", None, "X", "Z", "Y", "X", "X", None, "X", "Y"],
        ...         "col2": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        ...         "col3": [40, 50, 60, 10, 20, 30, 40, 80, 90, 10],
        ...         "col4": [-1, -2, -3, -4, -5, -6, -7, -8, -9, -10],
        ...     },
        ...     index=list("abcdefghij"),
        ... )
        >>> df
           col1  col2  col3  col4
        a     Z     1    40    -1
        b  None     2    50    -2
        c     X     3    60    -3
        d     Z     4    10    -4
        e     Y     5    20    -5
        f     X     6    30    -6
        g     X     7    40    -7
        h  None     8    80    -8
        i     X     9    90    -9
        j     Y    10    10   -10
        >>> df.groupby("col1", dropna=False).tail(2)
           col1  col2  col3  col4
        a     Z     1    40    -1
        b  None     2    50    -2
        d     Z     4    10    -4
        e     Y     5    20    -5
        g     X     7    40    -7
        h  None     8    80    -8
        i     X     9    90    -9
        j     Y    10    10   -10
        >>> df.groupby("col1", dropna=False).tail(-2)
          col1  col2  col3  col4
        g    X     7    40    -7
        i    X     9    90    -9
        """

    def expanding():
        pass

    def rolling():
        pass

    def hist():
        pass

    def quantile():
        """
        Return group values at the given quantile, like ``numpy.percentile``.

        Parameters
        ----------
        q : float or array-like, default 0.5 (50% quantile)
            Value(s) between 0 and 1 providing the quantile(s) to compute.

        interpolation : {'linear', 'lower', 'higher', 'midpoint', 'nearest'}
            Method to use when the desired quantile falls between two points.

            Snowpark pandas currently only supports "linear" and "nearest".

        numeric_only : bool, default False
            Include only float, int or boolean data.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Return type determined by caller of GroupBy object.
        """

    def diff():
        pass

    def take():
        pass

    def pipe():
        pass


class SeriesGroupBy:
    def get_group(self):
        pass

    @property
    def ndim(self):
        """
        Return 1.

        Returns
        -------
        int
            Returns 1.

        Notes
        -----
        Deprecated and removed in pandas and will be likely removed in Modin.
        """

    @property
    def is_monotonic_decreasing():
        pass

    @property
    def is_monotonic_increasing():
        pass

    @doc(_agg_template, examples=_agg_series_examples_doc, klass="Series")
    def aggregate():
        pass

    agg = aggregate

    def nlargest():
        pass

    def nsmallest():
        pass

    def nunique():
        """
        Return number unique elements in the group.

        Parameters
        ----------
        dropna : bool, default True
            Whether to exclude NaN in the counts.

        Returns
        -------
        Series
        """

    def size():
        """
        Compute group sizes.

        Returns
        -------
        DataFrame or Series
            Number of rows in each group as a Series if as_index is True
            or a DataFrame if as_index is False.

        Examples
        --------

        >>> data = [[1, 2, 3], [1, 5, 6], [7, 8, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["owl", "toucan", "eagle"])
        >>> df
                a  b  c
        owl     1  2  3
        toucan  1  5  6
        eagle   7  8  9
        >>> df.groupby("a").size()
        a
        1    2
        7    1
        dtype: int64

        For SeriesGroupBy:

        >>> data = [[1, 2, 3], [1, 5, 6], [7, 8, 9]]
        >>> df = pd.DataFrame(data, columns=["a", "b", "c"],
        ...                   index=["owl", "toucan", "eagle"])
        >>> df
                a  b  c
        owl     1  2  3
        toucan  1  5  6
        eagle   7  8  9
        >>> df.groupby("a")["b"].size()
        a
        1    2
        7    1
        Name: b, dtype: int64
        """
        pass

    def unique(self):
        pass

    def apply():
        pass
