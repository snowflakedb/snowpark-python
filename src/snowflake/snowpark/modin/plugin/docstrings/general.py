#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""This module contains general top-level pandas docstrings that override modin's docstrings."""


def melt():
    """
    Unpivot a DataFrame from wide to long format, optionally leaving identifiers set.

    Parameters
    ----------
    id_vars : list of identifiers to retain in the result
    value_vars : list of columns to unpivot on
        defaults to all columns, excluding the id_vars columns
    var_name : variable name, defaults to "variable"
    value_name : value name, defaults to "value"
    col_level : int, not implemented
    ignore_index : bool, not implemented

    Returns
    -------
    :class:`~modin.pandas.DataFrame`
        unpivoted on the value columns

    Examples
    --------
    >>> df = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
    ...           'B': {0: 1, 1: 3, 2: 5},
    ...           'C': {0: 2, 1: 4, 2: 6}})

    >>> pd.melt(df)
      variable value
    0        A     a
    1        A     b
    2        A     c
    3        B     1
    4        B     3
    5        B     5
    6        C     2
    7        C     4
    8        C     6

    >>> df = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
    ...           'B': {0: 1, 1: 3, 2: 5},
    ...           'C': {0: 2, 1: 4, 2: 6}})
    >>> pd.melt(df, id_vars=['A'], value_vars=['B'], var_name='myVarname', value_name='myValname')
       A myVarname  myValname
    0  a         B          1
    1  b         B          3
    2  c         B          5
    """


def pivot():
    """
    Return reshaped DataFrame organized by given index / column values.

    Reshape data (produce a “pivot” table) based on column values. Uses unique values from
    specified index / columns to form axes of the resulting DataFrame. This function does not
    support data aggregation, multiple values will result in a MultiIndex in the columns.

    Parameters
    ----------
    data : :class:`~modin.pandas.DataFrame`
    columns : str or object or a list of str
        Column to use to make new frame’s columns.
    index : str or object or a list of str, optional
        Column to use to make new frame’s index. If not given, uses existing index.
    values : str, object or a list of the previous, optional
        Column(s) to use for populating new frame’s values. If not specified, all remaining columns
        will be used and the result will have hierarchically indexed columns.

    Returns
    -------
    :class:`~modin.pandas.DataFrame`

    Notes
    -----
    Calls pivot_table with columns, values, index and aggregation "min".

    See Also
    --------
    DataFrame.pivot_table : Generalization of pivot that can handle
        duplicate values for one index/column pair.
    DataFrame.unstack: Pivot based on the index values instead
        of a column.
    wide_to_long : Wide panel to long format. Less flexible but more
        user-friendly than melt.

    Examples
    --------
    >>> df = pd.DataFrame({'foo': ['one', 'one', 'one', 'two', 'two',
    ...                   'two'],
    ...           'bar': ['A', 'B', 'C', 'A', 'B', 'C'],
    ...           'baz': [1, 2, 3, 4, 5, 6],
    ...           'zoo': ['x', 'y', 'z', 'q', 'w', 't']})
    >>> df
       foo bar  baz zoo
    0  one   A    1   x
    1  one   B    2   y
    2  one   C    3   z
    3  two   A    4   q
    4  two   B    5   w
    5  two   C    6   t
    >>> pd.pivot(data=df, index='foo', columns='bar', values='baz')  # doctest: +NORMALIZE_WHITESPACE
    bar  A  B  C
    foo
    one  1  2  3
    two  4  5  6
    >>> pd.pivot(data=df, index='foo', columns='bar')['baz']  # doctest: +NORMALIZE_WHITESPACE
    bar  A  B  C
    foo
    one  1  2  3
    two  4  5  6
    >>> pd.pivot(data=df, index='foo', columns='bar', values=['baz', 'zoo'])  # doctest: +NORMALIZE_WHITESPACE
        baz       zoo
    bar   A  B  C   A  B  C
    foo
    one   1  2  3   x  y  z
    two   4  5  6   q  w  t
    >>> df = pd.DataFrame({
    ...     "lev1": [1, 1, 1, 2, 2, 2],
    ...     "lev2": [1, 1, 2, 1, 1, 2],
    ...     "lev3": [1, 2, 1, 2, 1, 2],
    ...     "lev4": [1, 2, 3, 4, 5, 6],
    ...     "values": [0, 1, 2, 3, 4, 5]})
    >>> df
       lev1  lev2  lev3  lev4  values
    0     1     1     1     1       0
    1     1     1     2     2       1
    2     1     2     1     3       2
    3     2     1     2     4       3
    4     2     1     1     5       4
    5     2     2     2     6       5
    >>> pd.pivot(data=df, index="lev1", columns=["lev2", "lev3"], values="values")  # doctest: +NORMALIZE_WHITESPACE
    lev2  1       2
    lev3  1  2    1    2
    lev1
    1     0  1  2.0  NaN
    2     4  3  NaN  5.0
    >>> pd.pivot(data=df, index=["lev1", "lev2"], columns=["lev3"], values="values")  # doctest: +NORMALIZE_WHITESPACE
    lev3         1    2
    lev1 lev2
    1    1     0.0  1.0
         2     2.0  NaN
    2    1     4.0  3.0
         2     NaN  5.0
    """


def pivot_table():
    """
    Create a spreadsheet-style pivot table as a ``DataFrame``.

    The levels in the pivot table will be stored in MultiIndex objects
    (hierarchical indexes) on the index and columns of the result DataFrame.

    Parameters
    ----------
    values : list-like or scalar, optional
        Column or columns to aggregate.
    index : column, Grouper, array, or list of the previous
        Keys to group by on the pivot table index. If a list is passed,
        it can contain any of the other types (except list). If an array is
        passed, it must be the same length as the data and will be used in
        the same manner as column values.
    columns : column, Grouper, array, or list of the previous
        Keys to group by on the pivot table column. If a list is passed,
        it can contain any of the other types (except list). If an array is
        passed, it must be the same length as the data and will be used in
        the same manner as column values.
    aggfunc : function, list of functions, dict in string, default "mean".
        If a list of functions is passed, the resulting pivot table will have
        hierarchical columns whose top level are the function names
        (inferred from the function objects themselves).
        If a dict is passed, the key is column to aggregate and the value is
        function or list of functions. If ``margin=True``, aggfunc will be
        used to calculate the partial aggregates.
    fill_value : scalar, default None
        Value to replace missing values with (in the resulting pivot table,
        after aggregation).
    margins : bool, default False
        If ``margins=True``, special ``All`` columns and rows
        will be added with partial group aggregates across the categories
        on the rows and columns.
    dropna : bool, default True
        Do not include columns whose entries are all NaN. If True,
        rows with a NaN value in any column will be omitted before
        computing margins.
    margins_name : str, default 'All'
        Name of the row / column that will contain the totals
        when margins is True.
    observed : bool, default False
        This only applies if any of the groupers are Categoricals.
        Categoricals are not yet implemented in Snowpark pandas.
        If True: only show observed values for categorical groupers.
        If False: show all values for categorical groupers.

    sort : bool, default True
        Specifies if the result should be sorted.

    Returns
    -------
    Snowpark pandas :class:`~modin.pandas.DataFrame`
        An Excel style pivot table.

    Notes
    -----
    - Raise NotImplementedError if

        * observed or sort is given;
        * or index, columns, or values is not str, a list of str, or None;
        * or DataFrame contains MultiIndex;
        * or any aggfunc is not "count", "mean", "min", "max", or "sum"
        * index is None, and aggfunc is a dictionary containing lists.

    - Computing margins with no index has limited support:
        * when aggfunc is "count" or "mean" the result has discrepancies with pandas -
          Snowpark pandas computes the aggfunc over the data grouped by the first pivot
          column, while pandas computes the aggfunc over the result of the aggfunc from
          the initial pivot.
        * aggfunc as a dictionary is not supported.

    See Also
    --------
    DataFrame.pivot : Pivot without aggregation that can handle
        non-numeric data.
    DataFrame.melt: Unpivot a DataFrame from wide to long format,
        optionally leaving identifiers set.
    wide_to_long : Wide panel to long format. Less flexible but more
        user-friendly than melt.

    Examples
    --------
    >>> df = pd.DataFrame({"A": ["foo", "foo", "foo", "foo", "foo",
    ...                          "bar", "bar", "bar", "bar"],
    ...                    "B": ["one", "one", "one", "two", "two",
    ...                          "one", "one", "two", "two"],
    ...                    "C": ["small", "large", "large", "small",
    ...                          "small", "large", "small", "small",
    ...                          "large"],
    ...                    "D": [1, 2, 2, 3, 3, 4, 5, 6, 7],
    ...                    "E": [2, 4, 5, 5, 6, 6, 8, 9, 9]})
    >>> df
         A    B      C  D  E
    0  foo  one  small  1  2
    1  foo  one  large  2  4
    2  foo  one  large  2  5
    3  foo  two  small  3  5
    4  foo  two  small  3  6
    5  bar  one  large  4  6
    6  bar  one  small  5  8
    7  bar  two  small  6  9
    8  bar  two  large  7  9

    This first example aggregates values by taking the sum.

    >>> table = pd.pivot_table(df, values='D', index=['A', 'B'],
    ...                        columns=['C'], aggfunc="sum")
    >>> table  # doctest: +NORMALIZE_WHITESPACE
    C        large  small
    A   B
    bar one    4.0      5
        two    7.0      6
    foo one    4.0      1
        two    NaN      6

    We can also fill missing values using the `fill_value` parameter.

    >>> table = pd.pivot_table(df, values='D', index=['A', 'B'],
    ...                        columns=['C'], aggfunc="sum", fill_value=0)
    >>> table  # doctest: +NORMALIZE_WHITESPACE
    C        large  small
    A   B
    bar one    4.0      5
        two    7.0      6
    foo one    4.0      1
        two    NaN      6

    >>> table = pd.pivot_table(df, values=['D', 'E'], index=['A', 'C'],
    ...                        aggfunc={'D': "mean", 'E': "mean"})
    >>> table  # doctest: +NORMALIZE_WHITESPACE
                      D         E
                      D         E
    A   C
    bar large  5.500000  7.500000
        small  5.500000  8.500000
    foo large  2.000000  4.500000
        small  2.333333  4.333333

    >>> table = pd.pivot_table(df, values=['D', 'E'], index=['A', 'C'],
    ...                        aggfunc={'D': "mean",
    ...                                 'E': ["min", "max", "mean"]})
    >>> table  # doctest: +NORMALIZE_WHITESPACE
                      D   E
                   mean max      mean min
                      D   E         E   E
    A   C
    bar large  5.500000   9  7.500000   6
        small  5.500000   9  8.500000   8
    foo large  2.000000   5  4.500000   4
        small  2.333333   6  4.333333   2
    """


def crosstab():
    """
    Compute a simple cross tabulation of two (or more) factors.

    By default, computes a frequency table of the factors unless an array
    of values and an aggregation function are passed.

    Parameters
    ----------
    index : array-like, Series, or list of arrays/Series
        Values to group by in the rows.
    columns : array-like, Series, or list of arrays/Series
        Values to group by in the columns.
    values : array-like, optional
        Array of values to aggregate according to the factors.
        Requires aggfunc be specified.
    rownames : sequence, default None
        If passed, must match number of row arrays passed.
    colnames : sequence, default None
        If passed, must match number of column arrays passed.
    aggfunc : function, optional
        If specified, requires values be specified as well.
    margins : bool, default False
        Add row/column margins (subtotals).
    margins_name : str, default 'All'
        Name of the row/column that will contain the totals when margins is True.
    dropna : bool, default True
        Do not include columns whose entries are all NaN.

    normalize : bool, {'all', 'index', 'columns'}, or {0,1}, default False
        Normalize by dividing all values by the sum of values.

        * If passed 'all' or True, will normalize over all values.
        * If passed 'index' will normalize over each row.
        * If passed 'columns' will normalize over each column.
        * If margins is True, will also normalize margin values.

    Returns
    -------
    Snowpark pandas :class:`~modin.pandas.DataFrame`
        Cross tabulation of the data.

    Notes
    -----

    Raises NotImplementedError if aggfunc is not one of "count", "mean", "min", "max", or "sum", or
    margins is True, normalize is True or all, and values is passed.

    Examples
    --------
    >>> a = np.array(["foo", "foo", "foo", "foo", "bar", "bar",
    ...               "bar", "bar", "foo", "foo", "foo"], dtype=object)
    >>> b = np.array(["one", "one", "one", "two", "one", "one",
    ...               "one", "two", "two", "two", "one"], dtype=object)
    >>> c = np.array(["dull", "dull", "shiny", "dull", "dull", "shiny",
    ...               "shiny", "dull", "shiny", "shiny", "shiny"],
    ...              dtype=object)
    >>> pd.crosstab(a, [b, c], rownames=['a'], colnames=['b', 'c']) # doctest: +NORMALIZE_WHITESPACE
    b    one        two
    c   dull shiny dull shiny
    a
    bar    1     2    1     0
    foo    2     2    1     2
    """


def cut():
    """
    Bin values into discrete intervals.

    Use `cut` when you need to segment and sort data values into bins. This
    function is also useful for going from a continuous variable to a
    categorical variable. For example, `cut` could convert ages to groups of
    age ranges. Supports binning into an equal number of bins, or a
    pre-specified array of bins.

    Parameters
    ----------
    x : array-like
        The input array to be binned. Must be 1-dimensional.
    bins : int, sequence of scalars
        The criteria to bin by.

        * int : Defines the number of equal-width bins in the range of `x`. The
          range of `x` is extended by .1% on each side to include the minimum
          and maximum values of `x`.
        * sequence of scalars : Defines the bin edges allowing for non-uniform
          width. No extension of the range of `x` is done.

    right : bool, default True
        Indicates whether `bins` includes the rightmost edge or not. If
        ``right == True`` (the default), then the `bins` ``[1, 2, 3, 4]``
        indicate (1,2], (2,3], (3,4]. This argument is ignored when
        `bins` is an IntervalIndex.
    labels : array or False, default None
        Specifies the labels for the returned bins. Must be the same length as
        the resulting bins. If False, returns only integer indicators of the
        bins. This affects the type of the output container (see below).
        This argument is ignored when `bins` is an IntervalIndex. If True,
        raises an error. When `ordered=False`, labels must be provided.

        Snowpark pandas API does not support labels=None.
        Labels must be of a Snowpark pandas API supported dtype.

    retbins : bool, default False
        Snowpark pandas API does not support this parameter yet.
    precision : int, default 3
        The precision at which to store and display the bins labels.
    include_lowest : bool, default False
        Whether the first interval should be left-inclusive or not.
    duplicates : {default 'raise', 'drop'}, optional
        If bin edges are not unique, raise ValueError or drop non-uniques.
    ordered : bool, default True
        Whether the labels are ordered or not. Applies to returned types
        Categorical and Series (with Categorical dtype). If True,
        the resulting categorical will be ordered. If False, the resulting
        categorical will be unordered (labels must be provided).

    Returns
    -------
    out : Categorical, Series, or ndarray
        An array-like object representing the respective bin for each value
        of `x`. The type depends on the value of `labels`.

        * None (default) : returns a Series for Series `x` or a
          Categorical for all other inputs. The values stored within
          are Interval dtype.

        * sequence of scalars : returns a Series for Series `x` or a
          Categorical for all other inputs. The values stored within
          are whatever the type in the sequence is.

        * False : returns an ndarray of integers.

    bins : numpy.ndarray
        The computed or specified bins. Only returned when `retbins=True`.
        For scalar or sequence `bins`, this is an ndarray with the computed
        bins. If set `duplicates=drop`, `bins` will drop non-unique bin.

    Notes
    -----
    Any NA values will be NA in the result. Out of bounds values will be NA in
    the resulting Series or Categorical object.

    Snowpark pandas API does not natively support Categorical and categorical types. When calling `cut` with a
    Snowpark pandas Series and using `labels=False`, a Snowpark pandas Series object is returned. However,
    for `labels != False` an error is raised.

    Examples
    --------
    Discretize into three equal-sized bins.

    >>> pd.cut(np.array([1, 7, 5, 4, 6, 3]), 3, labels=False)
    ... # doctest: +ELLIPSIS
    array([0, 2, 1, 1, 2, 0])

    ``labels=False`` implies you just want the bins back.

    >>> pd.cut([0, 1, 1, 2], bins=4, labels=False)
    array([0, 1, 1, 3])

    Passing a Series as an input returns a Series with labels=False:

    >>> s = pd.Series(np.array([2, 4, 6, 8, 10]),
    ...               index=['a', 'b', 'c', 'd', 'e'])
    >>> pd.cut(s, 3, labels=False)
    ... # doctest: +ELLIPSIS
    a    0
    b    0
    c    1
    d    2
    e    2
    dtype: int64
    """


def qcut():
    """
    Quantile-based discretization function.

    Discretize variable into equal-sized buckets based on rank or based
    on sample quantiles.

    Parameters
    ----------
    x : 1-D ndarray or Series
        The data across which to compute buckets. If a Snowpark pandas Series is passed, the computation
        is distributed. Otherwise, if a numpy array or list is provided, the computation is performed
        client-side instead.

    q : int or list-like of float
        Number of quantiles. 10 for deciles, 4 for quartiles, etc. Alternately array of quantiles,
        e.g. [0, .25, .5, .75, 1.] for quartiles.

    labels : array or False, default None
        Used as labels for the resulting bin. Must be of the same length as the resulting bins. If False,
        return only integer indicators of the bins. If True, raise an error.

        ``labels=False`` will run binning computation in Snowflake; other values are not yet supported
        in Snowpark pandas.

    retbins : bool, default False
        Whether to return the (bins, labels) or not. Can be useful if bins is given as a scalar.
        ``retbins=True`` is not yet supported in Snowpark pandas.

    precision : int, optional
        The precision at which to store and display the bins labels.

    duplicates : {default 'raise', 'drop'}, optional
        If bin edges are not unique, raise ValueError or drop non-uniques.

    Returns
    -------
    Series
        Since Snowpark pandas does not yet support the ``pd.Categorical`` type, unlike native pandas, the
        return value is always a Series.
    """


def merge():
    """
    Merge DataFrame or named Series objects with a database-style join.

    A named Series object is treated as a DataFrame with a single named column.

    The join is done on columns or indexes. If joining columns on
    columns, the DataFrame indexes *will be ignored*. Otherwise if joining indexes
    on indexes or indexes on a column or columns, the index will be passed on.
    When performing a cross merge, no column specifications to merge on are
    allowed.

    .. warning::

        If both key columns contain rows where the key is a null value, those
        rows will be matched against each other. This is different from usual SQL
        join behaviour and can lead to unexpected results.

    Parameters
    ----------
    left : :class:`~modin.pandas.DataFrame` or named Series
    right : :class:`~modin.pandas.DataFrame` or named Series
        Object to merge with.
    how : {'left', 'right', 'outer', 'inner', 'cross'}, default 'inner'
        Type of merge to be performed.

        * left: use only keys from left frame, similar to a SQL left outer join;
          preserve key order.
        * right: use only keys from right frame, similar to a SQL right outer join;
          preserve key order.
        * outer: use union of keys from both frames, similar to a SQL full outer
          join; sort keys lexicographically.
        * inner: use intersection of keys from both frames, similar to a SQL inner
          join; preserve the order of the left keys.
        * cross: creates the cartesian product from both frames, preserves the order
          of the left keys.

    on : label or list
        Column or index level names to join on. These must be found in both
        DataFrames. If `on` is None and not merging on indexes then this defaults
        to the intersection of the columns in both DataFrames.
    left_on : label or list, or array-like
        Column or index level names to join on in the left DataFrame. Can also
        be an array or list of arrays of the length of the left DataFrame.
        These arrays are treated as if they are columns.
    right_on : label or list, or array-like
        Column or index level names to join on in the right DataFrame. Can also
        be an array or list of arrays of the length of the right DataFrame.
        These arrays are treated as if they are columns.
    left_index : bool, default False
        Use the index from the left DataFrame as the join key(s). If it is a
        MultiIndex, the number of keys in the other DataFrame (either the index
        or a number of columns) must match the number of levels.
    right_index : bool, default False
        Use the index from the right DataFrame as the join key. Same caveats as
        left_index.
    sort : bool, default False
        Sort the join keys lexicographically in the result DataFrame. If False,
        the order of the join keys depends on the join type (how keyword).
    suffixes : list-like, default is ("_x", "_y")
        A length-2 sequence where each element is optionally a string
        indicating the suffix to add to overlapping column names in
        `left` and `right` respectively. Pass a value of `None` instead
        of a string to indicate that the column name from `left` or
        `right` should be left as-is, with no suffix. At least one of the
        values must not be None.
    copy : bool, default True
        This argument is ignored in Snowpark pandas API.
    indicator : bool or str, default False
        If True, adds a column to the output DataFrame called "_merge" with
        information on the source of each row. The column can be given a different
        name by providing a string argument. The column will have a Categorical
        type with the value of "left_only" for observations whose merge key only
        appears in the left DataFrame, "right_only" for observations
        whose merge key only appears in the right DataFrame, and "both"
        if the observation's merge key is found in both DataFrames.

    validate : str, optional
        This is not yet supported.

    Returns
    -------
    :class:`~modin.pandas.DataFrame`
        A DataFrame of the two merged objects.

    See Also
    --------
    merge_ordered : Merge with optional filling/interpolation.
    merge_asof : Merge on nearest keys.
    DataFrame.join : Similar method using indices.

    Examples
    --------
    >>> df1 = pd.DataFrame({'lkey': ['foo', 'bar', 'baz', 'foo'],
    ...                     'value': [1, 2, 3, 5]})
    >>> df2 = pd.DataFrame({'rkey': ['foo', 'bar', 'baz', 'foo'],
    ...                     'value': [5, 6, 7, 8]})
    >>> df1
      lkey  value
    0  foo      1
    1  bar      2
    2  baz      3
    3  foo      5
    >>> df2
      rkey  value
    0  foo      5
    1  bar      6
    2  baz      7
    3  foo      8

    Merge df1 and df2 on the lkey and rkey columns. The value columns have
    the default suffixes, _x and _y, appended.

    >>> df1.merge(df2, left_on='lkey', right_on='rkey')
      lkey  value_x rkey  value_y
    0  foo        1  foo        5
    1  foo        1  foo        8
    2  bar        2  bar        6
    3  baz        3  baz        7
    4  foo        5  foo        5
    5  foo        5  foo        8

    Merge DataFrames df1 and df2 with specified left and right suffixes
    appended to any overlapping columns.

    >>> df1.merge(df2, left_on='lkey', right_on='rkey',
    ...           suffixes=('_left', '_right'))
      lkey  value_left rkey  value_right
    0  foo           1  foo            5
    1  foo           1  foo            8
    2  bar           2  bar            6
    3  baz           3  baz            7
    4  foo           5  foo            5
    5  foo           5  foo            8


    >>> df1 = pd.DataFrame({'a': ['foo', 'bar'], 'b': [1, 2]})
    >>> df2 = pd.DataFrame({'a': ['foo', 'baz'], 'c': [3, 4]})
    >>> df1
         a  b
    0  foo  1
    1  bar  2
    >>> df2
         a  c
    0  foo  3
    1  baz  4

    >>> df1.merge(df2, how='inner', on='a')
         a  b  c
    0  foo  1  3

    >>> df1.merge(df2, how='left', on='a')
         a  b    c
    0  foo  1  3.0
    1  bar  2  NaN

    >>> df1 = pd.DataFrame({'left': ['foo', 'bar']})
    >>> df2 = pd.DataFrame({'right': [7, 8]})
    >>> df1
      left
    0  foo
    1  bar
    >>> df2
       right
    0      7
    1      8

    >>> df1.merge(df2, how='cross')
      left  right
    0  foo      7
    1  foo      8
    2  bar      7
    3  bar      8
    """


def merge_ordered():
    """
    Merge DataFrame or named Series objects with a database-style join.

    A named Series object is treated as a DataFrame with a single named column.

    The join is done on columns or indexes. If joining columns on
    columns, the DataFrame indexes *will be ignored*. Otherwise if joining indexes
    on indexes or indexes on a column or columns, the index will be passed on.
    When performing a cross merge, no column specifications to merge on are
    allowed.

    .. warning::

        If both key columns contain rows where the key is a null value, those
        rows will be matched against each other. This is different from usual SQL
        join behaviour and can lead to unexpected results.

    Parameters
    ----------
    left : :class:`~modin.pandas.DataFrame` or named Series
    right : :class:`~modin.pandas.DataFrame` or named Series
        Object to merge with.
    how : {'left', 'right', 'outer', 'inner', 'cross'}, default 'inner'
        Type of merge to be performed.

        * left: use only keys from left frame, similar to a SQL left outer join;
          preserve key order.
        * right: use only keys from right frame, similar to a SQL right outer join;
          preserve key order.
        * outer: use union of keys from both frames, similar to a SQL full outer
          join; sort keys lexicographically.
        * inner: use intersection of keys from both frames, similar to a SQL inner
          join; preserve the order of the left keys.
        * cross: creates the cartesian product from both frames, preserves the order
          of the left keys.

    on : label or list
        Column or index level names to join on. These must be found in both
        DataFrames. If `on` is None and not merging on indexes then this defaults
        to the intersection of the columns in both DataFrames.
    left_on : label or list, or array-like
        Column or index level names to join on in the left DataFrame. Can also
        be an array or list of arrays of the length of the left DataFrame.
        These arrays are treated as if they are columns.
    right_on : label or list, or array-like
        Column or index level names to join on in the right DataFrame. Can also
        be an array or list of arrays of the length of the right DataFrame.
        These arrays are treated as if they are columns.
    left_index : bool, default False
        Use the index from the left DataFrame as the join key(s). If it is a
        MultiIndex, the number of keys in the other DataFrame (either the index
        or a number of columns) must match the number of levels.
    right_index : bool, default False
        Use the index from the right DataFrame as the join key. Same caveats as
        left_index.
    sort : bool, default False
        Sort the join keys lexicographically in the result DataFrame. If False,
        the order of the join keys depends on the join type (how keyword).
    suffixes : list-like, default is ("_x", "_y")
        A length-2 sequence where each element is optionally a string
        indicating the suffix to add to overlapping column names in
        `left` and `right` respectively. Pass a value of `None` instead
        of a string to indicate that the column name from `left` or
        `right` should be left as-is, with no suffix. At least one of the
        values must not be None.
    copy : bool, default True
        This argument is ignored in Snowpark pandas API.
    indicator : bool or str, default False
        If True, adds a column to the output DataFrame called "_merge" with
        information on the source of each row. The column can be given a different
        name by providing a string argument. The column will have a Categorical
        type with the value of "left_only" for observations whose merge key only
        appears in the left DataFrame, "right_only" for observations
        whose merge key only appears in the right DataFrame, and "both"
        if the observation's merge key is found in both DataFrames.

    validate : str, optional
        This is not yet supported.

    Returns
    -------
    :class:`~modin.pandas.DataFrame`
        A DataFrame of the two merged objects.

    See Also
    --------
    merge_ordered : Merge with optional filling/interpolation.
    merge_asof : Merge on nearest keys.
    DataFrame.join : Similar method using indices.

    Examples
    --------
    >>> df1 = pd.DataFrame({'lkey': ['foo', 'bar', 'baz', 'foo'],
    ...                     'value': [1, 2, 3, 5]})
    >>> df2 = pd.DataFrame({'rkey': ['foo', 'bar', 'baz', 'foo'],
    ...                     'value': [5, 6, 7, 8]})
    >>> df1
      lkey  value
    0  foo      1
    1  bar      2
    2  baz      3
    3  foo      5
    >>> df2
      rkey  value
    0  foo      5
    1  bar      6
    2  baz      7
    3  foo      8

    Merge df1 and df2 on the lkey and rkey columns. The value columns have
    the default suffixes, _x and _y, appended.

    >>> df1.merge(df2, left_on='lkey', right_on='rkey')
      lkey  value_x rkey  value_y
    0  foo        1  foo        5
    1  foo        1  foo        8
    2  bar        2  bar        6
    3  baz        3  baz        7
    4  foo        5  foo        5
    5  foo        5  foo        8

    Merge DataFrames df1 and df2 with specified left and right suffixes
    appended to any overlapping columns.

    >>> df1.merge(df2, left_on='lkey', right_on='rkey',
    ...           suffixes=('_left', '_right'))
      lkey  value_left rkey  value_right
    0  foo           1  foo            5
    1  foo           1  foo            8
    2  bar           2  bar            6
    3  baz           3  baz            7
    4  foo           5  foo            5
    5  foo           5  foo            8


    >>> df1 = pd.DataFrame({'a': ['foo', 'bar'], 'b': [1, 2]})
    >>> df2 = pd.DataFrame({'a': ['foo', 'baz'], 'c': [3, 4]})
    >>> df1
         a  b
    0  foo  1
    1  bar  2
    >>> df2
         a  c
    0  foo  3
    1  baz  4

    >>> df1.merge(df2, how='inner', on='a')
         a  b  c
    0  foo  1  3

    >>> df1.merge(df2, how='left', on='a')
         a  b    c
    0  foo  1  3.0
    1  bar  2  NaN

    >>> df1 = pd.DataFrame({'left': ['foo', 'bar']})
    >>> df2 = pd.DataFrame({'right': [7, 8]})
    >>> df1
      left
    0  foo
    1  bar
    >>> df2
       right
    0      7
    1      8

    >>> df1.merge(df2, how='cross')
      left  right
    0  foo      7
    1  foo      8
    2  bar      7
    3  bar      8
    """


def merge_asof():
    """
    Perform a merge by key distance.

    This is similar to a left-join except that we match on nearest key rather than equal keys.
    Both DataFrames must be sorted by the key. For each row in the left DataFrame:

    A “backward” search selects the last row in the right DataFrame whose ‘on’ key is less than or equal to the left’s key.
    A “forward” search selects the first row in the right DataFrame whose ‘on’ key is greater than or equal to the left’s key.
    A “nearest” search selects the row in the right DataFrame whose ‘on’ key is closest in absolute distance to the left’s key.

    Optionally match on equivalent keys with ‘by’ before searching with ‘on’.

    Parameters
    ----------
    left : :class:`~modin.pandas.DataFrame` or named :class:`~modin.pandas.Series`.
    right : :class:`~modin.pandas.DataFrame` or named :class:`~modin.pandas.Series`.
    on : label
        Field name to join on. Must be found in both DataFrames. The data MUST be ordered.
        Furthermore, this must be a numeric column such as datetimelike, integer, or float.
        On or left_on/right_on must be given.
    left_on : label
        Field name to join on in left DataFrame.
    right_on : label
        Field name to join on in right DataFrame.
    left_index : bool
        Use the index of the left DataFrame as the join key.
    right_index : bool
        Use the index of the right DataFrame as the join key.
    by : column name or list of column names
        Match on these columns before performing merge operation.
    left_by : column name
        Field names to match on in the left DataFrame.
    right_by : column name
        Field names to match on in the right DataFrame.
    suffixes : 2-length sequence (tuple, list, …)
        Suffix to apply to overlapping column names in the left and right side, respectively.
    tolerance: int or Timedelta, optional, default None
        Select asof tolerance within this range; must be compatible with the merge index.
    allow_exact_matches : bool, default True
        If True, allow matching with the same ‘on’ value (i.e. less-than-or-equal-to / greater-than-or-equal-to)
        If False, don’t match the same ‘on’ value (i.e., strictly less-than / strictly greater-than).
    direction : ‘backward’ (default), ‘forward’, or ‘nearest’
        Whether to search for prior, subsequent, or closest matches.

    Returns
    -------
    Snowpark pandas :class:`~modin.pandas.DataFrame`

    Examples
    --------
    >>> left = pd.DataFrame({"a": [1, 5, 10], "left_val": ["a", "b", "c"]})
    >>> left
        a left_val
    0   1        a
    1   5        b
    2  10        c
    >>> right = pd.DataFrame({"a": [1, 2, 3, 6, 7], "right_val": [1, 2, 3, 6, 7]})
    >>> right
       a  right_val
    0  1          1
    1  2          2
    2  3          3
    3  6          6
    4  7          7
    >>> pd.merge_asof(left, right, on="a")
        a left_val  right_val
    0   1        a          1
    1   5        b          3
    2  10        c          7
    >>> pd.merge_asof(left, right, on="a", allow_exact_matches=False)
        a left_val  right_val
    0   1        a        NaN
    1   5        b        3.0
    2  10        c        7.0
    >>> pd.merge_asof(left, right, on="a", direction="forward")
        a left_val  right_val
    0   1        a        1.0
    1   5        b        6.0
    2  10        c        NaN

    Here is a real-world times-series example:

    >>> quotes = pd.DataFrame(
    ...    {
    ...        "time": [
    ...            pd.Timestamp("2016-05-25 13:30:00.023"),
    ...            pd.Timestamp("2016-05-25 13:30:00.023"),
    ...            pd.Timestamp("2016-05-25 13:30:00.030"),
    ...            pd.Timestamp("2016-05-25 13:30:00.041"),
    ...            pd.Timestamp("2016-05-25 13:30:00.048"),
    ...            pd.Timestamp("2016-05-25 13:30:00.049"),
    ...            pd.Timestamp("2016-05-25 13:30:00.072"),
    ...            pd.Timestamp("2016-05-25 13:30:00.075")
    ...        ],
    ...        "bid": [720.50, 51.95, 51.97, 51.99, 720.50, 97.99, 720.50, 52.01],
    ...        "ask": [720.93, 51.96, 51.98, 52.00, 720.93, 98.01, 720.88, 52.03]
    ...    }
    ... )
    >>> quotes
                         time     bid     ask
    0 2016-05-25 13:30:00.023  720.50  720.93
    1 2016-05-25 13:30:00.023   51.95   51.96
    2 2016-05-25 13:30:00.030   51.97   51.98
    3 2016-05-25 13:30:00.041   51.99   52.00
    4 2016-05-25 13:30:00.048  720.50  720.93
    5 2016-05-25 13:30:00.049   97.99   98.01
    6 2016-05-25 13:30:00.072  720.50  720.88
    7 2016-05-25 13:30:00.075   52.01   52.03
    >>> trades = pd.DataFrame(
    ...    {
    ...        "time": [
    ...            pd.Timestamp("2016-05-25 13:30:00.023"),
    ...            pd.Timestamp("2016-05-25 13:30:00.038"),
    ...            pd.Timestamp("2016-05-25 13:30:00.048"),
    ...            pd.Timestamp("2016-05-25 13:30:00.048"),
    ...            pd.Timestamp("2016-05-25 13:30:00.048")
    ...        ],
    ...        "price": [51.95, 51.95, 720.77, 720.92, 98.0],
    ...        "quantity": [75, 155, 100, 100, 100]
    ...    }
    ... )
    >>> trades
                         time   price  quantity
    0 2016-05-25 13:30:00.023   51.95        75
    1 2016-05-25 13:30:00.038   51.95       155
    2 2016-05-25 13:30:00.048  720.77       100
    3 2016-05-25 13:30:00.048  720.92       100
    4 2016-05-25 13:30:00.048   98.00       100
    >>> pd.merge_asof(trades, quotes, on="time")
                         time   price  quantity     bid     ask
    0 2016-05-25 13:30:00.023   51.95        75   51.95   51.96
    1 2016-05-25 13:30:00.038   51.95       155   51.97   51.98
    2 2016-05-25 13:30:00.048  720.77       100  720.50  720.93
    3 2016-05-25 13:30:00.048  720.92       100  720.50  720.93
    4 2016-05-25 13:30:00.048   98.00       100  720.50  720.93
    """


def concat():
    """
    Concatenate pandas objects along a particular axis.

    Allows optional set logic along the other axes.

    Can also add a layer of hierarchical indexing on the concatenation axis,
    which may be useful if the labels are the same (or overlapping) on
    the passed axis number.

    Parameters
    ----------
    objs : a sequence or mapping of Series or DataFrame objects
        If a mapping is passed, the sorted keys will be used as the `keys`
        argument, unless it is passed, in which case the values will be
        selected (see below). Any None objects will be dropped silently unless
        they are all None in which case a ValueError will be raised.
    axis : {0/'index', 1/'columns'}, default 0
        The axis to concatenate along.
    join : {'inner', 'outer'}, default 'outer'
        How to handle indexes on other axis (or axes).
    ignore_index : bool, default False
        If True, do not use the index values along the concatenation axis. The
        resulting axis will be labeled 0, ..., n - 1. This is useful if you are
        concatenating objects where the concatenation axis does not have
        meaningful indexing information. Note the index values on the other
        axes are still respected in the join.
    keys : sequence, default None
        If multiple levels passed, should contain tuples. Construct
        hierarchical index using the passed keys as the outermost level.
    levels : list of sequences, default None
        Specific levels (unique values) to use for constructing a
        MultiIndex. Otherwise they will be inferred from the keys.
        Snowpark pandas does not support 'levels' argument.
    names : list, default None
        Names for the levels in the resulting hierarchical index.
    verify_integrity : bool, default False
        Check whether the new concatenated axis contains duplicates.
        Snowpark pandas does not support distributed computation of concat when
        'verify_integrity' is True.
    sort : bool, default False
        Sort non-concatenation axis if it is not already aligned.
    copy : bool, default True
        If False, do not copy data unnecessarily.
        This argument is ignored in Snowpark pandas.

    Returns
    -------
    object, type of objs
        When concatenating all Snowpark pandas :class:`~modin.pandas.Series` along the index (axis=0),
        a Snowpark pandas :class:`~modin.pandas.Series` is returned. When ``objs`` contains at least
        one Snowpark pandas :class:`~modin.pandas.DataFrame`,
        a Snowpark pandas :class:`~modin.pandas.DataFrame` is returned. When concatenating along
        the columns (axis=1), a Snowpark pandas :class:`~modin.pandas.DataFrame` is returned.

    See Also
    --------
    DataFrame.join : Join DataFrames using indexes.
    DataFrame.merge : Merge DataFrames by indexes or columns.

    Notes
    -----
    The keys, levels, and names arguments are all optional.

    It is not recommended to build DataFrames by adding single rows in a
    for loop. Build a list of rows and make a DataFrame in a single concat.

    Examples
    --------
    Combine two ``Series``.

    >>> s1 = pd.Series(['a', 'b'])
    >>> s2 = pd.Series(['c', 'd'])
    >>> pd.concat([s1, s2])
    0    a
    1    b
    0    c
    1    d
    dtype: object

    Clear the existing index and reset it in the result
    by setting the ``ignore_index`` option to ``True``.

    >>> pd.concat([s1, s2], ignore_index=True)
    0    a
    1    b
    2    c
    3    d
    dtype: object

    Add a hierarchical index at the outermost level of
    the data with the ``keys`` option.

    >>> pd.concat([s1, s2], keys=['s1', 's2'])
    s1  0    a
        1    b
    s2  0    c
        1    d
    dtype: object

    Label the index keys you create with the ``names`` option.

    >>> pd.concat([s1, s2], keys=['s1', 's2'],
    ...           names=['Series name', 'Row ID'])
    Series name  Row ID
    s1           0         a
                 1         b
    s2           0         c
                 1         d
    dtype: object

    Combine two ``DataFrame`` objects with identical columns.

    >>> df1 = pd.DataFrame([['a', 1], ['b', 2]],
    ...                    columns=['letter', 'number'])
    >>> df1
      letter  number
    0      a       1
    1      b       2
    >>> df2 = pd.DataFrame([['c', 3], ['d', 4]],
    ...                    columns=['letter', 'number'])
    >>> df2
      letter  number
    0      c       3
    1      d       4
    >>> pd.concat([df1, df2])
      letter  number
    0      a       1
    1      b       2
    0      c       3
    1      d       4

    Combine ``DataFrame`` objects with overlapping columns
    and return everything. Columns outside the intersection will
    be filled with ``NaN`` values.

    >>> df3 = pd.DataFrame([['c', 3, 'cat'], ['d', 4, 'dog']],
    ...                    columns=['letter', 'number', 'animal'])
    >>> df3
      letter  number animal
    0      c       3    cat
    1      d       4    dog
    >>> pd.concat([df1, df3], sort=False)
      letter  number animal
    0      a       1   None
    1      b       2   None
    0      c       3    cat
    1      d       4    dog

    Combine ``DataFrame`` objects with overlapping columns
    and return only those that are shared by passing ``inner`` to
    the ``join`` keyword argument.

    >>> pd.concat([df1, df3], join="inner")
      letter  number
    0      a       1
    1      b       2
    0      c       3
    1      d       4

    Combine ``DataFrame`` objects horizontally along the x axis by
    passing in ``axis=1``.

    >>> df4 = pd.DataFrame([['bird', 'polly'], ['monkey', 'george']],
    ...                    columns=['animal', 'name'])
    >>> pd.concat([df1, df4], axis=1)
      letter  number  animal    name
    0      a       1    bird   polly
    1      b       2  monkey  george

    Combining series horizontally creates a DataFrame. Missing names are replaced with
    numeric values.

    >>> pd.concat([s1, s2], axis=1)
       0  1
    0  a  c
    1  b  d

    When combining objects horizoantally ``ignore_index=True`` will clear the existing
    column names and reset it in the result.

    >>> pd.concat([df1, df4], axis=1, ignore_index=True)
       0  1       2       3
    0  a  1    bird   polly
    1  b  2  monkey  george

    When combining objects horizontally, add a hierarchical column index at the
    outermost level of the column labels with the ``keys`` option.

    >>> pd.concat([df1, df4], axis=1, keys=['x', 'y']) # doctest: +NORMALIZE_WHITESPACE
           x              y
      letter number  animal    name
    0      a      1    bird   polly
    1      b      2  monkey  george

    Concatenatiing series horizontally with ``keys``.

    >>> pd.concat([s1, s2], axis=1, keys=['x', 'y'])
       x  y
    0  a  c
    1  b  d

    When combining objects horizontally, ``how='inner'`` to keep only overalpping
    index values.

    >>> df5 = pd.DataFrame([['a', 1], ['b', 2]],
    ...                    columns=['letter', 'number'],
    ...                    index=[1, 2])
    >>> df5
      letter  number
    1      a       1
    2      b       2
    >>> pd.concat([df1, df5], axis=1, join='inner')
      letter  number letter  number
    1      b       2      a       1

    Prevent the result from including duplicate index values with the
    ``verify_integrity`` option.

    >>> df5 = pd.DataFrame([1], index=['a'])
    >>> df5
       0
    a  1
    >>> df6 = pd.DataFrame([2], index=['a'])
    >>> df6
       0
    a  2
    >>> pd.concat([df5, df6], verify_integrity=True)
    Traceback (most recent call last):
        ...
    ValueError: Indexes have overlapping values: Index(['a'], dtype='object')

    Append a single row to the end of a ``DataFrame`` object.

    >>> df7 = pd.DataFrame({'a': 1, 'b': 2}, index=[0])
    >>> df7
       a  b
    0  1  2
    >>> new_row = pd.DataFrame({'a': 3, 'b': 4}, index=[0])
    >>> new_row
       a  b
    0  3  4
    >>> pd.concat([df7, new_row], ignore_index=True)
       a  b
    0  1  2
    1  3  4
    """


def get_dummies():
    """
    Convert categorical variable into dummy/indicator variables.

    Parameters
    ----------
    data : array-like, Series, or :class:`~modin.pandas.DataFrame`
        Data of which to get dummy indicators.
    prefix : str, list of str, or dict of str, default None
        String to append DataFrame column names.
        Pass a list with length equal to the number of columns
        when calling get_dummies on a DataFrame. Alternatively, `prefix`
        can be a dictionary mapping column names to prefixes.
        Only str, list of str and None is supported for this parameter.
    prefix_sep : str, default '_'
        If appending prefix, separator/delimiter to use.
    dummy_na : bool, default False
        Add a column to indicate NaNs, if False NaNs are ignored. Only the
        value False is supported for this parameter.
    columns : list-like, default None
        Column names in the DataFrame to be encoded.
        If `columns` is None then all the columns with
        `string` dtype will be converted.
    sparse : bool, default False
        Whether the dummy-encoded columns should be backed by
        a :class:`SparseArray` (True) or a regular NumPy array (False).
        This parameter is ignored.
    drop_first : bool, default False
        Whether to get k-1 dummies out of k categorical levels by removing the
        first level. Only the value False is supported for this parameter.
    dtype : dtype, default np.uint8
        Data type for new columns. Only the value None is supported for this parameter.

    Returns
    -------
    :class:`~modin.pandas.DataFrame`
        Dummy-coded data.

    Examples
    --------
    >>> s = pd.Series(list('abca'))

    >>> pd.get_dummies(s)
           a      b      c
    0   True  False  False
    1  False   True  False
    2  False  False   True
    3   True  False  False

    >>> df = pd.DataFrame({'A': ['a', 'b', 'a'], 'B': ['b', 'a', 'c'],
    ...                    'C': [1, 2, 3]})

    >>> pd.get_dummies(df, prefix=['col1', 'col2'])
       C  col1_a  col1_b  col2_a  col2_b  col2_c
    0  1    True   False   False    True   False
    1  2   False    True    True   False   False
    2  3    True   False   False   False    True

    >>> pd.get_dummies(pd.Series(list('abcaa')))
           a      b      c
    0   True  False  False
    1  False   True  False
    2  False  False   True
    3   True  False  False
    4   True  False  False
    """


def unique():
    """
    Return unique values based on a hash table. Unique values are
    returned in the order of appearance. This does NOT sort.

    Parameters
    ----------
    values : ndarray (1-d), list, bytearray, tuple, Series, Index, list-like
        Non-hashable objects like set, dict, and user defined classes are
        invalid input.
        Values to perform computation.

    Returns
    -------
    ndarray
    The unique values returned as a NumPy array. See Notes.

    See Also
    --------
    Series.unique()

    Notes
    -----
    Returns the unique values as a NumPy array. This includes

        * Datetime with Timezone
        * IntegerNA

    See Examples section.

    Examples
    --------
    >>> pd.unique([2, 1, 3, 3])
    array([2, 1, 3])

    >>> pd.unique([pd.Timestamp('2016-01-01', tz='US/Eastern')
    ...            for _ in range(3)])
    array([Timestamp('2016-01-01 00:00:00-0500', tz='UTC-05:00')],
          dtype=object)

    >>> pd.unique([("a", "b"), ("b", "a"), ("a", "c"), ("b", "a")])
    array([list(['a', 'b']), list(['b', 'a']), list(['a', 'c'])], dtype=object)

    >>> pd.unique([None, np.nan, 2])
    array([nan,  2.])
    """


def lreshape():
    """
    Reshape wide-format data to long. Generalized inverse of ``DataFrame.pivot``.

    Accepts a dictionary, `groups`, in which each key is a new column name
    and each value is a list of old column names that will be "melted" under
    the new column name as part of the reshape.

    Parameters
    ----------
    data : DataFrame
        The wide-format DataFrame.
    groups : dict
        Dictionary in the form: `{new_name : list_of_columns}`.
    dropna : bool, default: True
        Whether include columns whose entries are all NaN or not.
    label : optional
        Deprecated parameter.

    Returns
    -------
    DataFrame
        Reshaped DataFrame.
    """


def wide_to_long():
    """
    Reshape wide-format data to long. Generalized inverse of ``DataFrame.pivot``.

    Accepts a dictionary, `groups`, in which each key is a new column name
    and each value is a list of old column names that will be "melted" under
    the new column name as part of the reshape.

    Parameters
    ----------
    data : DataFrame
        The wide-format DataFrame.
    groups : dict
        Dictionary in the form: `{new_name : list_of_columns}`.
    dropna : bool, default: True
        Whether include columns whose entries are all NaN or not.
    label : optional
        Deprecated parameter.

    Returns
    -------
    DataFrame
        Reshaped DataFrame.
    """


def isna():
    """
    Detect missing values for an array-like object.
    """


def notna():
    """
    Detect non-missing values for an array-like object.
    """


def to_numeric():
    """
    Convert argument to a numeric type.

    If the input arg type is already a numeric type, the return dtype
    will be the original type; otherwise, the return dtype is float.

    Parameters
    ----------
    arg : scalar, list, tuple, 1-d array, or Series
        Argument to be converted.
    errors : {'ignore', 'raise', 'coerce'}, default 'raise'
        - If 'raise', then invalid parsing will raise an exception.
        - If 'coerce', then invalid parsing will be set as NaN.
        - If 'ignore', then invalid parsing will return the input.
    downcast : str, default None
        downcast is ignored in Snowflake backend.

    Returns
    -------
    ret
        Numeric if parsing succeeded.
        Return type depends on input.  Series if `arg` is not scalar.

    See Also
    --------
    DataFrame.astype : Cast argument to a specified dtype.
    to_datetime : Convert argument to datetime.
    to_timedelta : Convert argument to timedelta.
    numpy.ndarray.astype : Cast a numpy array to a specified type.
    DataFrame.convert_dtypes : Convert dtypes.

    Examples
    --------
    Take separate series and convert to numeric, coercing when told to

    >>> s = pd.Series(['1.0', '2', -3])
    >>> pd.to_numeric(s)
    0    1.0
    1    2.0
    2   -3.0
    dtype: float64

    Note: to_numeric always converts non-numeric values to floats
    >>> s = pd.Series(['1', '2', '-3'])
    >>> pd.to_numeric(s)
    0    1.0
    1    2.0
    2   -3.0
    dtype: float64
    >>> pd.to_numeric(s, downcast='float')  # downcast is ignored
    0    1.0
    1    2.0
    2   -3.0
    dtype: float64
    >>> pd.to_numeric(s, downcast='signed')  # downcast is ignored
    0    1.0
    1    2.0
    2   -3.0
    dtype: float64
    >>> s = pd.Series(['apple', '1.0', '2', -3])
    >>> pd.to_numeric(s, errors='coerce')
    0    NaN
    1    1.0
    2    2.0
    3   -3.0
    dtype: float64
    """


def to_datetime():
    """
    Convert argument to datetime.

    This function converts a scalar, array-like, :class:`~modin.pandas.Series` or
    :class:`~modin.pandas.DataFrame`/dict-like to a pandas datetime object.

    Parameters
    ----------
    arg : int, float, str, datetime, list, tuple, 1-d array, Series, :class:`~modin.pandas.DataFrame`/dict-like
        The object to convert to a datetime. If a :class:`~modin.pandas.DataFrame` is provided, the
        method expects minimally the following columns: :const:`"year"`,
        :const:`"month"`, :const:`"day"`.
    errors : {'ignore', 'raise', 'coerce'}, default 'raise'
        - If :const:`'raise'`, then invalid parsing will raise an exception.
        - If :const:`'coerce'`, then invalid parsing will be set as :const:`NaT`.
        - If :const:`'ignore'`, then invalid parsing will return the input.
    dayfirst : bool, default False
        Specify a date parse order if `arg` is str or is list-like.
        If :const:`True`, parses dates with the day first, e.g. :const:`"10/11/12"`
        is parsed as :const:`2012-11-10`.

        .. warning::

            ``dayfirst=True`` is not strict, but will prefer to parse
            with day first. If a delimited date string cannot be parsed in
            accordance with the given `dayfirst` option, e.g.
            ``to_datetime(['31-12-2021'])``, then a warning will be shown.

    yearfirst : bool, default False
        Specify a date parse order if `arg` is str or is list-like.

        - If :const:`True` parses dates with the year first, e.g.
          :const:`"10/11/12"` is parsed as :const:`2010-11-12`.
        - If both `dayfirst` and `yearfirst` are :const:`True`, `yearfirst` is
          preceded (same as :mod:`dateutil`).

        .. warning::

            ``yearfirst=True`` is not strict, but will prefer to parse
            with year first.

    utc : bool, default None
        Control timezone-related parsing, localization and conversion.

        - If :const:`True`, the function *always* returns a timezone-aware
          UTC-localized :class:`Timestamp`, :class:`~modin.pandas.Series` or
          :class:`DatetimeIndex`. To do this, timezone-naive inputs are
          *localized* as UTC, while timezone-aware inputs are *converted* to UTC.

        - If :const:`False` (default), inputs will not be coerced to UTC.
          Timezone-naive inputs will remain naive, while timezone-aware ones
          will keep their time offsets. Limitations exist for mixed
          offsets (typically, daylight savings), see :ref:`Examples
          <to_datetime_tz_examples>` section for details.

        See also: pandas general documentation about `timezone conversion and
        localization
        <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html
        #time-zone-handling>`_.

    format : str, default None
        The strftime to parse time, e.g. :const:`"%d/%m/%Y"`. Note that
        :const:`"%f"` will parse all the way up to nanoseconds. See
        `strftime documentation
        <https://docs.python.org/3/library/datetime.html
        #strftime-and-strptime-behavior>`_ for more information on choices.
    exact : bool, default True
        Control how `format` is used:

        - If :const:`True`, require an exact `format` match.
        - If :const:`False`, allow the `format` to match anywhere in the target
          string.

    unit : str, default 'ns'
        The unit of the arg (D,s,ms,us,ns) denote the unit, which is an
        integer or float number. This will be based off the origin.
        Example, with ``unit='ms'`` and ``origin='unix'``, this would calculate
        the number of milliseconds to the unix epoch start.
    infer_datetime_format : bool, default False
        If :const:`True` and no `format` is given, attempt to infer the format
        of the datetime strings based on the first non-NaN element,
        and if it can be inferred, switch to a faster method of parsing them.
        In some cases this can increase the parsing speed by ~5-10x.
    origin : scalar, default 'unix'
        Define the reference date. The numeric values would be parsed as number
        of units (defined by `unit`) since this reference date.

        - If :const:`'unix'` (or POSIX) time; origin is set to 1970-01-01.
        - If :const:`'julian'`, unit must be :const:`'D'`, and origin is set to
          beginning of Julian Calendar. Julian day number :const:`0` is assigned
          to the day starting at noon on January 1, 4713 BC.
        - If Timestamp convertible, origin is set to Timestamp identified by
          origin.
    cache : bool, default True
        cache parameter is ignored with Snowflake backend, i.e., no caching will be
        applied

    Returns
    -------
    datetime
        If parsing succeeded.
        Return type depends on input (types in parenthesis correspond to
        fallback in case of unsuccessful timezone or out-of-range timestamp
        parsing):

        - scalar: :class:`Timestamp` (or :class:`datetime.datetime`)
        - array-like: :class:`~modin.pandas.DatetimeIndex` (or
          :class: :class:`~modin.pandas.Series` of :class:`object` dtype containing
          :class:`datetime.datetime`)
        - Series: :class:`~modin.pandas.Series` of :class:`datetime64` dtype (or
          :class: :class:`~modin.pandas.Series` of :class:`object` dtype containing
          :class:`datetime.datetime`)
        - DataFrame: :class:`~modin.pandas.Series` of :class:`datetime64` dtype (or
          :class:`~modin.pandas.Series` of :class:`object` dtype containing
          :class:`datetime.datetime`)

    Raises
    ------
    ParserError
        When parsing a date from string fails.
    ValueError
        When another datetime conversion error happens. For example when one
        of 'year', 'month', day' columns is missing in a :class:`~modin.pandas.DataFrame`, or
        when a Timezone-aware :class:`datetime.datetime` is found in an array-like
        of mixed time offsets, and ``utc=False``.

    See Also
    --------
    DataFrame.astype : Cast argument to a specified dtype.
    to_timedelta : Convert argument to timedelta.
    convert_dtypes : Convert dtypes.

    Notes
    -----

    Many input types are supported, and lead to different output types:

    - **scalars** can be int, float, str, datetime object (from stdlib :mod:`datetime`
      module or :mod:`numpy`). They are converted to :class:`Timestamp` when
      possible, otherwise they are converted to :class:`datetime.datetime`.
      None/NaN/null scalars are converted to :const:`NaT`.

    - **array-like** can contain int, float, str, datetime objects. They are
      converted to :class:`DatetimeIndex` when possible, otherwise they are
      converted to :class:`Index` with :class:`object` dtype, containing
      :class:`datetime.datetime`. None/NaN/null entries are converted to
      :const:`NaT` in both cases.

    - **Series** are converted to :class:`~modin.pandas.Series` with :class:`datetime64`
      dtype when possible, otherwise they are converted to :class:`~modin.pandas.Series` with
      :class:`object` dtype, containing :class:`datetime.datetime`. None/NaN/null
      entries are converted to :const:`NaT` in both cases.

    - **DataFrame/dict-like** are converted to :class:`~modin.pandas.Series` with
      :class:`datetime64` dtype. For each row a datetime is created from assembling
      the various dataframe columns. Column keys can be common abbreviations
      like [‘year’, ‘month’, ‘day’, ‘minute’, ‘second’, ‘ms’, ‘us’, ‘ns’]) or
      plurals of the same.

    The following causes are responsible for :class:`datetime.datetime` objects
    being returned (possibly inside an :class:`Index` or a :class:`~modin.pandas.Series` with
    :class:`object` dtype) instead of a proper pandas designated type
    (:class:`Timestamp` or :class:`~modin.pandas.Series` with :class:`datetime64` dtype):

    - when any input element is before :const:`Timestamp.min` or after
      :const:`Timestamp.max`, see `timestamp limitations
      <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html
      #timeseries-timestamp-limits>`_.

    - when ``utc=False`` (default) and the input is an array-like or
      :class:`~modin.pandas.Series` containing mixed naive/aware datetime, or aware with mixed
      time offsets. Note that this happens in the (quite frequent) situation when
      the timezone has a daylight savings policy. In that case you may wish to
      use ``utc=True``.

    Examples
    --------

    **Handling various input formats**

    Assembling a datetime from multiple columns of a :class:`~modin.pandas.DataFrame`. The keys
    can be common abbreviations like ['year', 'month', 'day', 'minute', 'second',
    'ms', 'us', 'ns']) or plurals of the same

    >>> df = pd.DataFrame({'year': [2015, 2016],
    ...                    'month': [2, 3],
    ...                    'day': [4, 5]})
    >>> pd.to_datetime(df)
    0   2015-02-04
    1   2016-03-05
    dtype: datetime64[ns]

    Passing ``infer_datetime_format=True`` can often-times speedup a parsing
    if it's not an ISO8601 format exactly, but in a regular format.

    >>> s = pd.Series(['3/11/2000', '3/12/2000', '3/13/2000'] * 1000)
    >>> s.head()
    0    3/11/2000
    1    3/12/2000
    2    3/13/2000
    3    3/11/2000
    4    3/12/2000
    dtype: object

    Using a unix epoch time

    >>> pd.to_datetime(1490195805, unit='s')
    Timestamp('2017-03-22 15:16:45')
    >>> pd.to_datetime(1490195805433502912, unit='ns')
    Timestamp('2017-03-22 15:16:45.433502912')

    .. warning:: For float arg, precision rounding might happen. To prevent
        unexpected behavior use a fixed-width exact type.

    Using a non-unix epoch origin

    >>> pd.to_datetime([1, 2, 3], unit='D',
    ...                origin=pd.Timestamp('1960-01-01'))
    DatetimeIndex(['1960-01-02', '1960-01-03', '1960-01-04'], dtype='datetime64[ns]', freq=None)


    **Non-convertible date/times**

    If a date does not meet the `timestamp limitations
    <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html
    #timeseries-timestamp-limits>`_, passing ``errors='ignore'``
    will return the original input instead of raising any exception.

    Passing ``errors='coerce'`` will force an out-of-bounds date to :const:`NaT`,
    in addition to forcing non-dates (or non-parseable dates) to :const:`NaT`.

    >>> pd.to_datetime(['13000101', 'abc'], format='%Y%m%d', errors='coerce')
    DatetimeIndex(['NaT', 'NaT'], dtype='datetime64[ns]', freq=None)


    .. _to_datetime_tz_examples:

    **Timezones and time offsets**

    The default behaviour (``utc=False``) is as follows:

    - Timezone-naive inputs are kept as timezone-naive :class:`~modin.pandas.DatetimeIndex`:

    >>> pd.to_datetime(['2018-10-26 12:00:00', '2018-10-26 13:00:15'])
    DatetimeIndex(['2018-10-26 12:00:00', '2018-10-26 13:00:15'], dtype='datetime64[ns]', freq=None)

    >>> pd.to_datetime(['2018-10-26 12:00:00 -0500', '2018-10-26 13:00:00 -0500'])
    DatetimeIndex(['2018-10-26 12:00:00-05:00', '2018-10-26 13:00:00-05:00'], dtype='datetime64[ns, UTC-05:00]', freq=None)

    - Use right format to convert to timezone-aware type (Note that when call Snowpark
      pandas API to_pandas() the timezone-aware output will always be converted to session timezone):

    >>> pd.to_datetime(['2018-10-26 12:00:00 -0500', '2018-10-26 13:00:00 -0500'], format="%Y-%m-%d %H:%M:%S %z")
    DatetimeIndex(['2018-10-26 12:00:00-05:00', '2018-10-26 13:00:00-05:00'], dtype='datetime64[ns, UTC-05:00]', freq=None)

    - Timezone-aware inputs *with mixed time offsets* (for example
      issued from a timezone with daylight savings, such as Europe/Paris):

    >>> pd.to_datetime(['2020-10-25 02:00:00 +0200', '2020-10-25 04:00:00 +0100'])
    DatetimeIndex([2020-10-25 02:00:00+02:00, 2020-10-25 04:00:00+01:00], dtype='object', freq=None)

    >>> pd.to_datetime(['2020-10-25 02:00:00 +0200', '2020-10-25 04:00:00 +0100'], format="%Y-%m-%d %H:%M:%S %z")
    DatetimeIndex([2020-10-25 02:00:00+02:00, 2020-10-25 04:00:00+01:00], dtype='object', freq=None)

    Setting ``utc=True`` makes sure always convert to timezone-aware outputs:

    - Timezone-naive inputs are *localized* based on the session timezone

    >>> pd.to_datetime(['2018-10-26 12:00', '2018-10-26 13:00'], utc=True)
    DatetimeIndex(['2018-10-26 12:00:00+00:00', '2018-10-26 13:00:00+00:00'], dtype='datetime64[ns, UTC]', freq=None)

    - Timezone-aware inputs are *converted* to session timezone

    >>> pd.to_datetime(['2018-10-26 12:00:00 -0530', '2018-10-26 12:00:00 -0500'],
    ...                utc=True)
    DatetimeIndex(['2018-10-26 17:30:00+00:00', '2018-10-26 17:00:00+00:00'], dtype='datetime64[ns, UTC]', freq=None)
    """


def to_timedelta():
    """
    Convert argument to timedelta.

    Timedeltas are absolute differences in times, expressed in difference
    units (e.g. days, hours, minutes, seconds). This method converts
    an argument from a recognized timedelta format / value into
    a Timedelta type.

    Parameters
    ----------
    arg : str, timedelta, list-like or Series
        The data to be converted to timedelta.
    unit : str, optional
        Denotes the unit of the arg for numeric `arg`. Defaults to ``"ns"``.

        Possible values:
        * 'W'
        * 'D' / 'days' / 'day'
        * 'hours' / 'hour' / 'hr' / 'h' / 'H'
        * 'm' / 'minute' / 'min' / 'minutes' / 'T'
        * 's' / 'seconds' / 'sec' / 'second' / 'S'
        * 'ms' / 'milliseconds' / 'millisecond' / 'milli' / 'millis' / 'L'
        * 'us' / 'microseconds' / 'microsecond' / 'micro' / 'micros' / 'U'
        * 'ns' / 'nanoseconds' / 'nano' / 'nanos' / 'nanosecond' / 'N'

        Must not be specified when `arg` contains strings and ``errors="raise"``.
    errors : {'ignore', 'raise', 'coerce'}, default 'raise'
        - If 'raise', then invalid parsing will raise an exception.
        - If 'coerce', then invalid parsing will be set as NaT.
        - If 'ignore', then invalid parsing will return the input.

    Returns
    -------
    timedelta
        If parsing succeeded.
        Return type depends on input:
        - list-like: TimedeltaIndex of timedelta64 dtype
        - Series: Series of timedelta64 dtype
        - scalar: Timedelta

    See Also
    --------
    DataFrame.astype : Cast argument to a specified dtype.
    to_datetime : Convert argument to datetime.
    convert_dtypes : Convert dtypes.

    Notes
    -----
    If the precision is higher than nanoseconds, the precision of the duration is
    truncated to nanoseconds for string inputs.

    Examples
    --------
    Parsing a single string to a Timedelta:

    >>> pd.to_timedelta('1 days 06:05:01.00003')
    Timedelta('1 days 06:05:01.000030')
    >>> pd.to_timedelta('15.5us')
    Timedelta('0 days 00:00:00.000015500')

    Parsing a list or array of strings:

    >>> pd.to_timedelta(['1 days 06:05:01.00003', '15.5us', 'nan'])
    TimedeltaIndex(['1 days 06:05:01.000030', '0 days 00:00:00.000015500', NaT], dtype='timedelta64[ns]', freq=None)

    Converting numbers by specifying the `unit` keyword argument:

    >>> pd.to_timedelta(np.arange(5), unit='s')
    TimedeltaIndex(['0 days 00:00:00', '0 days 00:00:01', '0 days 00:00:02',
                    '0 days 00:00:03', '0 days 00:00:04'],
                   dtype='timedelta64[ns]', freq=None)
    >>> pd.to_timedelta(np.arange(5), unit='d')
    TimedeltaIndex(['0 days', '1 days', '2 days', '3 days', '4 days'], dtype='timedelta64[ns]', freq=None)
    """


def date_range():
    """
    Return a fixed frequency DatetimeIndex.

    Returns the range of equally spaced time points (where the difference between any
    two adjacent points is specified by the given frequency) such that they all
    satisfy `start <[=] x <[=] end`, where the first one and the last one are, resp.,
    the first and last time points in that range that fall on the boundary of ``freq``
    (if given as a frequency string) or that are valid for ``freq`` (if given as a
    :class:`pandas.tseries.offsets.DateOffset`). (If exactly one of ``start``,
    ``end``, or ``freq`` is *not* specified, this missing parameter can be computed
    given ``periods``, the number of timesteps in the range. See the note below.)

    Parameters
    ----------
    start : str or datetime-like, optional
        Left bound for generating dates.
    end : str or datetime-like, optional
        Right bound for generating dates.
    periods : int, optional
        Number of periods to generate.
    freq : str or DateOffset, default 'D'
        Frequency strings can have multiples, e.g. '5H'.
    tz : str or tzinfo, optional
        Time zone name for returning localized DatetimeIndex, for example
        'Asia/Hong_Kong'. By default, the resulting DatetimeIndex is
        timezone-naive.
    normalize : bool, default False
        Normalize start/end dates to midnight before generating date range.
    name : str, default None
        Name of the resulting DatetimeIndex.
    inclusive : {"both", "neither", "left", "right"}, default "both"
        Include boundaries; Whether to set each bound as closed or open.
    **kwargs
        For compatibility. Has no effect on the result.

    Returns
    -------
    rng : DatetimeIndex

    See Also
    --------
    DatetimeIndex : An immutable container for datetimes.
    timedelta_range : Return a fixed frequency TimedeltaIndex.
    period_range : Return a fixed frequency PeriodIndex.
    interval_range : Return a fixed frequency IntervalIndex.

    Notes
    -----
    Of the four parameters ``start``, ``end``, ``periods``, and ``freq``,
    exactly three must be specified. If ``freq`` is omitted, the resulting
    ``DatetimeIndex`` will have ``periods`` linearly spaced elements between
    ``start`` and ``end`` (closed on both sides).

    To learn more about the frequency strings, please see `this link
    <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`__.

    Also, custom or business frequencies are not implemented in Snowpark pandas, e.g., "B", "C", "SMS", "BMS", "CBMS",
    "BQS", "BYS", "bh", "cbh".

    Examples
    --------
    **Specifying the values**

    The next four examples generate the same `DatetimeIndex`, but vary
    the combination of `start`, `end` and `periods`.

    Specify `start` and `end`, with the default daily frequency.

    >>> pd.date_range(start='1/1/2018', end='1/08/2018')
    DatetimeIndex(['2018-01-01', '2018-01-02', '2018-01-03', '2018-01-04',
                   '2018-01-05', '2018-01-06', '2018-01-07', '2018-01-08'],
                  dtype='datetime64[ns]', freq=None)

    Specify timezone-aware `start` and `end`, with the default daily frequency. Note that Snowflake use TIMESTAMP_TZ to
    represent datetime with timezone, and internally it stores UTC time together with an associated time zone offset
    (and the actual timezone is no longer available). More details can be found in
    https://docs.snowflake.com/en/sql-reference/data-types-datetime#timestamp-ltz-timestamp-ntz-timestamp-tz.

    >>> pd.date_range(
    ...     start=pd.to_datetime("1/1/2018").tz_localize("Europe/Berlin"),
    ...     end=pd.to_datetime("1/08/2018").tz_localize("Europe/Berlin"),
    ... )
    DatetimeIndex(['2018-01-01 00:00:00+01:00', '2018-01-02 00:00:00+01:00',
                   '2018-01-03 00:00:00+01:00', '2018-01-04 00:00:00+01:00',
                   '2018-01-05 00:00:00+01:00', '2018-01-06 00:00:00+01:00',
                   '2018-01-07 00:00:00+01:00', '2018-01-08 00:00:00+01:00'],
                  dtype='datetime64[ns, UTC+01:00]', freq=None)

    Specify `start` and `periods`, the number of periods (days).

    >>> pd.date_range(start='1/1/2018', periods=8)
    DatetimeIndex(['2018-01-01', '2018-01-02', '2018-01-03', '2018-01-04',
                   '2018-01-05', '2018-01-06', '2018-01-07', '2018-01-08'],
                  dtype='datetime64[ns]', freq=None)

    Specify `end` and `periods`, the number of periods (days).

    >>> pd.date_range(end='1/1/2018', periods=8)
    DatetimeIndex(['2017-12-25', '2017-12-26', '2017-12-27', '2017-12-28',
                   '2017-12-29', '2017-12-30', '2017-12-31', '2018-01-01'],
                  dtype='datetime64[ns]', freq=None)

    Specify `start`, `end`, and `periods`; the frequency is generated
    automatically (linearly spaced).

    >>> pd.date_range(start='2018-04-24', end='2018-04-27', periods=3)
    DatetimeIndex(['2018-04-24 00:00:00', '2018-04-25 12:00:00',
                   '2018-04-27 00:00:00'],
                  dtype='datetime64[ns]', freq=None)


    **Other Parameters**

    Changed the `freq` (frequency) to ``'ME'`` (month end frequency).

    >>> pd.date_range(start='1/1/2018', periods=5, freq='ME')
    DatetimeIndex(['2018-01-31', '2018-02-28', '2018-03-31', '2018-04-30',
                   '2018-05-31'],
                  dtype='datetime64[ns]', freq=None)

    Multiples are allowed

    >>> pd.date_range(start='1/1/2018', periods=5, freq='3ME')
    DatetimeIndex(['2018-01-31', '2018-04-30', '2018-07-31', '2018-10-31',
                   '2019-01-31'],
                  dtype='datetime64[ns]', freq=None)

    `freq` can also be specified as an Offset object.

    >>> pd.date_range(start='1/1/2018', periods=5, freq=pd.offsets.MonthEnd(3))
    DatetimeIndex(['2018-01-31', '2018-04-30', '2018-07-31', '2018-10-31',
                   '2019-01-31'],
                  dtype='datetime64[ns]', freq=None)

    Specify `tz` to set the timezone.

    >>> pd.date_range(start='1/1/2018', periods=5, tz='Asia/Tokyo')
    DatetimeIndex(['2018-01-01 00:00:00+09:00', '2018-01-02 00:00:00+09:00',
                   '2018-01-03 00:00:00+09:00', '2018-01-04 00:00:00+09:00',
                   '2018-01-05 00:00:00+09:00'],
                  dtype='datetime64[ns, UTC+09:00]', freq=None)

    `inclusive` controls whether to include `start` and `end` that are on the
    boundary. The default, "both", includes boundary points on either end.

    >>> pd.date_range(start='2017-01-01', end='2017-01-04', inclusive="both")
    DatetimeIndex(['2017-01-01', '2017-01-02', '2017-01-03', '2017-01-04'], dtype='datetime64[ns]', freq=None)

    Use ``inclusive='left'`` to exclude `end` if it falls on the boundary.

    >>> pd.date_range(start='2017-01-01', end='2017-01-04', inclusive='left')
    DatetimeIndex(['2017-01-01', '2017-01-02', '2017-01-03'], dtype='datetime64[ns]', freq=None)

    Use ``inclusive='right'`` to exclude `start` if it falls on the boundary, and
    similarly ``inclusive='neither'`` will exclude both `start` and `end`.

    >>> pd.date_range(start='2017-01-01', end='2017-01-04', inclusive='right')
    DatetimeIndex(['2017-01-02', '2017-01-03', '2017-01-04'], dtype='datetime64[ns]', freq=None)
    """


def bdate_range():
    """
    Return a fixed frequency DatetimeIndex with business day as the default.

    Parameters
    ----------
    start : str or datetime-like, default None
        Left bound for generating dates.
    end : str or datetime-like, default None
        Right bound for generating dates.
    periods : int, default None
        Number of periods to generate.
    freq : str, Timedelta, datetime.timedelta, or DateOffset, default 'B'
        Frequency strings can have multiples, e.g. '5h'. The default is
        business daily ('B').
    tz : str or None
        Time zone name for returning localized DatetimeIndex, for example
        Asia/Beijing.
    normalize : bool, default False
        Normalize start/end dates to midnight before generating date range.
    name : str, default None
        Name of the resulting DatetimeIndex.
    weekmask : str or None, default None
        Weekmask of valid business days, passed to ``numpy.busdaycalendar``,
        only used when custom frequency strings are passed.  The default
        value None is equivalent to 'Mon Tue Wed Thu Fri'.
    holidays : list-like or None, default None
        Dates to exclude from the set of valid business days, passed to
        ``numpy.busdaycalendar``, only used when custom frequency strings
        are passed.
    inclusive : {"both", "neither", "left", "right"}, default "both"
        Include boundaries; Whether to set each bound as closed or open.
    **kwargs
        For compatibility. Has no effect on the result.

    Returns
    -------
    DatetimeIndex

    Notes
    -----
    Of the four parameters: ``start``, ``end``, ``periods``, and ``freq``,
    exactly three must be specified.  Specifying ``freq`` is a requirement
    for ``bdate_range``.  Use ``date_range`` if specifying ``freq`` is not
    desired.

    To learn more about the frequency strings, please see `this link
    <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`__.

    Examples
    --------
    Note how the two weekend days are skipped in the result.

    >>> pd.bdate_range(start='1/1/2018', end='1/08/2018') # doctest: +NORMALIZE_WHITESPACE
    DatetimeIndex(['2018-01-01', '2018-01-02', '2018-01-03', '2018-01-04',
            '2018-01-05', '2018-01-08'],
            dtype='datetime64[ns]', freq=None)
    """


def value_counts():
    """
    Compute a histogram of the counts of non-null values.

    Parameters
    ----------
    values : ndarray (1-d)
        Values to perform computation.
    sort : bool, default: True
        Sort by values.
    ascending : bool, default: False
        Sort in ascending order.
    normalize : bool, default: False
        If True then compute a relative histogram.
    bins : integer, optional
        Rather than count values, group them into half-open bins,
        convenience for pd.cut, only works with numeric data.
    dropna : bool, default: True
        Don't include counts of NaN.

    Returns
    -------
    Series
    """
