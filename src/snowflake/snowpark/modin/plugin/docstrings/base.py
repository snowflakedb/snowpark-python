#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""This module contains BasePandasDataset docstrings that override modin's docstrings."""

from pandas.util._decorators import doc

from snowflake.snowpark.modin.plugin.docstrings.shared_docs import (
    _doc_binary_op,
    _shared_docs,
)

_doc_binary_op_kwargs = {"returns": "BasePandasDataset", "left": "BasePandasDataset"}


_shared_docs[
    "stat_func_example"
] = """

Examples
--------
>>> idx = pd.MultiIndex.from_arrays([
...     ['warm', 'warm', 'cold', 'cold'],
...     ['dog', 'falcon', 'fish', 'spider']],
...     names=['blooded', 'animal'])
>>> s = pd.Series([4, 2, 0, 8], name='legs', index=idx)
>>> s
blooded  animal
warm     dog       4
         falcon    2
cold     fish      0
         spider    8
Name: legs, dtype: int64

>>> s.{stat_func}()  # doctest: +SKIP
{default_output}"""

_max_examples: str = _shared_docs["stat_func_example"].format(
    stat_func="max", verb="Max", default_output=8, level_output_0=4, level_output_1=8
)

_min_examples: str = _shared_docs["stat_func_example"].format(
    stat_func="min", verb="Min", default_output=0, level_output_0=2, level_output_1=0
)

_mean_examples: str = _shared_docs["stat_func_example"].format(
    stat_func="mean",
    verb="Mean",
    default_output="3.5",
    level_output_0=2,
    level_output_1=0,
)

_median_examples: str = _shared_docs["stat_func_example"].format(
    stat_func="median",
    verb="Median",
    default_output="3.0",
    level_output_0=2,
    level_output_1=0,
)

_sum_examples = _shared_docs["stat_func_example"].format(
    stat_func="sum", verb="Sum", default_output=14, level_output_0=6, level_output_1=8
)

_sum_examples += """

By default, the sum of an empty or all-NA Series is ``0``.

>>> pd.Series([], dtype="float64").sum()  # min_count=0 is the default  # doctest: +SKIP
0.0

This can be controlled with the ``min_count`` parameter. For example, if
you'd like the sum of an empty series to be NaN, pass ``min_count=1``.

>>> pd.Series([], dtype="float64").sum(min_count=1)  # doctest: +SKIP
nan

Thanks to the ``skipna`` parameter, ``min_count`` handles all-NA and
empty series identically.

>>> pd.Series([np.nan]).sum()  # doctest: +SKIP
0.0

>>> pd.Series([np.nan]).sum(min_count=1)  # doctest: +SKIP
nan"""

_num_doc = """
{desc}

Parameters
----------
axis : {axis_descr}
    Axis for the function to be applied on.
    For `Series` this parameter is unused and defaults to 0.
skipna : bool, default True
    Exclude NA/null values when computing the result.
numeric_only : bool, default False
    If True, Include only float, int, boolean columns. Not implemented for Series.
**kwargs
    Additional keyword arguments to be passed to the function.

Returns
-------
{name1}\
{see_also}\
{examples}
"""

_num_min_count_doc = """
{desc}

Parameters
----------
axis : {axis_descr}
    Axis for the function to be applied on.
    For `Series` this parameter is unused and defaults to 0.
skipna : bool, default True
    Exclude NA/null values when computing the result.
numeric_only : bool, default False
    If True, Include only float, int, boolean columns. Not implemented for Series.
min_count : int, default {min_count}
    The required number of valid values to perform the operation. If fewer than min_count non-NA values
    are present the result will be NA.

**kwargs
    Additional keyword arguments to be passed to the function.

Returns
-------
{name1}\
{see_also}\
{examples}
"""

_num_ddof_doc = """
{desc}

Parameters
----------
axis : {axis_descr}
    For `Series` this parameter is unused and defaults to 0.
skipna : bool, default True
    Exclude NA/null values. If an entire row/column is NA, the result
    will be NA.
ddof : int, default 1
    Delta Degrees of Freedom. The divisor used in calculations is N - ddof,
    where N represents the number of elements.
numeric_only : bool, default False
    If True, Include only float, int, boolean columns. Not implemented for Series.

Returns
-------
{name1}\
{notes}\
{examples}
"""

_std_notes = """

Notes
-----
To have the same behaviour as `numpy.std`, use `ddof=0` (instead of the
default `ddof=1`)"""

_std_examples = """

Examples
--------
>>> df = pd.DataFrame({'person_id': [0, 1, 2, 3],
...                   'age': [21, 25, 62, 43],
...                   'height': [1.61, 1.87, 1.49, 2.01]}
...                  ).set_index('person_id')
>>> df    # doctest: +NORMALIZE_WHITESPACE
           age  height
person_id
0           21    1.61
1           25    1.87
2           62    1.49
3           43    2.01

The standard deviation of the columns can be found as follows:

>>> df.std()
age       18.786076
height     0.237417
dtype: float64

Alternatively, `ddof=0` can be set to normalize by N instead of N-1:

>>> df.std(ddof=0)
age       16.269219
height     0.205609
dtype: float64"""

_var_examples = """

Examples
--------
>>> df = pd.DataFrame({'person_id': [0, 1, 2, 3],
...                   'age': [21, 25, 62, 43],
...                   'height': [1.61, 1.87, 1.49, 2.01]}
...                  ).set_index('person_id')
>>> df    # doctest: +NORMALIZE_WHITESPACE
           age  height
person_id
0           21    1.61
1           25    1.87
2           62    1.49
3           43    2.01

>>> df.var()
age       352.916667
height      0.056367
dtype: float64

Alternatively, ``ddof=0`` can be set to normalize by N instead of N-1:

>>> df.var(ddof=0)
age       264.687500
height      0.042275
dtype: float64"""

_name1 = "Series"
_name2 = "DataFrame"
_axis_descr = "{index (0), columns (1)}"

# running doctests with numpy 2.0+ will use np.True_ instead of python builtin True, so
# we need to skip tests that produce a scalar
_bool_doc = """
{desc}

Parameters
----------
axis : {{0 or 'index', 1 or 'columns', None}}, default 0
    Indicate which axis or axes should be reduced. For `Series` this parameter
    is unused and defaults to 0.

    * 0 / 'index' : reduce the index, return a Series whose index is the
      original column labels.
    * 1 / 'columns' : reduce the columns, return a Series whose index is the
      original index.
    * None : reduce all axes, return a scalar.

bool_only : bool, default False
    Include only boolean columns. Not implemented for Series.
skipna : bool, default True
    Exclude NA/null values. If the entire row/column is NA and skipna is
    True, then the result will be {empty_value}, as for an empty row/column.
    If skipna is False, then NA are treated as True, because these are not
    equal to zero.
**kwargs : any, default None
    Additional keywords have no effect but might be accepted for
    compatibility with NumPy.

Returns
-------
{name1}

Notes
-----
* Snowpark pandas currently only supports this function on DataFrames/Series with integer or boolean
  columns.

{see_also}
{examples}"""

_all_examples = """\
Examples
--------
**Series**

>>> pd.Series([True, True]).all()  # doctest: +SKIP
True
>>> pd.Series([True, False]).all()  # doctest: +SKIP
False

**DataFrames**

Create a dataframe from a dictionary.

>>> df = pd.DataFrame({'col1': [True, True], 'col2': [True, False]})
>>> df
   col1   col2
0  True   True
1  True  False

Default behaviour checks if values in each column all return True.

>>> df.all()
col1     True
col2    False
dtype: bool

Specify ``axis='columns'`` to check if values in each row all return True.

>>> df.all(axis='columns')
0     True
1    False
dtype: bool

Or ``axis=None`` for whether every value is True.

>>> df.all(axis=None)  # doctest: +SKIP
False
"""

_all_see_also = """\
See Also
--------
Series.all : Return True if all elements are True.
DataFrame.any : Return True if one (or more) elements are True.
"""

_any_see_also = """\
See Also
--------
numpy.any : Numpy version of this method.
Series.any : Return whether any element is True.
Series.all : Return whether all elements are True.
DataFrame.any : Return whether any element is True over requested axis.
DataFrame.all : Return whether all elements are True over requested axis.
"""

_any_examples = """\
Examples
--------
**Series**

For Series input, the output is a scalar indicating whether any element
is True.

>>> pd.Series([False, False]).any()  # doctest: +SKIP
False
>>> pd.Series([True, False]).any()  # doctest: +SKIP
True

**DataFrame**

Whether each column contains at least one True element (the default).

>>> df = pd.DataFrame({"A": [1, 2], "B": [0, 2], "C": [0, 0]})
>>> df
   A  B  C
0  1  0  0
1  2  2  0

>>> df.any()
A     True
B     True
C    False
dtype: bool

Aggregating over the columns.

>>> df = pd.DataFrame({"A": [True, False], "B": [1, 2]})
>>> df
       A  B
0   True  1
1  False  2

>>> df.any(axis='columns')
0    True
1    True
dtype: bool

>>> df = pd.DataFrame({"A": [True, False], "B": [1, 0]})
>>> df
       A  B
0   True  1
1  False  0

>>> df.any(axis='columns')
0     True
1    False
dtype: bool

Aggregating over the entire DataFrame with ``axis=None``.

>>> df.any(axis=None)  # doctest: +SKIP
True

`any` for an empty DataFrame is an empty Series.

>>> pd.DataFrame([]).any()
Series([], dtype: bool)
"""

_get_set_index_doc = """
{desc}

{parameters_or_returns}

Note
----
When setting `DataFrame.index` or `Series.index` where the length of the
`Series`/`DataFrame` object does not match with the new index's length,
pandas raises a ValueError. Snowpark pandas does not raise this error;
this operation is valid.
When the `Series`/`DataFrame` object is longer than the new index,
the `Series`/`DataFrame`'s new index is filled with `NaN` values for
the "extra" elements. When the `Series`/`DataFrame` object is shorter than
the new index, the extra values in the new index are ignored—`Series` and
`DataFrame` stay the same length `n`, and use only the first `n` values of
the new index.
"""


class BasePandasDataset:
    """
    Implement most of the common code that exists in DataFrame/Series.

    Since both objects share the same underlying representation, and the algorithms
    are the same, we use this object to define the general behavior of those objects
    and then use those objects to define the output type.
    """

    def abs():
        """
        Return a `BasePandasDataset` with absolute numeric value of each element.
        """

    def set_axis():
        """
        Assign desired index to given axis.
        """

    def add():
        """
        Return addition of `BasePandasDataset` and `other`, element-wise (binary operator `add`).
        """

    def aggregate():
        """
        Aggregate using one or more operations over the specified axis.
        """

    agg = aggregate

    def align():
        """
        Align two objects on their axes with the specified join method.

        Join method is specified for each axis Index.

        Args:
            other: DataFrame or Series
            join: {‘outer’, ‘inner’, ‘left’, ‘right’}, default ‘outer’
                Type of alignment to be performed.
                left: use only keys from left frame, preserve key order.
                right: use only keys from right frame, preserve key order.
                outer: use union of keys from both frames, sort keys lexicographically.
            axis: allowed axis of the other object, default None
                Align on index (0), columns (1), or both (None).
            level: int or level name, default None
                Broadcast across a level, matching Index values on the passed MultiIndex level.
            copy: bool, default True
                Always returns new objects. If copy=False and no reindexing is required then original objects are returned.
            fill_value: scalar, default np.nan
                Always returns new objects. If copy=False and no reindexing is required then original objects are returned.

        Returns:
            tuple of (Series/DataFrame, type of other)

        Notes
        -----
        Snowpark pandas DataFrame/Series.align currently does not support `axis = 1 or None`, non-default `fill_value`,
        `copy`, `level`, and MultiIndex.

        Examples::

            >>> df = pd.DataFrame(
            ...     [[1, 2, 3, 4], [6, 7, 8, 9]], columns=["D", "B", "E", "A"], index=[1, 2]
            ... )
            >>> other = pd.DataFrame(
            ...     [[10, 20, 30, 40], [60, 70, 80, 90], [600, 700, 800, 900]],
            ...     columns=["A", "B", "C", "D"],
            ...     index=[2, 3, 4],
            ... )
            >>> df
               D  B  E  A
            1  1  2  3  4
            2  6  7  8  9
            >>> other
                 A    B    C    D
            2   10   20   30   40
            3   60   70   80   90
            4  600  700  800  900

            Align on columns:

            >>> left, right = df.align(other, join="outer", axis=1)
            >>> left
               A  B   C  D  E
            1  4  2 NaN  1  3
            2  9  7 NaN  6  8
            >>> right
                 A    B    C    D   E
            2   10   20   30   40 NaN
            3   60   70   80   90 NaN
            4  600  700  800  900 NaN

            We can also align on the index:

            >>> left, right = df.align(other, join="outer", axis=0)
            >>> left
                 D    B    E    A
            1  1.0  2.0  3.0  4.0
            2  6.0  7.0  8.0  9.0
            3  NaN  NaN  NaN  NaN
            4  NaN  NaN  NaN  NaN
            >>> right
                   A      B      C      D
            1    NaN    NaN    NaN    NaN
            2   10.0   20.0   30.0   40.0
            3   60.0   70.0   80.0   90.0
            4  600.0  700.0  800.0  900.0

            Finally, the default axis=None will align on both index and columns:

            >>> left, right = df.align(other, join="outer", axis=None)
            >>> left
                 A    B   C    D    E
            1  4.0  2.0 NaN  1.0  3.0
            2  9.0  7.0 NaN  6.0  8.0
            3  NaN  NaN NaN  NaN  NaN
            4  NaN  NaN NaN  NaN  NaN
            >>> right
                   A      B      C      D   E
            1    NaN    NaN    NaN    NaN NaN
            2   10.0   20.0   30.0   40.0 NaN
            3   60.0   70.0   80.0   90.0 NaN
            4  600.0  700.0  800.0  900.0 NaN
        """

    @doc(
        _bool_doc,
        desc="Return whether all elements are True, potentially over an axis.\n\n"
        "Returns True unless there at least one element within a series or "
        "along a Dataframe axis that is False or equivalent (e.g. zero or "
        "empty).",
        see_also=_all_see_also,
        examples=_all_examples,
        name1="Series",
        empty_value="True",
    )
    def all():
        pass

    @doc(
        _bool_doc,
        desc="Return whether any element are True, potentially over an axis.\n\n"
        "Returns False unless there at least one element within a series or "
        "along a Dataframe axis that is True or equivalent (e.g. non-zero or "
        "non-empty).",
        see_also=_any_see_also,
        examples=_any_examples,
        name1="Series",
        empty_value="False",
    )
    def any():
        pass

    def apply():
        """
        Apply a function along an axis of the `BasePandasDataset`.
        """

    def asfreq():
        """
        Convert time series to specified frequency.

        Returns the original data conformed to a new index with the specified frequency.

        If the index of this Series/DataFrame is a PeriodIndex, the new index is the result of transforming the original
        index with PeriodIndex.asfreq (so the original index will map one-to-one to the new index).

        The new index will be equivalent to pd.date_range(start, end, freq=freq) where start and end are,
        respectively, the first and last entries in the original index (see pandas.date_range()). The values
        corresponding to any timesteps in the new index which were not present in the original index will be null (NaN),
        unless a method for filling such unknowns is provided (see the method parameter below).

        The resample() method is more appropriate if an operation on each group of timesteps (such as an aggregate) is
        necessary to represent the data at the new frequency.

        Parameters
        ----------
        freq : DateOffset or str
            Frequency DateOffset or string.

        method : {'backfill', 'bfill', 'pad', 'ffill'}, default None
            Method to use for filling holes in reindexed Series (note this does not fill NaNs that already were present):
            ‘pad’ / ‘ffill’: propagate last valid observation forward to next valid
            ‘backfill’ / ‘bfill’: use NEXT valid observation to fill.

        how : {'start', 'end'}, default None
            For PeriodIndex only.

        normalize : bool, default False
            Whether to reset output index to midnight.

        fill_value : scalar, optional
            Value to use for missing values, applied during upsampling
            (note this does not fill NaNs that already were present).

        Returns
        -------
        Snowpark pandas :class:`~modin.pandas.DataFrame` or Snowpark pandas :class:`~modin.pandas.Series`

        Notes
        -----
        This implementation calls `resample` with the `first` aggregation. `asfreq`
        is only supported on DataFrame/Series with DatetimeIndex, and only
        the `freq` and `method` parameters are currently supported.

        Examples
        --------
        >>> index = pd.date_range('1/1/2000', periods=4, freq='min')
        >>> series = pd.Series([0.0, None, 2.0, 3.0], index=index)
        >>> df = pd.DataFrame({'s': series})
        >>> df
                               s
        2000-01-01 00:00:00  0.0
        2000-01-01 00:01:00  NaN
        2000-01-01 00:02:00  2.0
        2000-01-01 00:03:00  3.0
        >>> df.asfreq(freq='30s')
                               s
        2000-01-01 00:00:00  0.0
        2000-01-01 00:00:30  NaN
        2000-01-01 00:01:00  NaN
        2000-01-01 00:01:30  NaN
        2000-01-01 00:02:00  2.0
        2000-01-01 00:02:30  NaN
        2000-01-01 00:03:00  3.0
        >>> df.asfreq(freq='30s', method='ffill')
                               s
        2000-01-01 00:00:00  0.0
        2000-01-01 00:00:30  0.0
        2000-01-01 00:01:00  NaN
        2000-01-01 00:01:30  NaN
        2000-01-01 00:02:00  2.0
        2000-01-01 00:02:30  2.0
        2000-01-01 00:03:00  3.0
        """

    def asof():
        """
        Return the last row(s) without any NaNs before `where`.
        """

    def astype():
        """
        Cast a pandas object to a specified dtype ``dtype``.

        Parameters
        ----------
        dtype : str, data type, Series or Mapping of column name -> data type
            Use a str, numpy.dtype, pandas.ExtensionDtype or Python type to
            cast entire pandas object to the same type. Alternatively, use a
            mapping, e.g. {col: dtype, ...}, where col is a column label and dtype is
            a numpy.dtype or Python type to cast one or more of the DataFrame's
            columns to column-specific types.
        copy : bool, default True
            Return a copy (i.e., a new object) when ``copy=True``; otherwise, astype
            operates inplace  (be very careful setting ``copy=False`` as changes to
            values then may propagate to other pandas objects).
        errors : {'raise', 'ignore'}, default 'raise'
            Control raising of exceptions on invalid data for provided dtype.

            - ``raise`` : allow exceptions to be raised
            - ``ignore`` : suppress exceptions. On error return original object.

        Returns
        -------
        same type as caller (Snowpark pandas :class:`~modin.pandas.DataFrame` or Snowpark pandas :class:`~modin.pandas.Series`)

        Examples
        --------
        Create a DataFrame:

        >>> d = {'col1': [1, 2], 'col2': [3, 4]}
        >>> df = pd.DataFrame(data=d)
        >>> df.dtypes
        col1    int64
        col2    int64
        dtype: object

        Cast all columns to int32 (dtypes will be int64 since Snowpark pandas API will cast all integers to int64):

        >>> df.astype('int32').dtypes
        col1    int64
        col2    int64
        dtype: object

        Cast col1 to float64 using a dictionary:

        >>> df.astype({'col1': 'float64'}).dtypes
        col1    float64
        col2      int64
        dtype: object

        Create a series:

        >>> ser = pd.Series([1, 2], dtype=str)
        >>> ser
        0    1
        1    2
        dtype: object
        >>> ser.astype('float64')
        0    1.0
        1    2.0
        dtype: float64

        """

    @property
    def at():
        """
        Get a single value for a row/column label pair.
        """

    def at_time():
        """
        Select values at particular time of day (e.g., 9:30AM).
        """

    def between_time():
        """
        Select values between particular times of the day (e.g., 9:00-9:30 AM).

        By setting start_time to be later than end_time, you can get the times that are not between the two times.
        """

    def bfill():
        """
        Synonym for `DataFrame.fillna` with ``method='bfill'``.
        """

    backfill = bfill

    def bool():
        """
        Return the bool of a single element `BasePandasDataset`.
        """

    def clip():
        """
        Trim values at input threshold(s).
        """

    def combine():
        """
        Perform combination of `BasePandasDataset`-s according to `func`.
        """

    def combine_first():
        """
        Update null elements with value in the same location in `other`.
        """

    def copy():
        """
        Make a copy of this object's indices and data.

        When ``deep=True`` (default), a new object will be created with a
        copy of the calling object's data and indices. Modifications to
        the data or indices of the copy will not be reflected in the
        original object (see examples below). In Snowpark pandas API this will not copy the
        underlying table/data, but we create a logical copy of data and indices to
        provide same sematics as native pandas.

        When ``deep=False``, a new object will be created without copying the calling
        object's data or index (only references to the data and index are copied).
        Any changes to the data of the original will be reflected in the shallow copy
        (and vice versa).

        Parameters
        ----------
        deep : bool, default True
            Make a deep copy, including a locial copy of the data and the indices.
            With ``deep=False`` neither the indices nor the data are copied.

        Returns
        -------
        copy : Snowpark pandas :class:`~modin.pandas.Series` or Snowpark pandas :class:`~modin.pandas.DataFrame`
            Object type matches caller.

        Examples
        --------
        >>> s = pd.Series([1, 2], index=["a", "b"])
        >>> s
        a    1
        b    2
        dtype: int64

        >>> s_copy = s.copy()
        >>> s_copy
        a    1
        b    2
        dtype: int64

        **Shallow copy versus default (deep) copy:**

        >>> s = pd.Series([1, 2], index=["a", "b"])
        >>> deep = s.copy()
        >>> shallow = s.copy(deep=False)

        Updates to the data shared by shallow copy and original is reflected
        in both; deep copy remains unchanged.

        >>> s.sort_values(ascending=False, inplace=True)
        >>> shallow.sort_values(ascending=False, inplace=True)
        >>> s
        b    2
        a    1
        dtype: int64
        >>> shallow
        b    2
        a    1
        dtype: int64
        >>> deep
        a    1
        b    2
        dtype: int64
        """

    def count():
        """
        Count non-NA cells for each column or row.

        The values `None`, `NaN`, `NaT` are considered NA.

        Parameters
        ----------
        axis : {0 or 'index', 1 or 'columns'}, default 0
            If 0 or 'index' counts are generated for each column.
            If 1 or 'columns' counts are generated for each row. Not supported yet.
        numeric_only : bool, default False
            Include only `float`, `int` or `boolean` data.

        Returns
        -------
        Snowpark pandas :class:`~modin.pandas.Series`
            For each column/row the number of non-NA/null entries.

        See Also
        --------
        Series.count: Number of non-NA elements in a Series.
        DataFrame.value_counts: Count unique combinations of columns.
        DataFrame.shape: Number of DataFrame rows and columns (including NA
            elements).
        DataFrame.isna: Boolean same-sized DataFrame showing places of NA
            elements.

        Examples
        --------
        Constructing DataFrame from a dictionary:

        >>> df = pd.DataFrame({"Person":
        ...                    ["John", "Myla", "Lewis", "John", "Myla"],
        ...                    "Age": [24., np.nan, 21., 33, 26],
        ...                    "Single": [False, True, True, True, False]})
        >>> df   # doctest: +NORMALIZE_WHITESPACE
           Person   Age  Single
        0    John  24.0   False
        1    Myla   NaN    True
        2   Lewis  21.0    True
        3    John  33.0    True
        4    Myla  26.0   False

        Notice the uncounted NA values:

        >>> df.count()
        Person    5
        Age       4
        Single    5
        dtype: int64
        """

    def cummax():
        """
        Return cumulative maximum over a `BasePandasDataset` axis.

        Parameters
        --------
        axis : {0 or 'index', 1 or 'columns'}, default 0
            The index or the name of the axis. 0 is equivalent to None or 'index'. For Series this parameter is unused and defaults to 0.
        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.
        *args, **kwargs :
            Additional keywords have no effect but might be accepted for compatibility with NumPy.

        Returns
        -------
        Series or DataFrame
            Return cumulative maximum of Series or DataFrame.

        Examples
        --------
        Series

        >>> s = pd.Series([2, np.nan, 5, -1, 0])
        >>> s
        0    2.0
        1    NaN
        2    5.0
        3   -1.0
        4    0.0
        dtype: float64

        By default, NA values are ignored.

        >>> s.cummax()
        0    2.0
        1    NaN
        2    5.0
        3    5.0
        4    5.0
        dtype: float64

        To include NA values in the operation, use skipna=False:

        >>> s.cummax(skipna=False)
        0    2.0
        1    NaN
        2    NaN
        3    NaN
        4    NaN
        dtype: float64

        DataFrame

        >>> df = pd.DataFrame([[2.0, 1.0], [3.0, np.nan], [1.0, 0.0]], columns=list('AB'))
        >>> df
             A    B
        0  2.0  1.0
        1  3.0  NaN
        2  1.0  0.0

        By default, iterates over rows and finds the maximum in each column. This is equivalent to axis=None or axis='index'.

        >>> df.cummax()
             A    B
        0  2.0  1.0
        1  3.0  NaN
        2  3.0  1.0
        """

    def cummin():
        """
        Return cumulative minimum over a `BasePandasDataset` axis.

        Parameters
        --------
        axis : {0 or 'index', 1 or 'columns'}, default 0
            The index or the name of the axis. 0 is equivalent to None or 'index'. For Series this parameter is unused and defaults to 0.
        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.
        *args, **kwargs :
            Additional keywords have no effect but might be accepted for compatibility with NumPy.

        Returns
        -------
        Series or DataFrame
            Return cumulative minimum of Series or DataFrame.

        Examples
        --------
        Series

        >>> s = pd.Series([2, np.nan, 5, -1, 0])
        >>> s
        0    2.0
        1    NaN
        2    5.0
        3   -1.0
        4    0.0
        dtype: float64

        By default, NA values are ignored.

        >>> s.cummin()
        0    2.0
        1    NaN
        2    2.0
        3   -1.0
        4   -1.0
        dtype: float64

        To include NA values in the operation, use skipna=False:

        >>> s.cummin(skipna=False)
        0    2.0
        1    NaN
        2    NaN
        3    NaN
        4    NaN
        dtype: float64

        DataFrame

        >>> df = pd.DataFrame([[2.0, 1.0], [3.0, np.nan], [1.0, 0.0]], columns=list('AB'))
        >>> df
             A    B
        0  2.0  1.0
        1  3.0  NaN
        2  1.0  0.0

        By default, iterates over rows and finds the minimum in each column. This is equivalent to axis=None or axis='index'.

        >>> df.cummin()
             A    B
        0  2.0  1.0
        1  2.0  NaN
        2  1.0  0.0
        """

    def cumprod():
        """
        Return cumulative product over a `BasePandasDataset` axis.
        """

    def cumsum():
        """
        Return cumulative sum over a `BasePandasDataset` axis.

        Parameters
        --------
        axis : {0 or 'index', 1 or 'columns'}, default 0
            The index or the name of the axis. 0 is equivalent to None or 'index'. For Series this parameter is unused and defaults to 0.
        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.
        *args, **kwargs :
            Additional keywords have no effect but might be accepted for compatibility with NumPy.

        Returns
        -------
        Series or DataFrame
            Return cumulative sum of Series or DataFrame.

        Examples
        --------
        Series

        >>> s = pd.Series([2, np.nan, 5, -1, 0])
        >>> s
        0    2.0
        1    NaN
        2    5.0
        3   -1.0
        4    0.0
        dtype: float64

        By default, NA values are ignored.

        >>> s.cumsum()
        0    2.0
        1    NaN
        2    7.0
        3    6.0
        4    6.0
        dtype: float64

        To include NA values in the operation, use skipna=False:

        >>> s.cumsum(skipna=False)
        0    2.0
        1    NaN
        2    NaN
        3    NaN
        4    NaN
        dtype: float64

        DataFrame

        >>> df = pd.DataFrame([[2.0, 1.0], [3.0, np.nan], [1.0, 0.0]], columns=list('AB'))
        >>> df
             A    B
        0  2.0  1.0
        1  3.0  NaN
        2  1.0  0.0

        By default, iterates over rows and finds the sum in each column. This is equivalent to axis=None or axis='index'.

        >>> df.cumsum()
             A    B
        0  2.0  1.0
        1  5.0  NaN
        2  6.0  1.0
        """

    def describe():
        """
        Generate descriptive statistics for columns in the dataset.

        For non-numeric columns, computes `count` (# of non-null items), `unique` (# of unique items),
        `top` (the mode; the element at the lowest position if multiple), and `freq` (# of times the mode appears)
        for each column.

        For numeric columns, computes `count` (# of non-null items), `mean`, `std`, `min`,
        the specified percentiles, and `max` for each column.

        If both non-numeric and numeric columns are specified, the rows for statistics of
        non-numeric columns appear first in the output.

        Parameters
        ----------
        percentiles: Optional[ListLike], default None
            The percentiles to compute for numeric columns. If unspecified, defaults to [0.25, 0.5, 0.75],
            which returns the 25th, 50th, and 75th percentiles. All values should fall between 0 and 1.
            The median (0.5) will always be added to the displayed percentile if not already included;
            the min and max are always displayed in addition to the percentiles.
        include: Optional[List[str, ExtensionDtype | np.dtype]] | "all", default None
            A list of dtypes to include in the result (ignored for Series).

            * "all": Include all columns in the output.
            * list-like: Include only columns of the listed dtypes. To limit the result to numeric
              types submit `numpy.number`. To limit it instead to object columns submit the
              `numpy.object` data type. Strings can also be used in the style of `select_dtypes`
              (e.g. `df.describe(include=['O'])`).
            * None: If the dataframe has at least one numeric column, then include only numeric
              columns; otherwise include all columns in the output.

        exclude: Optional[List[str, ExtensionDtype | np.dtype]], default None
            A list of dtypes to omit from the result (ignored for Series).

            * list-like: Exclude all columns of the listed dtypes. To exclude numeric types submit
              `numpy.number`. To exclude object columns submit the data type `numpy.object`. Strings
              can also be used in the style of `select_dtypes` (e.g. `df.describe(exclude=['O'])`).
            * None: Exclude nothing.

        Returns
        -------
        BasePandasDataset
            Snowpark DataFrame if this was a DataFrame, and Snowpark Series if this was a Series.
            Each column contains statistics for the corresponding column in the input dataset.

        Examples
        --------
        Describing a frame with both numeric and object columns:

        >>> df = pd.DataFrame({'numeric': [1, 2, 3],
        ...                    'object': ['a', 'b', 'c']
        ...                   })
        >>> df.describe(include='all') # doctest: +NORMALIZE_WHITESPACE
                numeric object
        count       3.0      3
        unique      NaN      3
        top         NaN      a
        freq        NaN      1
        mean        2.0   None
        std         1.0   None
        min         1.0   None
        25%         1.5   None
        50%         2.0   None
        75%         2.5   None
        max         3.0   None

        Describing only numeric columns:

        >>> pd.DataFrame({'numeric': [1, 2, 3], 'object': ['a', 'b', 'c']}).describe(include='number') # doctest: +NORMALIZE_WHITESPACE
               numeric
        count      3.0
        mean       2.0
        std        1.0
        min        1.0
        25%        1.5
        50%        2.0
        75%        2.5
        max        3.0

        Excluding numeric columns:

        >>> pd.DataFrame({'numeric': [1, 2, 3], 'object': ['a', 'b', 'c']}).describe(exclude='number') # doctest: +NORMALIZE_WHITESPACE
               object
        count       3
        unique      3
        top         a
        freq        1
        """

    def diff():
        """
        First discrete difference of element.
        """

    def drop():
        """
        Drop specified labels from `BasePandasDataset`.
        """

    def droplevel():
        """
        Return `BasePandasDataset` with requested index / column level(s) removed.
        """

    def drop_duplicates():
        """
        Return `BasePandasDataset` with duplicate rows removed.
        """

    def mask():
        """
        Replace values where the condition is True.
        """

    def where():
        """
        Replace values where the condition is False.
        """

    def eq():
        """
        Get equality of `BasePandasDataset` and `other`, element-wise (binary operator `eq`).
        """

    def explode():
        """
        Transform each element of a list-like to a row.
        """

    def ewm():
        """
        Provide exponentially weighted (EW) calculations.
        """

    def expanding():
        """
        Provide expanding window calculations.
        Currently, ``axis = 1`` is not supported.

        Parameters
        ----------
        min_periods: int, default 1.
            Minimum number of observations in window required to have a value; otherwise, result is np.nan.
        axis: int or str, default 0
            If 0 or 'index', roll across the rows.
            If 1 or 'columns', roll across the columns.
            For Series this parameter is unused and defaults to 0.
        method: str {‘single’, ‘table’}, default ‘single’
            **This parameter is ignored in Snowpark pandas since the execution engine will always be Snowflake.**
        """

    def ffill():
        """
        Synonym for :meth:`DataFrame.fillna` with ``method='ffill'``.
        """

    pad = ffill

    def fillna():
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

    def filter():
        """
        Subset the `BasePandasDataset` rows or columns according to the specified index labels.
        """

    def first():
        """
        Select initial periods of time series data based on a date offset.
        """

    def first_valid_index():
        """
        Return index for first non-NA value or None, if no non-NA value is found.

        Returns
        -------
        scalar or None, Tuple of scalars if MultiIndex

        Examples
        --------
        >>> s = pd.Series([None, 3, 4])
        >>> s.first_valid_index()
        1
        >>> s = pd.Series([None, None])
        >>> s.first_valid_index()
        >>> df = pd.DataFrame({'A': [None, 1, 2, None], 'B': [3, 2, 1, None]}, index=[10, 11, 12, 13])
        >>> df
              A    B
        10  NaN  3.0
        11  1.0  2.0
        12  2.0  1.0
        13  NaN  NaN
        >>> df.first_valid_index()
        10
        >>> df = pd.DataFrame([5, 6, 7, 8], index=["i", "am", "iron", "man"])
        >>> df.first_valid_index()
        'i'
        """

    @property
    def flags():
        pass

    def floordiv():
        """
        Get integer division of `BasePandasDataset` and `other`, element-wise (binary operator `floordiv`).
        """

    def ge():
        """
        Get greater than or equal comparison of `BasePandasDataset` and `other`, element-wise (binary operator `ge`).
        """

    def get():
        """
        Get item from object for given key (ex: DataFrame column).

        Returns default value if not found.

        Parameters
        ----------
        key : object

        Returns
        -------
        same type as items contained in object

        Examples
        --------
        >>> df = pd.DataFrame(
        ...     [
        ...         [24.3, 75.7, "high"],
        ...         [31, 87.8, "high"],
        ...         [22, 71.6, "medium"],
        ...         [35, 95, "medium"],
        ...     ],
        ...     columns=["temp_celsius", "temp_fahrenheit", "windspeed"],
        ...     index=pd.date_range(start="2014-02-12", end="2014-02-15", freq="D"),
        ... )

        >>> df
                    temp_celsius  temp_fahrenheit windspeed
        2014-02-12          24.3             75.7      high
        2014-02-13          31.0             87.8      high
        2014-02-14          22.0             71.6    medium
        2014-02-15          35.0             95.0    medium

        >>> df.get(["temp_celsius", "windspeed"])
                    temp_celsius windspeed
        2014-02-12          24.3      high
        2014-02-13          31.0      high
        2014-02-14          22.0    medium
        2014-02-15          35.0    medium

        >>> ser = df['windspeed']
        >>> ser.get('2014-02-13')
        2014-02-13    high
        Freq: None, Name: windspeed, dtype: object

        >>> ser.get('2014-02-10', '[unknown]')
        Series([], Freq: None, Name: windspeed, dtype: object)

        Notes:
            Generally Snowpark pandas won't raise KeyError or IndexError if any key is not found.
            So the result of `get` will be a result with existing keys or an empty result if no key is found.
            Default value won't be used.
        """

    def gt():
        """
        Get greater than comparison of `BasePandasDataset` and `other`, element-wise (binary operator `gt`).
        """

    def head():
        """
        Return the first `n` rows.
        """

    @property
    def iat():
        """
        Get a single value for a row/column pair by integer position.
        """

    def idxmax():
        """
        Return index of first occurrence of maximum over requested axis.

        Parameters
        ----------
        axis : {0 or 1}, default 0
            The axis to use. 0 for row-wise, 1 for column-wise.
        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.
        numeric_only: bool, default False:
            Include only float, int or boolean data.

        Returns
        -------
        Series if DataFrame input, Index if Series input

        Examples
        --------
        >>> df = pd.DataFrame({'consumption': [10.51, 103.11, 55.48],
        ...                     'co2_emissions': [37.2, 19.66, 1712]},
        ...                   index=['Pork', 'Wheat Products', 'Beef'])
        >>> df
                        consumption  co2_emissions
        Pork                  10.51          37.20
        Wheat Products       103.11          19.66
        Beef                  55.48        1712.00
        >>> df.idxmax()
        consumption      Wheat Products
        co2_emissions              Beef
        dtype: object
        >>> df.idxmax(axis=1)
        Pork              co2_emissions
        Wheat Products      consumption
        Beef              co2_emissions
        dtype: object
        >>> s = pd.Series(data=[1, None, 4, 3, 4],
        ...               index=['A', 'B', 'C', 'D', 'E'])
        >>> s.idxmax()
        'C'
        >>> s.idxmax(skipna=False)  # doctest: +SKIP
        nan
        """

    def idxmin():
        """
        Return index of first occurrence of minimum over requested axis.

        Parameters
        ----------
        axis : {0 or 1}, default 0
            The axis to use. 0 for row-wise, 1 for column-wise.
        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.
        numeric_only: bool, default False:
            Include only float, int or boolean data.

        Returns
        -------
        Series if DataFrame input, Index if Series input

        Examples
        --------
        >>> df = pd.DataFrame({'consumption': [10.51, 103.11, 55.48],
        ...                     'co2_emissions': [37.2, 19.66, 1712]},
        ...                   index=['Pork', 'Wheat Products', 'Beef'])
        >>> df
                        consumption  co2_emissions
        Pork                  10.51          37.20
        Wheat Products       103.11          19.66
        Beef                  55.48        1712.00
        >>> df.idxmin()
        consumption                Pork
        co2_emissions    Wheat Products
        dtype: object
        >>> df.idxmin(axis=1)
        Pork                consumption
        Wheat Products    co2_emissions
        Beef                consumption
        dtype: object
        >>> s = pd.Series(data=[1, None, 4, 3, 4],
        ...               index=['A', 'B', 'C', 'D', 'E'])
        >>> s.idxmin()
        'A'
        >>> s.idxmin(skipna=False)  # doctest: +SKIP
        nan
        """

    def infer_objects():
        """
        Attempt to infer better dtypes for object columns. This is not currently supported
        in Snowpark pandas.
        """

    def interpolate():
        """
        Fill NaN values using an interpolation method.

        Snowpark pandas only supports ``method='linear'``, ``'pad'``/``'ffill'``, and
        ``'bfill'``/``'backfill'``. See below for additional restrictions on other parameters.

        Only ``method='linear'`` is supported for DataFrame/Series with a MultiIndex.

        Parameters
        ----------
        method : str, default 'linear'
            Interpolation technique to use. One of:

            * 'linear': Ignore the index and treat the values as equally
              spaced. This is the only method supported on MultiIndexes.
            * 'pad': Fill in NaNs using existing values.

        axis : {{0 or 'index', 1 or 'columns', None}}, default None
            Axis to interpolate along. For `Series` this parameter is unused
            and defaults to 0. Snowpark pandas only supports ``axis=0``.
        limit : int, optional
            Maximum number of consecutive NaNs to fill. Unsupported by Snowpark pandas.
        inplace : bool, default False
            Update the data in place if possible.
        limit_direction : {{'forward', 'backward', 'both'}}, Optional
            Consecutive NaNs will be filled in this direction.

            If limit is specified:
                * If 'method' is 'pad' or 'ffill', 'limit_direction' must be 'forward'.
                * If 'method' is 'backfill' or 'bfill', 'limit_direction' must be
                  'backwards'.

            If 'limit' is not specified:
                * If 'method' is 'backfill' or 'bfill', the default is 'backward'
                * else the default is 'forward'

            raises ValueError if `limit_direction` is 'forward' or 'both' and
                method is 'backfill' or 'bfill'.
            raises ValueError if `limit_direction` is 'backward' or 'both' and
                method is 'pad' or 'ffill'.

        limit_area : {{`None`, 'inside', 'outside'}}, default None
            If limit is specified, consecutive NaNs will be filled with this
            restriction.

            * ``None``: No fill restriction.
            * 'inside': Only fill NaNs surrounded by valid values
              (interpolate).
            * 'outside': Only fill NaNs outside valid values (extrapolate).

            Snowpark pandas does not support 'outside'. Snowpark pandas only supports
            'inside' if ``method='linear'``.

        downcast : optional, 'infer' or None, defaults to None
            Downcast dtypes if possible. Unsupported by Snowpark pandas.

        ``**kwargs`` : optional
            Keyword arguments to pass on to the interpolating function. Ignored by Snowpark pandas.

        Returns
        -------
        Series or DataFrame or None
            Returns the same object type as the caller, interpolated at
            some or all ``NaN`` values or None if ``inplace=True``.

        See Also
        --------
        fillna : Fill missing values using different methods.

        Examples
        --------
        Filling in ``NaN`` in a :class:`~pandas.Series` via linear
        interpolation.

        >>> s = pd.Series([0, 1, np.nan, 3])
        >>> s
        0    0.0
        1    1.0
        2    NaN
        3    3.0
        dtype: float64
        >>> s.interpolate()  # doctest: +SKIP
        0    0.0
        1    1.0
        2    2.0
        3    3.0
        dtype: float64

        Fill the DataFrame forward (that is, going down) along each column
        using linear interpolation.

        Note how the last entry in column 'a' is interpolated differently,
        because there is no entry after it to use for interpolation.
        Note how the first entry in column 'b' remains ``NaN``, because there
        is no entry before it to use for interpolation.

        >>> df = pd.DataFrame([(0.0, np.nan, -1.0, 1.0),
        ...                    (np.nan, 2.0, np.nan, np.nan),
        ...                    (2.0, 3.0, np.nan, 9.0),
        ...                    (np.nan, 4.0, -4.0, 16.0)],
        ...                   columns=list('abcd'))
        >>> df
             a    b    c     d
        0  0.0  NaN -1.0   1.0
        1  NaN  2.0  NaN   NaN
        2  2.0  3.0  NaN   9.0
        3  NaN  4.0 -4.0  16.0
        >>> df.interpolate(method='linear', limit_direction='forward', axis=0)  # doctest: +SKIP
             a    b    c     d
        0  0.0  NaN -1.0   1.0
        1  1.0  2.0 -2.0   5.0
        2  2.0  3.0 -3.0   9.0
        3  2.0  4.0 -4.0  16.0
        """

    def convert_dtypes():
        """
        Convert columns to best possible dtypes using dtypes supporting ``pd.NA``.

        This is not supported in Snowpark pandas because Snowpark pandas always uses nullable
        data types internally. Calling this method will raise a `NotImplementedError`.
        """

    def isin():
        """
        Whether elements in `BasePandasDataset` are contained in `values`.
        """

    @property
    def iloc():
        """
        Purely integer-location based indexing for selection by position.

        ``.iloc[]`` is primarily integer position based (from ``0`` to
        ``length-1`` of the axis), but may also be used with a boolean
        array.

        Allowed inputs are:

        - An integer, e.g. ``5``, ``-1``.
        - A list or array of integers, e.g. ``[4, 3, 0]``.
        - A slice object with ints, e.g. ``1:7``.
        - A boolean array.
        - A ``callable`` function with one argument (the calling Series or
          DataFrame) and that returns valid output for indexing (one of the above).
          This is useful in method chains, when you don't have a reference to the
          calling object, but would like to base your selection on some value.
        - A tuple of row and column indexes. The tuple elements consist of one of the
          above inputs, e.g. ``(0, 1)``.

        Notes
        -----
        To meet the nature of lazy evaluation:

        - Snowpark pandas ``.iloc`` ignores out-of-bounds indexing for all types of indexers (while pandas ``.iloc``
          will raise error except *slice* indexer). If all values are out-of-bound, an empty result will be returned.
        - In Snowpark pandas ``.iloc``, the length of boolean list-like indexers (e.g., list, Series, Index, numpy
          ndarray) does not need to be the same length as the row/column being indexed. Internally a join is going to
          be performed so missing or out of bound values will be ignored.

        See Also
        --------
        DataFrame.iat : Fast integer location scalar accessor.
        DataFrame.loc : Purely label-location based indexer for selection by label.
        Series.iloc : Purely integer-location based indexing for
                       selection by position.

        Examples
        --------
        >>> mydict = [{'a': 1, 'b': 2, 'c': 3, 'd': 4},
        ...           {'a': 100, 'b': 200, 'c': 300, 'd': 400},
        ...           {'a': 1000, 'b': 2000, 'c': 3000, 'd': 4000}]
        >>> df = pd.DataFrame(mydict)
        >>> df
              a     b     c     d
        0     1     2     3     4
        1   100   200   300   400
        2  1000  2000  3000  4000

        **Indexing just the rows**

        With a scalar integer.

        >>> type(df.iloc[0])
        <class 'modin.pandas.series.Series'>
        >>> df.iloc[0]
        a    1
        b    2
        c    3
        d    4
        Name: 0, dtype: int64

        >>> df.iloc[-1]
        a    1000
        b    2000
        c    3000
        d    4000
        Name: 2, dtype: int64

        With a list of integers.

        >>> df.iloc[[0]]
           a  b  c  d
        0  1  2  3  4

        >>> df.iloc[[0, 1]]
             a    b    c    d
        0    1    2    3    4
        1  100  200  300  400

        With out-of-bound values in the list and those out of bound values will be ignored.

        >>> df.iloc[[0, 1, 10, 11]]
             a    b    c    d
        0    1    2    3    4
        1  100  200  300  400

        With all out-of-bound values. Return empty dataset.

        >>> df.iloc[[10, 11, 12]]
        Empty DataFrame
        Columns: [a, b, c, d]
        Index: []

        With a `slice` object.

        >>> df.iloc[:3]
              a     b     c     d
        0     1     2     3     4
        1   100   200   300   400
        2  1000  2000  3000  4000

        With a boolean mask the same length as the index.

        >>> df.iloc[[True, False, True]]
              a     b     c     d
        0     1     2     3     4
        2  1000  2000  3000  4000

        When a boolean mask shorter than the index.

        >>> df.iloc[[True, False]]      # doctest: +NORMALIZE_WHITESPACE
              a     b     c     d
        0     1     2     3     4

        When a boolean mask longer than the index.

        >>> df.iloc[[True, False, True, True, True]]      # doctest: +NORMALIZE_WHITESPACE
              a     b     c     d
        0     1     2     3     4
        2  1000  2000  3000  4000

        With a callable, useful in method chains. The `x` passed
        to the ``lambda`` is the DataFrame being sliced. This selects
        the rows whose index labels are even.

        >>> df.iloc[lambda x: x.index % 2 == 0]
              a     b     c     d
        0     1     2     3     4
        2  1000  2000  3000  4000

        **Indexing both axes**

        You can mix the indexer types for the index and columns. Use ``:`` to
        select the entire axis.

        With scalar integers.

        >>> df.iloc[0, 1]  # doctest: +SKIP
        2

        With lists of integers.

        >>> df.iloc[[0, 2], [1, 3]]
              b     d
        0     2     4
        2  2000  4000

        With `slice` objects.

        >>> df.iloc[1:3, 0:3]
              a     b     c
        1   100   200   300
        2  1000  2000  3000

        With a boolean array whose length matches the columns.

        >>> df.iloc[:, [True, False, True, False]]
              a     c
        0     1     3
        1   100   300
        2  1000  3000

        With a callable function that expects the Series or DataFrame.

        >>> df.iloc[:, lambda df: [0, 2]]
              a     c
        0     1     3
        1   100   300
        2  1000  3000
        """

    def kurt():
        """
        Return unbiased kurtosis over requested axis.

        Kurtosis obtained using Fisher's definition of
        kurtosis (kurtosis of normal == 0.0). Normalized by N-1.
        """

    def last():
        """
        Select final periods of time series data based on a date offset.
        """

    def last_valid_index():
        """
        Return index for last non-NA value or None, if no non-NA value is found.

        Returns
        -------
        scalar or None, Tuple of scalars if MultiIndex

        Examples
        --------
        >>> s = pd.Series([None, 3, 4])
        >>> s.last_valid_index()
        2
        >>> s = pd.Series([None, None])
        >>> s.last_valid_index()
        >>> df = pd.DataFrame({'A': [None, 1, 2, None], 'B': [3, 2, 1, None]}, index=[10, 11, 12, 13])
        >>> df
              A    B
        10  NaN  3.0
        11  1.0  2.0
        12  2.0  1.0
        13  NaN  NaN
        >>> df.last_valid_index()
        12
        >>> df = pd.DataFrame([5, 6, 7, 8], index=["i", "am", "iron", "man"])
        >>> df.last_valid_index()
        'man'
        """

    def le():
        """
        Get less than or equal comparison of `BasePandasDataset` and `other`, element-wise (binary operator `le`).
        """

    def lt():
        """
        Get less than comparison of `BasePandasDataset` and `other`, element-wise (binary operator `lt`).
        """

    @property
    def loc():
        """
        Access a group of rows and columns by label(s) or a boolean array.

        ``.loc[]`` is primarily label based, but may also be used with a
        boolean array.

        Allowed inputs are:

        - A single label, e.g. ``5`` or ``'a'``, (note that ``5`` is
          interpreted as a *label* of the index, and **never** as an
          integer position along the index).
        - A list or array of labels, e.g. ``['a', 'b', 'c']``.
        - A slice object with labels, e.g. ``'a':'f'``.

          .. warning:: Note that contrary to usual python slices, **both** the
              start and the stop are included

        - A boolean array of the same length as the axis being sliced,
          e.g. ``[True, False, True]``.
        - An alignable boolean Series. The index of the key will be aligned before
          masking.
        - An alignable Index. The Index of the returned selection will be the input.
        - A ``callable`` function with one argument (the calling Series or
          DataFrame) and that returns valid output for indexing (one of the above)

        Notes
        -----
        To meet the nature of lazy evaluation:

        - Snowpark pandas ``.loc`` ignores out-of-bounds indexing for row indexers (while pandas ``.loc``
          may raise KeyError). If all values are out-of-bound, an empty result will be returned.
        - Out-of-bounds indexing for columns will still raise a KeyError the same way pandas does.
        - In Snowpark pandas ``.loc``, unalignable boolean Series provided as indexer will perform a join on the index
          of the main dataframe or series. (while pandas will raise an IndexingError)
        - When there is a slice key, Snowpark pandas ``.loc`` performs the same as native pandas when both the start and
          stop are labels present in the index or either one is absent but the index is sorted. When any of the two
          labels is absent from an unsorted index, Snowpark pandas will return rows in between while native pandas will
          raise a KeyError.
        - Special indexing for DatetimeIndex is unsupported in Snowpark pandas, e.g., `partial string indexing <https://pandas.pydata.org/docs/user_guide/timeseries.html#partial-string-indexing>`_.
        - While setting rows with duplicated index, Snowpark pandas won't raise ValueError for duplicate labels to avoid
          eager evaluation.
        - When using ``.loc`` to set values with a Series key and Series item, the index of the item is ignored, and values are set positionally.
        - pandas ``.loc`` may sometimes raise a ValueError when using ``.loc`` to set values in a DataFrame from a Series using a Series as the
          column key, but Snowpark pandas ``.loc`` supports this type of operation according to the rules specified above.
        - ``.loc`` with boolean indexers for columns is currently unsupported.
        - When using ``.loc`` to set column values for a Series item, with a ``slice(None)`` for the row columns, Snowpark pandas
          sets the value for each row from the Series.

        See Also
        --------
        DataFrame.at : Access a single value for a row/column label pair.
        DataFrame.iloc : Access group of rows and columns by integer position(s).
        DataFrame.xs : Returns a cross-section (row(s) or column(s)) from the
            Series/DataFrame.
        Series.loc : Access group of values using labels.

        Examples
        --------
        **Getting values**

        >>> df = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
        ...      index=['cobra', 'viper', 'sidewinder'],
        ...      columns=['max_speed', 'shield'])
        >>> df
                    max_speed  shield
        cobra               1       2
        viper               4       5
        sidewinder          7       8

        Single label. Note this returns the row as a Series.

        >>> df.loc['viper']
        max_speed    4
        shield       5
        Name: viper, dtype: int64

        List of labels. Note using ``[[]]`` returns a DataFrame.

        >>> df.loc[['viper', 'sidewinder']]
                    max_speed  shield
        viper               4       5
        sidewinder          7       8

        Single label for row and column

        >>> df.loc['cobra', 'shield']  # doctest: +SKIP
        2

        Slice with labels for row and single label for column. As mentioned
        above, note that both the start and stop of the slice are included.

        >>> df.loc['cobra':'viper', 'max_speed']
        cobra    1
        viper    4
        Name: max_speed, dtype: int64

        Boolean list with the same length as the row axis

        >>> df.loc[[False, False, True]]
                    max_speed  shield
        sidewinder          7       8

        Alignable boolean Series:

        >>> df.loc[pd.Series([False, True, False],
        ...        index=['viper', 'sidewinder', 'cobra'])]
                    max_speed  shield
        sidewinder          7       8

        Index (same behavior as ``df.reindex``)

        >>> df.loc[pd.Index(["cobra", "viper"], name="foo")]  # doctest: +SKIP
               max_speed  shield
        foo
        cobra          1       2
        viper          4       5

        Conditional that returns a boolean Series

        >>> df.loc[df['shield'] > 6]
                    max_speed  shield
        sidewinder          7       8

        Conditional that returns a boolean Series with column labels specified

        >>> df.loc[df['shield'] > 6, ['max_speed']]
                    max_speed
        sidewinder          7

        Callable that returns a boolean Series

        >>> df.loc[lambda df: df['shield'] == 8]
                    max_speed  shield
        sidewinder          7       8

        **Setting values**

        Set value for all items matching the list of labels

        >>> df.loc[['viper', 'sidewinder'], ['shield']] = 50
        >>> df
                    max_speed  shield
        cobra               1       2
        viper               4      50
        sidewinder          7      50

        Set value for an entire row

        >>> df.loc['cobra'] = 10
        >>> df
                    max_speed  shield
        cobra              10      10
        viper               4      50
        sidewinder          7      50

        Set value for an entire column

        >>> df.loc[:, 'max_speed'] = 30
        >>> df
                    max_speed  shield
        cobra              30      10
        viper              30      50
        sidewinder         30      50

        Set value for rows matching callable condition

        >>> df.loc[df['shield'] > 35] = 0
        >>> df
                    max_speed  shield
        cobra              30      10
        viper               0       0
        sidewinder          0       0

        Setting the values with a Series item.

        >>> df.loc["viper"] = pd.Series([99, 99], index=["max_speed", "shield"])
        >>> df
                    max_speed  shield
        cobra              30      10
        viper              99      99
        sidewinder          0       0

        **Getting values on a DataFrame with an index that has integer labels**

        Another example using integers for the index

        >>> df = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
        ...      index=[7, 8, 9], columns=['max_speed', 'shield'])
        >>> df
           max_speed  shield
        7          1       2
        8          4       5
        9          7       8

        Slice with integer labels for rows. As mentioned above, note that both
        the start and stop of the slice are included.

        >>> df.loc[7:9]
           max_speed  shield
        7          1       2
        8          4       5
        9          7       8

        **Getting values with a MultiIndex**

        A number of examples using a DataFrame with a MultiIndex

        >>> tuples = [
        ...    ('cobra', 'mark i'), ('cobra', 'mark ii'),
        ...    ('sidewinder', 'mark i'), ('sidewinder', 'mark ii'),
        ...    ('viper', 'mark ii'), ('viper', 'mark iii')
        ... ]
        >>> index = pd.MultiIndex.from_tuples(tuples)
        >>> values = [[12, 2], [0, 4], [10, 20],
        ...         [1, 4], [7, 1], [16, 36]]
        >>> df = pd.DataFrame(values, columns=['max_speed', 'shield'], index=index)
        >>> df
                             max_speed  shield
        cobra      mark i           12       2
                   mark ii           0       4
        sidewinder mark i           10      20
                   mark ii           1       4
        viper      mark ii           7       1
                   mark iii         16      36

        Single label. Note this returns a DataFrame with a single index.

        >>> df.loc['cobra']
                 max_speed  shield
        mark i          12       2
        mark ii          0       4

        Single index tuple. Note this returns a Series.

        >>> df.loc[('cobra', 'mark ii')]
        max_speed    0
        shield       4
        Name: ('cobra', 'mark ii'), dtype: int64

        Single label for row and column. Similar to passing in a tuple, this
        returns a Series.

        >>> df.loc['cobra', 'mark i']
        max_speed    12
        shield        2
        Name: ('cobra', 'mark i'), dtype: int64

        Single tuple. Note using ``[[]]`` returns a DataFrame.

        >>> df.loc[[('cobra', 'mark ii')]]
                       max_speed  shield
        cobra mark ii          0       4

        Single tuple for the index with a single label for the column

        >>> df.loc[('cobra', 'mark i'), 'shield']  # doctest: +SKIP
        2

        Slice from index tuple to single label

        >>> df.loc[('cobra', 'mark i'):'viper']
                             max_speed  shield
        cobra      mark i           12       2
                   mark ii           0       4
        sidewinder mark i           10      20
                   mark ii           1       4
        viper      mark ii           7       1
                   mark iii         16      36

        Slice from index tuple to index tuple

        >>> df.loc[('cobra', 'mark i'):('viper', 'mark ii')]
                            max_speed  shield
        cobra      mark i          12       2
                   mark ii          0       4
        sidewinder mark i          10      20
                   mark ii          1       4
        viper      mark ii          7       1

        Set column values from Series with Series key.

        >>> df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=list("ABC"))
        >>> df.loc[:, pd.Series(list("ABC"))] = pd.Series([-10, -20, -30])
        >>> df
            A   B   C
        0 -10 -20 -30
        1 -10 -20 -30
        >>> df.loc[:, pd.Series(list("ABC"))] = pd.Series([10, 20, 30], index=list("CBA"))
        >>> df
            A   B   C
        0  10  20  30
        1  10  20  30
        >>> df.loc[:, pd.Series(list("BAC"))] = pd.Series([-10, -20, -30], index=list("ABC"))
        >>> df
            A   B   C
        0 -20 -10 -30
        1 -20 -10 -30

        Set column values from Series with list key.

        >>> df.loc[:, list("ABC")] = pd.Series([1, 3, 5], index=list("CAB"))
        >>> df
           A  B  C
        0  3  5  1
        1  3  5  1

        Set column values for all rows from Series item.

        >>> df.loc[:, "A":"B"] = pd.Series([10, 20, 30], index=list("ABC"))
        >>> df
            A   B  C
        0  10  20  1
        1  10  20  1
        """

    @doc(
        _num_doc,
        desc="Return the maximum of the values over the requested axis.\n\n"
        "If you want the *index* of the maximum, use ``idxmax``. This is "
        "the equivalent of the ``numpy.ndarray`` method ``argmax``.",
        axis_descr=_axis_descr,
        name1=_name1,
        see_also="",
        examples=_max_examples,
    )
    def max():
        pass

    def memory_usage():
        """
        Return the memory usage of the `BasePandasDataset`.
        """

    @doc(
        _num_doc,
        desc="Return the minimum of the values over the requested axis.\n\n"
        "If you want the *index* of the minimum, use ``idxmin``. This is "
        "the equivalent of the ``numpy.ndarray`` method ``argmin``.",
        name1=_name1,
        axis_descr=_axis_descr,
        see_also="",
        examples=_min_examples,
    )
    def min():
        pass

    def mod():
        """
        Get modulo of `BasePandasDataset` and `other`, element-wise (binary operator `mod`).
        """

    def mode():
        """
        Get the mode(s) of each element along the selected axis.
        """

    def mul():
        """
        Get multiplication of `BasePandasDataset` and `other`, element-wise (binary operator `mul`).
        """

    multiply = mul

    def ne():
        """
        Get Not equal comparison of `BasePandasDataset` and `other`, element-wise (binary operator `ne`).
        """

    def notna():
        """
        Detect non-missing values for an array-like object.

        This function takes a scalar or array-like object and indicates whether values are valid (not missing, which
        is NaN in numeric arrays, None or NaN in object arrays, NaT in datetimelike).

        Parameters
        ----------
        obj : array-like or object value
            Object to check for not null or non-missing values.

        Returns
        -------
            bool or array-like of bool
            For scalar input, returns a scalar boolean. For array input, returns an array of boolean indicating whether
            each corresponding element is valid.

        Example
        -------
        >>> df = pd.DataFrame([['ant', 'bee', 'cat'], ['dog', None, 'fly']])
        >>> df
             0     1    2
        0  ant   bee  cat
        1  dog  None  fly
        >>> df.notna()
              0      1     2
        0  True   True  True
        1  True  False  True
        >>> df.notnull()
              0      1     2
        0  True   True  True
        1  True  False  True
        """

    notnull = notna

    def nunique():
        """
        Count number of distinct elements in specified axis.

        Return Series with number of distinct elements. Can ignore NaN values.
        Snowpark pandas API does not distinguish between different NaN types like None,
        pd.NA, and np.nan, and treats them as the same.

        Parameters
        ----------
        axis : {0 or 'index', 1 or 'columns'}, default 0
            The axis to use. 0 or 'index' for row-wise, 1 or 'columns' for
            column-wise. Snowpark pandas currently only supports axis=0.
        dropna : bool, default True
            Don't include NaN in the counts.

        Returns
        -------
        Series

        Examples
        --------
        >>> df = pd.DataFrame({'A': [4, 5, 6], 'B': [4, 1, 1]})
        >>> df.nunique()
        A    3
        B    2
        dtype: int64

        >>> df = pd.DataFrame({'A': [None, pd.NA, None], 'B': [1, 2, 1]})
        >>> df.nunique()
        A    0
        B    2
        dtype: int64

        >>> df.nunique(dropna=False)
        A    1
        B    2
        dtype: int64
        """

    def pct_change():
        """
        Fractional change between the current and a prior element.

        Computes the fractional change from the immediately previous row by
        default. This is useful in comparing the fraction of change in a time
        series of elements.

        .. note::

            Despite the name of this method, it calculates fractional change
            (also known as per unit change or relative change) and not
            percentage change. If you need the percentage change, multiply
            these values by 100.

        Parameters
        ----------
        periods : int, default 1
            Periods to shift for forming percent change.

        fill_method : {'backfill', 'bfill', 'pad', 'ffill', None}, default 'pad'
            How to handle NAs **before** computing percent changes.

            All options of `fill_method` are deprecated except `fill_method=None`.

        limit : int, default None
            The number of consecutive NAs to fill before stopping.

            Snowpark pandas does not yet support this parameter.

            Deprecated parameter.

        freq : DateOffset, timedelta, or str, optional
            Increment to use from time series API (e.g. 'ME' or BDay()).

            Snowpark pandas does not yet support this parameter.

        **kwargs
            Additional keyword arguments are passed into
            `DataFrame.shift` or `Series.shift`.

            Unlike pandas, Snowpark pandas does not use `shift` under the hood, and
            thus may not yet support the passed keyword arguments.

        Returns
        -------
        Series or DataFrame
            The same type as the calling object.

        See Also
        --------
        Series.diff : Compute the difference of two elements in a Series.
        DataFrame.diff : Compute the difference of two elements in a DataFrame.
        Series.shift : Shift the index by some number of periods.
        DataFrame.shift : Shift the index by some number of periods.

        Examples
        --------
        **Series**

        >>> s = pd.Series([90, 91, 85])
        >>> s
        0    90
        1    91
        2    85
        dtype: int64

        >>> s.pct_change()
        0         NaN
        1    0.011111
        2   -0.065934
        dtype: float64

        >>> s.pct_change(periods=2)
        0         NaN
        1         NaN
        2   -0.055556
        dtype: float64

        See the percentage change in a Series where filling NAs with last
        valid observation forward to next valid.

        >>> s = pd.Series([90, 91, None, 85])
        >>> s
        0    90.0
        1    91.0
        2     NaN
        3    85.0
        dtype: float64

        >>> s.ffill().pct_change()
        0         NaN
        1    0.011111
        2    0.000000
        3   -0.065934
        dtype: float64

        **DataFrame**

        Percentage change in French franc, Deutsche Mark, and Italian lira from
        1980-01-01 to 1980-03-01.

        >>> df = pd.DataFrame({
        ...     'FR': [4.0405, 4.0963, 4.3149],
        ...     'GR': [1.7246, 1.7482, 1.8519],
        ...     'IT': [804.74, 810.01, 860.13]},
        ...     index=['1980-01-01', '1980-02-01', '1980-03-01'])
        >>> df
                        FR      GR      IT
        1980-01-01  4.0405  1.7246  804.74
        1980-02-01  4.0963  1.7482  810.01
        1980-03-01  4.3149  1.8519  860.13

        >>> df.pct_change()
                          FR        GR        IT
        1980-01-01       NaN       NaN       NaN
        1980-02-01  0.013810  0.013684  0.006549
        1980-03-01  0.053365  0.059318  0.061876

        Percentage of change in GOOG and APPL stock volume. Shows computing
        the percentage change between columns.

        >>> df = pd.DataFrame({
        ...     '2016': [1769950, 30586265],
        ...     '2015': [1500923, 40912316],
        ...     '2014': [1371819, 41403351]},
        ...     index=['GOOG', 'APPL'])
        >>> df
                  2016      2015      2014
        GOOG   1769950   1500923   1371819
        APPL  30586265  40912316  41403351

        >>> df.pct_change(axis='columns', periods=-1)
                  2016      2015  2014
        GOOG  0.179241  0.094112   NaN
        APPL -0.252395 -0.011860   NaN
        """

    def pipe():
        """
        Apply chainable functions that expect `BasePandasDataset`.
        """

    def pop():
        """
        Return item and drop from frame. Raise KeyError if not found.

        Parameters
        ----------
            item : label
                Label of column to be popped.

        Returns
        -------
            Series

        Examples
        --------
        >>> df = pd.DataFrame([('falcon', 'bird', 389.0),
        ...                    ('parrot', 'bird', 24.0),
        ...                    ('lion', 'mammal', 80.5),
        ...                    ('monkey', 'mammal', np.nan)],
        ...                   columns=('name', 'class', 'max_speed'))
        >>> df
             name   class  max_speed
        0  falcon    bird      389.0
        1  parrot    bird       24.0
        2    lion  mammal       80.5
        3  monkey  mammal        NaN

        >>> df.pop('class')
        0      bird
        1      bird
        2    mammal
        3    mammal
        Name: class, dtype: object

        >>> df
             name  max_speed
        0  falcon      389.0
        1  parrot       24.0
        2    lion       80.5
        3  monkey        NaN
        """

    def pow():
        """
        Get exponential power of `BasePandasDataset` and `other`, element-wise (binary operator `pow`).
        """

    def quantile():
        """
        Return values at the given quantile over requested axis.

        Parameters
        ----------
        q: float or array-like of float, default 0.5
            Value(s) between 0 <= q <= 1, the quantile(s) to compute.
        axis: {0 or 'index', 1 or 'columns'}, default 0
            Axis across which to compute quantiles.
        numeric_only: bool, default False
            Include only data where `is_numeric_dtype` is true.
            When True, bool columns are included, but attempting to compute quantiles across
            bool values is an ill-defined error in both pandas and Snowpark pandas.
        interpolation: {"linear", "lower", "higher", "midpoint", "nearest"}, default "linear"
            Specifies the interpolation method to use if a quantile lies between two data points
            *i* and *j*:

            * linear: *i* + (*j* - *i*) * *fraction*, where *fraction* is the fractional part of the
              index surrounded by *i* and *j*.
            * lower: *i*.
            * higher: *j*.
            * nearest: *i* or *j*, whichever is nearest.
            * midpoint: (*i* + *j*) / 2.
        method: {"single", "table"}, default "single"
            Whether to compute quantiles per-column ("single") or over all columns ("table").
            When "table", the only allowed interpolation methods are "nearest", "lower", and "higher".

        Returns
        -------
        float, Series, or DataFrame
            If ``q`` is an array:
            - If this is a DataFrame, a DataFrame will be returned where the index is ``q``, the columns
            are the columns of ``self``, and the values are the quantiles.
            - If this is a Series, a Series will be returned where the index is ``q`` and the values
            are the quantiles.

            If ``q`` is a float:
            - If this is a DataFrame, a Series will be returned where the index is the columns of
            ``self`` and the values are the quantiles.
            - If this is a Series, the float value of that quantile will be returned.
        """

    def rank():
        """
        Compute numerical data ranks (1 through n) along axis.

        Parameters
        ----------
        axis: {0 or 'index', 1 or 'columns'}, default 0
            Index to direct ranking. For Series this parameter is unused and defaults to 0.
        method: {'average', 'min', 'max', 'first', 'dense'}, default 'average'
            How to rank the group of records that have the same value (i.e. break ties):
            - average: average rank of the group
            - min: lowest rank in the group
            - max: highest rank in the group
            - first: ranks assigned in order they appear in the array
            - dense: like 'min', but rank always increases by 1 between groups.
        numeric_only: bool, default False
            For DataFrame objects, rank only numeric columns if set to True.
        na_option: {'keep', 'top', 'bottom'}, default 'keep'
            How to rank NaN values:
            - keep: assign NaN rank to NaN values
            - top: assign lowest rank to NaN values
            - bottom: assign highest rank to NaN values
        ascending: bool, default True
            Whether or not the elements should be ranked in ascending order.
        pct: bool, default False
            Whether or not to display the returned rankings in percentile form.

        Returns
        -------
        Series or DataFrame with data ranks as values.
        """

    def reindex():
        """
        Conform `BasePandasDataset` to new index with optional filling logic.
        """

    def rename_axis():
        """
        Set the name of the axis for the index or columns.

        Parameters
        ----------
        mapper : scalar, list-like, optional
            Value to set the axis name attribute.

        index, columns : scalar, list-like, dict-like or function, optional
            A scalar, list-like, dict-like or functions transformations to apply to that axis' values.

            Use either ``mapper`` and ``axis`` to specify the axis to target with ``mapper``, or
            ``index`` and/or ``columns``.

        axis : {0 or 'index', 1 or 'columns'}, default 0
            The axis to rename.

        copy : bool, default None
            Also copy underlying data. This parameter is ignored in Snowpark pandas.

        inplace : bool, default False
            Modifies the object directly, instead of creating a new DataFrame.

        Returns
        -------
        DataFrame or None
            DataFrame, or None if ``inplace=True``.
        """

    def reorder_levels():
        """
        Rearrange index levels using input order.
        """

    def resample():
        """
        Resample time-series data. Convenience method for frequency conversion and resampling of time series.
        The object must have a datetime-like index.

        Snowpark pandas DataFrame/Series.resample only supports frequencies "second", "minute", "hour", and "day" in
        conjunction with aggregations "max", "min", "mean", "median", "sum", "std", "var", "count", and "ffill".
        Snowpark pandas also only supports DatetimeIndex, and does not support PeriodIndex or TimedeltaIndex.

        Parameters
        ----------
        rule : DateOffset, Timedelta or str
            The offset string or object representing target conversion.
            Snowpark pandas only supports frequencies "second", "minute", "hour", and "day"
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Which axis to use for up- or down-sampling. For Series this parameter is unused and defaults to 0.
            Snowpark pandas only supports ``axis`` 0 and DatetimeIndex.

            Deprecated: Use frame.T.resample(…) instead.
        closed : {'right', 'left'}, default None
            Which side of bin interval is closed. The default is 'left' for all frequency offsets except for
            'ME', 'YE', 'QE', 'BME', 'BA', 'BQE', and 'W' which all have a default of 'right'.

            Snowpark pandas only supports ``closed=left`` and frequencies "second", "minute", "hour", and "day".
        label : {'right', 'left'}, default None
            Which bin edge label to label bucket with. The default is 'left' for all frequency offsets except for
            'ME', 'YE', 'QE', 'BME', 'BA', 'BQE', and 'W' which all have a default of 'right'.

            Snowpark pandas only supports ``label=left`` and frequencies "second", "minute", "hour", and "day".
        convention : {'start', 'end', 's', 'e'}, default 'start'
            For PeriodIndex only, controls whether to use the start or end of rule.
            Snowpark pandas does not support PeriodIndex.

            Deprecated: Convert PeriodIndex to DatetimeIndex before resampling instead.
        kind : {'timestamp', 'period'}, optional, default None
            Pass 'timestamp' to convert the resulting index to a DateTimeIndex
            or 'period' to convert it to a PeriodIndex. By default, the input representation is retained.

            Snowpark pandas does not support ``kind``.
        on : str, optional
            For a DataFrame, column to use instead of index for resampling. Column must be datetime-like.
            Snowpark pandas does not support ``on``.
        level : str or int, optional
            For a MultiIndex, level (name or number) to use for resampling. level must be datetime-like.
            Snowpark pandas does not support DataFrame/Series.resample with a MultiIndex.
        origin : Timestamp or str, default 'start_day'
            The timestamp on which to adjust the grouping. The timezone of origin must match the timezone of the index.
            If a string, must be one of the following:

            'epoch': origin is 1970-01-01
            'start': origin is the first value of the timeseries
            'start_day': origin is the first day at midnight of the timeseries
            'end': origin is the last value of the timeseries
            'end_day': origin is the ceiling midnight of the last day

            Snowpark pandas does not support ``origin``.
        offset : Timedelta or str, default is None
            An offset timedelta added to the origin.
            Snowpark pandas does not support ``offset``.
        group_keys : bool, default False
            Whether to include the group keys in the result index when using ``.apply()`` on the resampled object.
            Snowpark pandas does not support ``group_keys``.

        Returns
        -------
        Resampler

        See Also
        --------
        Series.resample: Resample a Series.
        DataFrame.resample : Resample a dataframe.
        groupby: Group Series/DataFrame by mapping, function, label, or list of labels.
        asfreq: Reindex a Series/DataFrame with the given frequency without grouping.

        Notes
        -----
        Snowpark pandas DataFrame/Series.resample only supports frequencies "second", "minute", "hour", and "day" in conjunction
        with aggregations "max", "min", "mean", "median", "sum", "std", "var", "count", and "ffill". Snowpark pandas also only
        supports DatetimeIndex, and does not support PeriodIndex or TimedeltaIndex.

        Examples
        --------
        >>> index = pd.date_range('1/1/2000', periods=9, freq='min')
        >>> series = pd.Series(range(9), index=index)
        >>> series
        2000-01-01 00:00:00    0
        2000-01-01 00:01:00    1
        2000-01-01 00:02:00    2
        2000-01-01 00:03:00    3
        2000-01-01 00:04:00    4
        2000-01-01 00:05:00    5
        2000-01-01 00:06:00    6
        2000-01-01 00:07:00    7
        2000-01-01 00:08:00    8
        Freq: None, dtype: int64
        >>> series.resample('3min').sum()
        2000-01-01 00:00:00     3
        2000-01-01 00:03:00    12
        2000-01-01 00:06:00    21
        Freq: None, dtype: int64
        """

    def reset_index():
        """
        Reset the index, or a level of it.

        Reset the index of the DataFrame, and use the default one instead.
        If the DataFrame has a MultiIndex, this method can remove one or more
        levels.

        Parameters
        ----------
        level : int, str, tuple, or list, default None
            Only remove the given levels from the index. Removes all levels by
            default.
        drop : bool, default False
            Do not try to insert index into dataframe columns. This resets
            the index to the default integer index.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
        col_level : int or str, default 0
            If the columns have multiple levels, determines which level the
            labels are inserted into. By default, it is inserted into the first
            level.
        col_fill : object, default ''
            If the columns have multiple levels, determines how the other
            levels are named. If None then the index name is repeated.
        allow_duplicates : bool, optional, default lib.no_default
            Allow duplicate column labels to be created.
        names : int, str or 1-dimensional list, default None
            Using the given string, rename the DataFrame column which contains the
            index data. If the DataFrame has a MultiIndex, this has to be a list or
            tuple with length equal to the number of levels.

        Returns
        -------
        DataFrame or None
            DataFrame with the new index or None if ``inplace=True``.

        See Also
        --------
        Series.reset_index: Analogous function for Series.
        DataFrame.set_index : Opposite of reset_index.
        DataFrame.reindex : Change to new indices or expand indices.
        DataFrame.reindex_like : Change to same indices as other DataFrame.

        Examples
        --------
        >>> df = pd.DataFrame([('bird', 389.0),
        ...                    ('bird', 24.0),
        ...                    ('mammal', 80.5),
        ...                    ('mammal', np.nan)],
        ...                   index=['falcon', 'parrot', 'lion', 'monkey'],
        ...                   columns=('class', 'max_speed'))
        >>> df
                 class  max_speed
        falcon    bird      389.0
        parrot    bird       24.0
        lion    mammal       80.5
        monkey  mammal        NaN

        When we reset the index, the old index is added as a column, and a
        new sequential index is used:

        >>> df.reset_index()
            index   class  max_speed
        0  falcon    bird      389.0
        1  parrot    bird       24.0
        2    lion  mammal       80.5
        3  monkey  mammal        NaN

        We can use the `drop` parameter to avoid the old index being added as
        a column:

        >>> df.reset_index(drop=True)
            class  max_speed
        0    bird      389.0
        1    bird       24.0
        2  mammal       80.5
        3  mammal        NaN

        You can also use `reset_index` with `MultiIndex`.

        >>> index = pd.MultiIndex.from_tuples([('bird', 'falcon'),
        ...                                    ('bird', 'parrot'),
        ...                                    ('mammal', 'lion'),
        ...                                    ('mammal', 'monkey')],
        ...                                   names=['class', 'name'])
        >>> columns = pd.MultiIndex.from_tuples([('speed', 'max'),
        ...                                      ('species', 'type')])
        >>> df = pd.DataFrame([(389.0, 'fly'),
        ...                    ( 24.0, 'fly'),
        ...                    ( 80.5, 'run'),
        ...                    (np.nan, 'jump')],
        ...                   index=index,
        ...                   columns=columns)
        >>> df # doctest: +NORMALIZE_WHITESPACE
                       speed species
                         max    type
        class  name
        bird   falcon  389.0     fly
               parrot   24.0     fly
        mammal lion     80.5     run
               monkey    NaN    jump

        Using the `names` parameter, choose a name for the index column:

        >>> df.reset_index(names=['classes', 'names']) # doctest: +NORMALIZE_WHITESPACE
          classes   names  speed species
                             max    type
        0    bird  falcon  389.0     fly
        1    bird  parrot   24.0     fly
        2  mammal    lion   80.5     run
        3  mammal  monkey    NaN    jump

        If the index has multiple levels, we can reset a subset of them:

        >>> df.reset_index(level='class') # doctest: +NORMALIZE_WHITESPACE
                 class  speed species
                          max    type
        name
        falcon    bird  389.0     fly
        parrot    bird   24.0     fly
        lion    mammal   80.5     run
        monkey  mammal    NaN    jump

        If we are not dropping the index, by default, it is placed in the top
        level. We can place it in another level:

        >>> df.reset_index(level='class', col_level=1) # doctest: +NORMALIZE_WHITESPACE
                        speed species
                 class    max    type
        name
        falcon    bird  389.0     fly
        parrot    bird   24.0     fly
        lion    mammal   80.5     run
        monkey  mammal    NaN    jump

        When the index is inserted under another level, we can specify under
        which one with the parameter `col_fill`:

        >>> df.reset_index(level='class', col_level=1, col_fill='species') # doctest: +NORMALIZE_WHITESPACE
               species  speed species
                 class    max    type
        name
        falcon    bird  389.0     fly
        parrot    bird   24.0     fly
        lion    mammal   80.5     run
        monkey  mammal    NaN    jump

        If we specify a nonexistent level for `col_fill`, it is created:

        >>> df.reset_index(level='class', col_level=1, col_fill='genus') # doctest: +NORMALIZE_WHITESPACE
                 genus  speed species
                 class    max    type
        name
        falcon    bird  389.0     fly
        parrot    bird   24.0     fly
        lion    mammal   80.5     run
        monkey  mammal    NaN    jump
        """

    def radd():
        """
        Return addition of `BasePandasDataset` and `other`, element-wise (binary operator `radd`).
        """

    def rfloordiv():
        """
        Get integer division of `BasePandasDataset` and `other`, element-wise (binary operator `rfloordiv`).
        """

    def rmod():
        """
        Get modulo of `BasePandasDataset` and `other`, element-wise (binary operator `rmod`).
        """

    def rmul():
        """
        Get Multiplication of dataframe and other, element-wise (binary operator `rmul`).
        """

    def rolling():
        """
        Provide rolling window calculations.
        Currently, support is only provided for integer ``window``, ``axis = 0``, and ``min_periods = 1``.

        Parameters
        ----------
        window: int, timedelta, str, offset, or BaseIndexer subclass. Size of the moving window.
            If an integer, the fixed number of observations used for each window.
            If a timedelta, str, or offset, the time period of each window. Each window will be a variable sized based on the observations included in the time-period. This is only valid for datetimelike indexes.
            If a BaseIndexer subclass, the window boundaries based on the defined get_window_bounds method. Additional rolling keyword arguments, namely min_periods, center, closed and step will be passed to get_window_bounds.
        min_periods: int, default None.
            Minimum number of observations in window required to have a value; otherwise, result is np.nan.
            For a window that is specified by an offset, min_periods will default to 1.
            For a window that is specified by an integer, min_periods will default to the size of the window.
        center: bool, default False.
            If False, set the window labels as the right edge of the window index.
            If True, set the window labels as the center of the window index.
        win_type: str, default None
            If None, all points are evenly weighted.
            If a string, it must be a valid scipy.signal window function.
            Certain Scipy window types require additional parameters to be passed in the aggregation function. The additional parameters must match the keywords specified in the Scipy window type method signature.
        on: str, optional
            For a DataFrame, a column label or Index level on which to calculate the rolling window, rather than the DataFrame’s index.
            Provided integer column is ignored and excluded from result since an integer index is not used to calculate the rolling window.
        axis: int or str, default 0
            If 0 or 'index', roll across the rows.
            If 1 or 'columns', roll across the columns.
            For Series this parameter is unused and defaults to 0.
        closed: str, default None
            If 'right', the first point in the window is excluded from calculations.
            If 'left', the last point in the window is excluded from calculations.
            If 'both', the no points in the window are excluded from calculations.
            If 'neither', the first and last points in the window are excluded from calculations.
            Default None ('right').
        step: int, default None
            Evaluate the window at every step result, equivalent to slicing as [::step]. window must be an integer. Using a step argument other than None or 1 will produce a result with a different shape than the input.
        method: str {‘single’, ‘table’}, default ‘single’
            **This parameter is ignored in Snowpark pandas since the execution engine will always be Snowflake.**
        """

    def round():
        """
        Round a `BasePandasDataset` to a variable number of decimal places.
        """

    def rpow():
        """
        Get exponential power of `BasePandasDataset` and `other`, element-wise (binary operator `rpow`).
        """

    def rsub():
        """
        Get subtraction of `BasePandasDataset` and `other`, element-wise (binary operator `rsub`).
        """

    def rtruediv():
        """
        Get floating division of `BasePandasDataset` and `other`, element-wise (binary operator `rtruediv`).
        """

    rdiv = rtruediv

    def sample():
        """
        Return a random sample of items from an axis of object.

        You can use `random_state` for reproducibility.

        Parameters
        ----------
        n : int, optional
            Number of items from axis to return. Cannot be used with `frac`.
            Default = 1 if `frac` = None.
        frac : float, optional
            Fraction of axis items to return. Cannot be used with `n`.
        replace : bool, default False
            Allow or disallow sampling of the same row more than once.
        weights : str or ndarray-like, optional
            Default 'None' results in equal probability weighting.
            If passed a Series, will align with target object on index. Index
            values in weights not found in sampled object will be ignored and
            index values in sampled object not in weights will be assigned
            weights of zero.
            If called on a DataFrame, will accept the name of a column
            when axis = 0.
            Unless weights are a Series, weights must be same length as axis
            being sampled.
            If weights do not sum to 1, they will be normalized to sum to 1.
            Missing values in the weights column will be treated as zero.
            Infinite values not allowed.
        random_state : int, array-like, BitGenerator, np.random.RandomState, np.random.Generator, optional
            If int, array-like, or BitGenerator, seed for random number generator.
            If np.random.RandomState or np.random.Generator, use as given.
        axis : {0 or ‘index’, 1 or ‘columns’, None}, default None
            Axis to sample. Accepts axis number or name. Default is stat axis
            for given data type. For `Series` this parameter is unused and defaults to `None`.
        ignore_index : bool, default False
            If True, the resulting index will be labeled 0, 1, …, n - 1.

        Returns
        -------
        Series or DataFrame
            A new object of same type as caller containing `n` items randomly
            sampled from the caller object.

        See Also
        --------
        DataFrameGroupBy.sample: Generates random samples from each group of a
            DataFrame object.
        SeriesGroupBy.sample: Generates random samples from each group of a
            Series object.
        numpy.random.choice: Generates a random sample from a given 1-D numpy
            array.

        Notes
        -----
        If `frac` > 1, `replacement` should be set to `True`.

        Snowpark pandas `sample` does not support the following cases: `weights` or `random_state`
        when `axis = 0`. Also, when `replace = False`, native pandas will raise error if `n` is larger
        than the length of the DataFrame while Snowpark pandas will return all rows from the DataFrame.

        Examples
        --------
        >>> df = pd.DataFrame({'num_legs': [2, 4, 8, 0],
        ...                    'num_wings': [2, 0, 0, 0],
        ...                    'num_specimen_seen': [10, 2, 1, 8]},
        ...                   index=['falcon', 'dog', 'spider', 'fish'])
        >>> df
                num_legs  num_wings  num_specimen_seen
        falcon         2          2                 10
        dog            4          0                  2
        spider         8          0                  1
        fish           0          0                  8

        Extract 3 random elements from the ``Series`` ``df['num_legs']``:

        >>> df['num_legs'].sample(n=3) # doctest: +SKIP
        fish      0
        spider    8
        falcon    2
        Name: num_legs, dtype: int64

        A random 50% sample of the ``DataFrame``:

        >>> df.sample(frac=0.5, replace=True) with replacement # doctest: +SKIP
              num_legs  num_wings  num_specimen_seen
        dog          4          0                  2
        fish         0          0                  8

        An upsample sample of the DataFrame with replacement: Note that replace parameter has to be True for frac parameter > 1.

        >>> df.sample(frac=2, replace=True) # doctest: +SKIP
                num_legs  num_wings  num_specimen_seen
        dog            4          0                  2
        fish           0          0                  8
        falcon         2          2                 10
        falcon         2          2                 10
        fish           0          0                  8
        dog            4          0                  2
        fish           0          0                  8
        dog            4          0                  2

        The exact number of specified rows is returned unless the DataFrame contains fewer rows:

        >>> df.sample(n=20)
                num_legs  num_wings  num_specimen_seen
        falcon         2          2                 10
        dog            4          0                  2
        spider         8          0                  1
        fish           0          0                  8
        """

    def sem():
        """
        Return unbiased standard error of the mean over requested axis.
        """

    @doc(
        _num_doc,
        desc="Return the mean of the values over the requested axis.",
        name1=_name1,
        axis_descr=_axis_descr,
        see_also="",
        examples=_mean_examples,
    )
    def mean():
        pass

    @doc(
        _num_doc,
        desc="Return the median of the values over the requested axis.",
        name1=_name1,
        axis_descr=_axis_descr,
        see_also="",
        examples=_median_examples,
    )
    def median():
        pass

    def set_flags():
        """
        Return a new `BasePandasDataset` with updated flags.
        """

    def shift():
        """
        Implement shared functionality between DataFrame and Series for shift. axis argument is only relevant for
        Dataframe, and should be 0 for Series.

        Args:
            periods : int | Sequence[int]
                Number of periods to shift. Can be positive or negative. If an iterable of ints,
                the data will be shifted once by each int. This is equivalent to shifting by one
                value at a time and concatenating all resulting frames. The resulting columns
                will have the shift suffixed to their column names. For multiple periods, axis must not be 1.

                Snowpark pandas does not currently support sequences of int for `periods`.

            freq : DateOffset, tseries.offsets, timedelta, or str, optional
                Offset to use from the tseries module or time rule (e.g. ‘EOM’).

                Snowpark pandas does not yet support this parameter.

            axis : {0 or 'index', 1 or 'columns', None}, default None
                Shift direction.

            fill_value : object, optional
                The scalar value to use for newly introduced missing values.
                the default depends on the dtype of `self`.
                For numeric data, ``np.nan`` is used.
                For datetime, timedelta, or period data, etc. :attr:`NaT` is used.
                For extension dtypes, ``self.dtype.na_value`` is used.

            suffix : str, optional
                If str is specified and periods is an iterable, this is added after the column name
                and before the shift value for each shifted column name.

                Snowpark pandas does not yet support this parameter.

        Returns:
            BasePandasDataset
        """

    def skew():
        """
        Return unbiased skew, normalized over n-1

        Parameters
        ----------
        axis: Optional[int]
            Axis to calculate skew on, only 0 (columnar) is supported
        skipna: Optional[bool]
            Exclude NA values when calculating result ( only True is supported )
        numeric_only: Optional[bool]
            Include only the numeric columns ( only True is supported )
        level: Optional[bool]
            Not Supported, included for compatibility with other stats calls

        Returns
        -------
        A series ( or scalar if used on a series ) with the calculated skew

        Examples
        --------
        >>> df = pd.DataFrame({'A': [0, 1, 2],
        ...           'B': [1, 2, 1],
        ...           'C': [3, 4, 5]})
        >>> df.skew()
        A    0.000000
        B    1.732059
        C    0.000000
        dtype: float64
        """

    def sort_index():
        """
        Sort object by labels (along an axis).
        """

    def sort_values():
        """
        Sort by the values along either axis.

        Parameters
        ----------
        by : str or list of str
            Name or list of names to sort by.
            - if axis is 0 or ‘index’ then by may contain index levels and/or column labels.
            - if axis is 1 or ‘columns’ then by may contain column levels and/or index labels.
        axis : {0 or ‘index’, 1 or ‘columns’}, default 0
             Axis to be sorted.
        ascending : bool or list of bool, default True
             Sort ascending vs. descending. Specify list for multiple sort
             orders.  If this is a list of bools, must match the length of
             the by.
        inplace : bool, default False
             If True, perform operation in-place.
        kind : {'quicksort', 'mergesort', 'heapsort', 'stable'} default 'None'
            Choice of sorting algorithm. By default, Snowpark Pandaas performs
            unstable sort. Please use 'stable' to perform stable sort. Other choices
            'quicksort', 'mergesort' and 'heapsort' are ignored.
        na_position : {'first', 'last'}, default 'last'
             Puts NaNs at the beginning if `first`; `last` puts NaNs at the
             end.
        ignore_index : bool, default False
             If True, the resulting axis will be labeled 0, 1, …, n - 1.
        key : callable, optional
            Apply the key function to the values
            before sorting. This is similar to the `key` argument in the
            builtin :meth:`sorted` function, with the notable difference that
            this `key` function should be *vectorized*. It should expect a
            ``Series`` and return a Series with the same shape as the input.
            It will be applied to each column in `by` independently.

        Returns
        -------
        DataFrame or None
            DataFrame with sorted values or None if ``inplace=True``.

        Notes
        -----
        Snowpark pandas API doesn't currently support distributed computation of
        sort_values when 'key' argument is provided or frame is sorted on 'columns' axis.

        See Also
        --------
        DataFrame.sort_index : Sort a DataFrame by the index.
        Series.sort_values : Similar method for a Series.

        Examples
        --------
        >>> df = pd.DataFrame({
        ...     'col1': ['A', 'A', 'B', np.nan, 'D', 'C'],
        ...     'col2': [2, 1, 9, 8, 7, 4],
        ...     'col3': [0, 1, 9, 4, 2, 3],
        ...     'col4': ['a', 'B', 'c', 'D', 'e', 'F']
        ... })
        >>> df
           col1  col2  col3 col4
        0     A     2     0    a
        1     A     1     1    B
        2     B     9     9    c
        3  None     8     4    D
        4     D     7     2    e
        5     C     4     3    F

        Sort by col1

        >>> df.sort_values(by=['col1'])
           col1  col2  col3 col4
        0     A     2     0    a
        1     A     1     1    B
        2     B     9     9    c
        5     C     4     3    F
        4     D     7     2    e
        3  None     8     4    D

        Sort by multiple columns

        >>> df.sort_values(by=['col1', 'col2'])
           col1  col2  col3 col4
        1     A     1     1    B
        0     A     2     0    a
        2     B     9     9    c
        5     C     4     3    F
        4     D     7     2    e
        3  None     8     4    D

        Sort Descending

        >>> df.sort_values(by='col1', ascending=False)
           col1  col2  col3 col4
        4     D     7     2    e
        5     C     4     3    F
        2     B     9     9    c
        0     A     2     0    a
        1     A     1     1    B
        3  None     8     4    D

        Putting NAs first

        >>> df.sort_values(by='col1', ascending=False, na_position='first')
           col1  col2  col3 col4
        3  None     8     4    D
        4     D     7     2    e
        5     C     4     3    F
        2     B     9     9    c
        0     A     2     0    a
        1     A     1     1    B
        """

    @doc(
        _num_ddof_doc,
        desc="Return sample standard deviation over requested axis."
        "\n\nNormalized by N-1 by default. This can be changed using the "
        "ddof argument.",
        name1=_name1,
        axis_descr=_axis_descr,
        notes=_std_notes,
        examples=_std_examples,
    )
    def std():
        pass

    @doc(
        _num_min_count_doc,
        desc="Return the sum of the values over the requested axis."
        "\n\nThis is equivalent to the method numpy.sum.",
        name1=_name1,
        axis_descr=_axis_descr,
        min_count=0,
        see_also="",
        examples=_sum_examples,
    )
    def sum():
        pass

    def sub():
        """
        Get subtraction of `BasePandasDataset` and `other`, element-wise (binary operator `sub`).
        """

    subtract = sub

    def swapaxes():
        """
        Interchange axes and swap values axes appropriately.
        """

    def swaplevel():
        """
        Swap levels `i` and `j` in a `MultiIndex`.
        """

    def tail():
        """
        Return the last n rows.
        """

    def take():
        """
        Return the elements in the given *positional* indices along an axis.

        This means that we are not indexing according to actual values in
        the index attribute of the object. We are indexing according to the
        actual position of the element in the object.

        Parameters
        ----------
        indices : array-like or slice
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
        >>> df = pd.DataFrame([('falcon', 'bird', 389.0),
        ...                    ('parrot', 'bird', 24.0),
        ...                    ('lion', 'mammal', 80.5),
        ...                    ('monkey', 'mammal', np.nan)],
        ...                   columns=['name', 'class', 'max_speed'],
        ...                   index=[0, 2, 3, 1])
        >>> df
             name   class  max_speed
        0  falcon    bird      389.0
        2  parrot    bird       24.0
        3    lion  mammal       80.5
        1  monkey  mammal        NaN

        Take elements at positions 0 and 3 along the axis 0 (default).

        Note how the actual indices selected (0 and 1) do not correspond to
        our selected indices 0 and 3. That's because we are selecting the 0th
        and 3rd rows, not rows whose indices equal 0 and 3.

        >>> df.take([0, 3])
             name   class  max_speed
        0  falcon    bird      389.0
        1  monkey  mammal        NaN

        Take elements at indices 1 and 2 along the axis 1 (column selection).

        >>> df.take([1, 2], axis=1)
            class  max_speed
        0    bird      389.0
        2    bird       24.0
        3  mammal       80.5
        1  mammal        NaN

        We may take elements using negative integers for positive indices,
        starting from the end of the object, just like with Python lists.

        >>> df.take([-1, -2])
             name   class  max_speed
        1  monkey  mammal        NaN
        3    lion  mammal       80.5
        """

    def to_clipboard():
        """
        Copy object to the system clipboard.
        """

    def to_csv():
        """
        Write object to a comma-separated values (csv) file.
        """

    def to_excel():
        """
        Write object to an Excel sheet.
        """

    def to_hdf():
        """
        Write the contained data to an HDF5 file using HDFStore.
        """

    def to_json():
        """
        Convert the object to a JSON string.
        """

    def to_latex():
        """
        Render object to a LaTeX tabular, longtable, or nested table.
        """

    def to_markdown():
        """
        Print `BasePandasDataset` in Markdown-friendly format.
        """

    def to_pickle():
        """
        Pickle (serialize) object to file.
        """

    def to_numpy():
        """
        Convert the DataFrame or Series to a NumPy array.

        By default, the dtype of the returned array will be the common NumPy
        dtype of all types in the DataFrame or Series. Snowpark pandas API will draw data from Snowflake,
        and perform client-side conversion into desired dtype.

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
            on `dtype` and the dtypes of the DataFrame columns. For example, in a
            float64 array, ``np.nan`` will be replaced by this value.

        Returns
        -------
        numpy.ndarray

        Examples
        --------
        >>> pd.DataFrame({"A": [1, 2], "B": [3, 4]}).to_numpy()
        array([[1, 3],
               [2, 4]])
        """

    def to_period():
        """
        Convert `BasePandasDataset` from DatetimeIndex to PeriodIndex.
        """

    def to_string():
        """
        Render a `BasePandasDataset` to a console-friendly tabular output.
        """

    def to_sql():
        """
        Write records stored in a `BasePandasDataset` to a SQL database.
        """

    def to_timestamp():
        """
        Cast to DatetimeIndex of timestamps, at *beginning* of period.
        """

    def to_xarray():
        """
        Return an xarray object from the `BasePandasDataset`.
        """

    def truediv():
        """
        Get floating division of `BasePandasDataset` and `other`, element-wise (binary operator `truediv`).
        """

    div = divide = truediv

    def truncate():
        """
        Truncate a `BasePandasDataset` before and after some index value.
        """

    def transform():
        """
        Call ``func`` on self producing a `BasePandasDataset` with the same axis shape as self.
        """

    def tz_convert():
        """
        Convert tz-aware axis to target time zone.
        """

    def tz_localize():
        """
        Localize tz-naive index of a `BasePandasDataset` to target time zone.
        """

    @doc(
        _num_ddof_doc,
        desc="Return unbiased variance over requested axis.\n\nNormalized by "
        "N-1 by default. This can be changed using the ddof argument.",
        name1=_name1,
        axis_descr=_axis_descr,
        notes="",
        examples=_var_examples,
    )
    def var():
        pass

    def __abs__():
        """
        Return a `BasePandasDataset` with absolute numeric value of each element.

        Returns
        -------
        BasePandasDataset
            Object containing the absolute value of each element.
        """

    @_doc_binary_op(
        operation="union", bin_op="and", right="other", **_doc_binary_op_kwargs
    )
    def __and__():
        pass

    @_doc_binary_op(
        operation="union", bin_op="rand", right="other", **_doc_binary_op_kwargs
    )
    def __rand__():
        pass

    def __array__():
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

    def __array_wrap__():
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

    def __copy__():
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

    def __deepcopy__():
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

    @_doc_binary_op(
        operation="equality comparison",
        bin_op="eq",
        right="other",
        **_doc_binary_op_kwargs,
    )
    def __eq__():
        pass

    def __finalize__():
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

    @_doc_binary_op(
        operation="greater than or equal comparison",
        bin_op="ge",
        right="right",
        **_doc_binary_op_kwargs,
    )
    def __ge__():
        pass

    def __getitem__():
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

    @_doc_binary_op(
        operation="greater than comparison",
        bin_op="gt",
        right="right",
        **_doc_binary_op_kwargs,
    )
    def __gt__():
        pass

    def __invert__():
        """
        Apply bitwise inverse to each element of the `BasePandasDataset`.

        Returns
        -------
        BasePandasDataset
            New BasePandasDataset containing bitwise inverse to each value.
        """

    @_doc_binary_op(
        operation="less than or equal comparison",
        bin_op="le",
        right="right",
        **_doc_binary_op_kwargs,
    )
    def __le__():
        pass

    def __len__():
        """
        Return length of info axis.

        Returns
        -------
        int
        """

    @_doc_binary_op(
        operation="less than comparison",
        bin_op="lt",
        right="right",
        **_doc_binary_op_kwargs,
    )
    def __lt__():
        pass

    def __matmul__():
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

    @_doc_binary_op(
        operation="not equal comparison",
        bin_op="ne",
        right="other",
        **_doc_binary_op_kwargs,
    )
    def __ne__():
        pass

    def __neg__():
        """
        Change the sign for every value of self.

        Returns
        -------
        BasePandasDataset
        """

    def __nonzero__():
        """
        Evaluate `BasePandasDataset` as boolean object.

        Raises
        ------
        ValueError
            Always since truth value for self is ambiguous.
        """

    __bool__ = __nonzero__

    @_doc_binary_op(
        operation="disjunction",
        bin_op="or",
        right="other",
        **_doc_binary_op_kwargs,
    )
    def __or__():
        pass

    @_doc_binary_op(
        operation="disjunction",
        bin_op="ror",
        right="other",
        **_doc_binary_op_kwargs,
    )
    def __ror__():
        pass

    def __sizeof__():
        """
        Generate the total memory usage for an `BasePandasDataset`.

        Returns
        -------
        int
        """

    def __str__():
        """
        Return str(self).

        Returns
        -------
        str
        """

    @_doc_binary_op(
        operation="exclusive disjunction",
        bin_op="xor",
        right="other",
        **_doc_binary_op_kwargs,
    )
    def __xor__():
        pass

    @_doc_binary_op(
        operation="exclusive disjunction",
        bin_op="rxor",
        right="other",
        **_doc_binary_op_kwargs,
    )
    def __rxor__():
        pass

    @property
    def size():
        """Return an int representing the number of elements in this object."""

    @property
    def values():
        """
        Return a NumPy representation of the dataset.

        Returns
        -------
        np.ndarray
        """

    def __array_ufunc__():
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

    def __array_function__():
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

    @doc(
        _get_set_index_doc,
        desc="Get the index for this `Series`/`DataFrame`.",
        parameters_or_returns="Returns\n-------\nIndex\n    The index for this `Series`/`DataFrame`.",
    )
    def _get_index():
        pass

    @doc(
        _get_set_index_doc,
        desc="Set the index for this `Series`/`DataFrame`.",
        parameters_or_returns="Parameters\n----------\nnew_index : Index\n    The new index to set.",
    )
    def _set_index():
        pass

    index = property(_get_index, _set_index)
