#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This module contains DataFrame docstrings that override modin's docstrings."""

from textwrap import dedent

from pandas.util._decorators import doc

from snowflake.snowpark.modin.plugin.docstrings.shared_docs import (
    _doc_binary_op,
    _shared_docs,
)

_doc_binary_op_kwargs = {"returns": "BasePandasDataset", "left": "BasePandasDataset"}


_shared_doc_kwargs = {
    "axes": "index, columns",
    "klass": "DataFrame",
    "axes_single_arg": "{0 or 'index', 1 or 'columns'}",
    "axis": """axis : {0 or 'index', 1 or 'columns'}, default 0
        If 0 or 'index': apply function to each column.
        If 1 or 'columns': apply function to each row.""",
    "inplace": """
    inplace : bool, default False
        Whether to modify the DataFrame rather than creating a new one.""",
    "optional_by": """
by : str or list of str
    Name or list of names to sort by.

    - if `axis` is 0 or `'index'` then `by` may contain index
      levels and/or column labels.
    - if `axis` is 1 or `'columns'` then `by` may contain column
      levels and/or index labels.""",
    "optional_reindex": """
labels : array-like, optional
    New labels / index to conform the axis specified by 'axis' to.
index : array-like, optional
    New labels for the index. Preferably an Index object to avoid
    duplicating data.
columns : array-like, optional
    New labels for the columns. Preferably an Index object to avoid
    duplicating data.
axis : int or str, optional
    Axis to target. Can be either the axis name ('index', 'columns')
    or number (0, 1).""",
}


class DataFrame:
    """
    Snowpark pandas representation of ``pandas.DataFrame`` with a lazily-evaluated relational dataset.

    A DataFrame is considered lazy because it encapsulates the computation or query required to produce
    the final dataset. The computation is not performed until the datasets need to be displayed, or I/O
    methods like to_pandas, to_snowflake are called.

    Internally, the underlying data are stored as Snowflake table with rows and columns.

    Parameters
    ----------
    data : DataFrame, Series, `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_, ndarray, Iterable or dict, optional
        Dict can contain ``Series``, arrays, constants, dataclass or list-like objects.
        If data is a dict, column order follows insertion-order.
    index : Index or array-like, optional
        Index to use for resulting frame. Will default to ``RangeIndex`` if no
        indexing information part of input data and no index provided.
    columns : Index or array-like, optional
        Column labels to use for resulting frame. Will default to
        ``RangeIndex`` if no column labels are provided.
    dtype : str, np.dtype, or pandas.ExtensionDtype, optional
        Data type to force. Only a single dtype is allowed. If None, infer.
    copy : bool, default: False
        Copy data from inputs. Only affects ``pandas.DataFrame`` / 2d ndarray input.
    query_compiler : BaseQueryCompiler, optional
        A query compiler object to create the ``DataFrame`` from.

    Notes
    -----
    ``DataFrame`` can be created either from passed `data` or `query_compiler`. If both
    parameters are provided, data source will be prioritized in the next order:

    1) Modin ``DataFrame`` or ``Series`` passed with `data` parameter.
    2) Query compiler from the `query_compiler` parameter.
    3) Various pandas/NumPy/Python data structures passed with `data` parameter.

    The last option is less desirable since import of such data structures is very
    inefficient, please use previously created Modin structures from the fist two
    options or import data using highly efficient Modin IO tools (for example
    ``pd.read_csv``).

    Examples
    --------
    Creating a Snowpark pandas DataFrame from a dictionary:

    >>> d = {'col1': [1, 2], 'col2': [3, 4]}
    >>> df = pd.DataFrame(data=d)
    >>> df
       col1  col2
    0     1     3
    1     2     4

    Constructing DataFrame from numpy ndarray:

    >>> df2 = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
    ...                    columns=['a', 'b', 'c'])
    >>> df2
       a  b  c
    0  1  2  3
    1  4  5  6
    2  7  8  9

    Constructing DataFrame from a numpy ndarray that has labeled columns:

    >>> data = np.array([(1, 2, 3), (4, 5, 6), (7, 8, 9)],
    ...                 dtype=[("a", "i4"), ("b", "i4"), ("c", "i4")])
    >>> df3 = pd.DataFrame(data, columns=['c', 'a'])
    ...
    >>> df3
       c  a
    0  3  1
    1  6  4
    2  9  7

    Constructing DataFrame from Series/DataFrame:

    >>> ser = pd.Series([1, 2, 3], index=["a", "b", "c"], name = "s")
    >>> df = pd.DataFrame(data=ser, index=["a", "c"])
    >>> df
       s
    a  1
    c  3
    >>> df1 = pd.DataFrame([1, 2, 3], index=["a", "b", "c"], columns=["x"])
    >>> df2 = pd.DataFrame(data=df1, index=["a", "c"])
    >>> df2
       x
    a  1
    c  3
    """

    def __repr__():
        """
        Return a string representation for a particular ``DataFrame``.

        Returns
        -------
        str
        """

    @property
    def ndim():
        """
        Return the number of dimensions of the underlying data, by definition 2.
        """

    def drop_duplicates():
        """
        Return ``DataFrame`` with duplicate rows removed.

        Considering certain columns is optional. Indexes, including time indexes are ignored.

        Parameters
        ----------
        subset : column label or sequence of labels, optional
            Only consider certain columns for identifying duplicates, by default use all columns.
        keep : {'first', 'last', False}, default 'first'
            Determines which duplicates (if any) to keep.
            'first' : Drop duplicates except for the first occurrence.
            'last' : Drop duplicates except for the last occurrence.
            False : Drop all duplicates.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
        ignore_index : bool, default False
            If True, the resulting axis will be labeled 0, 1, …, n - 1.

        Returns
        -------
        DataFrame or None
            DataFrame with duplicates removed or None if inplace=True.

        Examples
        --------
        Consider dataset containing ramen rating.

        >>> df = pd.DataFrame({
        ...     'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        ...     'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        ...     'rating': [4, 4, 3.5, 15, 5]
        ... })
        >>> df
             brand style  rating
        0  Yum Yum   cup     4.0
        1  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        3  Indomie  pack    15.0
        4  Indomie  pack     5.0

        By default, it removes duplicate rows based on all columns.

        >>> df.drop_duplicates()
             brand style  rating
        0  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        3  Indomie  pack    15.0
        4  Indomie  pack     5.0

        To remove duplicates on specific column(s), use subset.

        >>> df.drop_duplicates(subset=['brand'])
             brand style  rating
        0  Yum Yum   cup     4.0
        2  Indomie   cup     3.5

        To remove duplicates and keep last occurrences, use keep.

        >>> df.drop_duplicates(subset=['brand', 'style'], keep='last')
             brand style  rating
        1  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        4  Indomie  pack     5.0
        """

    def dropna():
        """
        Remove missing values.

        Parameters
        ----------
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Determine if rows or columns which contain missing values are
            removed.

            * 0, or 'index' : Drop rows which contain missing values.
            * 1, or 'columns' : Drop columns which contain missing value.

            .. versionchanged:: 1.0.0

               Pass tuple or list to drop on multiple axes.
               Only a single axis is allowed.

        how : {'any', 'all'}, default 'any'
            Determine if row or column is removed from DataFrame, when we have
            at least one NA or all NA.

            * 'any' : If any NA values are present, drop that row or column.
            * 'all' : If all values are NA, drop that row or column.

        thresh : int, optional
            Require that many non-NA values. Cannot be combined with how.
        subset : column label or sequence of labels, optional
            Labels along other axis to consider, e.g. if you are dropping rows
            these would be a list of columns to include.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.

        Returns
        -------
        DataFrame
            DataFrame with NA entries dropped from it or None if ``inplace=True``.

        See Also
        --------
        DataFrame.isna : Indicate missing values.
        DataFrame.notna : Indicate existing (non-missing) values.
        DataFrame.fillna : Replace missing values.
        Series.dropna : Drop missing values.
        Index.dropna : Drop missing indices.

        Examples
        --------
        >>> df = pd.DataFrame({"name": ['Alfred', 'Batman', 'Catwoman'],
        ...                    "toy": [None, 'Batmobile', 'Bullwhip'],
        ...                    "born": [pd.NaT, pd.Timestamp("1940-04-25"),
        ...                             pd.NaT]})
        >>> df
               name        toy       born
        0    Alfred       None        NaT
        1    Batman  Batmobile 1940-04-25
        2  Catwoman   Bullwhip        NaT

        Drop the rows where at least one element is missing.

        >>> df.dropna()
             name        toy       born
        1  Batman  Batmobile 1940-04-25

        Drop the rows where all elements are missing.

        >>> df.dropna(how='all')
               name        toy       born
        0    Alfred       None        NaT
        1    Batman  Batmobile 1940-04-25
        2  Catwoman   Bullwhip        NaT

        Keep only the rows with at least 2 non-NA values.

        >>> df.dropna(thresh=2)
               name        toy       born
        1    Batman  Batmobile 1940-04-25
        2  Catwoman   Bullwhip        NaT

        Define in which columns to look for missing values.

        >>> df.dropna(subset=['name', 'toy'])
               name        toy       born
        1    Batman  Batmobile 1940-04-25
        2  Catwoman   Bullwhip        NaT

        Keep the DataFrame with valid entries in the same variable.

        >>> df.dropna(inplace=True)
        >>> df
             name        toy       born
        1  Batman  Batmobile 1940-04-25
        """

    @property
    def dtypes():
        """
        Return the dtypes in the ``DataFrame``.
        This returns a Series with the data type of each column.
        The result's index is the original DataFrame's columns. Columns
        with mixed types are stored with the  ``object`` dtype.

        The returned dtype for each label is the 'largest' numpy type for the
        underlying data.

        For labels with integer-type data, int64 is returned.

        For floating point and decimal data, float64 is returned.

        For boolean data, numpy.bool is returned.

        For datetime or timestamp data, datetime64[ns] is returned.

        For all other data types, including string, date, binary or snowflake variants,
        the dtype object is returned.

        This function is lazy and does NOT trigger evaluation of the underlying
        ``DataFrame``.

        Note that because the returned dtype(s) may be of a larger type than the underlying
        data, the result of this function may differ from the dtypes of the output of the
        :func:`to_pandas()` function.
        Calling :func:`to_pandas()` triggers materialization into a native
        pandas DataFrame. The dtypes of this materialized result are the narrowest
        type(s) that can represent the underlying data (like int16, or int32).

        Returns
        -------
        pandas.Series
            Native pandas (not Snowpark pandas) Series with the dtype for each label.

        Examples
        --------
        >>> df = pd.DataFrame({'float': [1.0],
        ...                    'int': [1],
        ...                    'datetime': [pd.Timestamp('20180310')],
        ...                    'string': ['foo']})
        >>> df.dtypes
        float              float64
        int                  int64
        datetime    datetime64[ns]
        string              object
        dtype: object
        """

    def duplicated():
        """
        Return boolean Series denoting duplicate rows.

        Considering certain columns is optional.

        Parameters
        ----------
        subset : column label or sequence of labels, optional
            Only consider certain columns for identifying duplicates, by
            default use all the columns.
        keep : {'first', 'last', False}, default 'first'
            Determines which duplicates (if any) to mark.

            - ``first`` : Mark duplicates as ``True`` except for the first occurrence.
            - ``last`` : Mark duplicates as ``True`` except for the last occurrence.
            - False : Mark all duplicates as ``True``.

        Returns
        -------
        Series
            Boolean series for each duplicated rows.

        See Also
        --------
        Index.duplicated : Equivalent method on index.
        Series.duplicated : Equivalent method on Series.
        Series.drop_duplicates : Remove duplicate values from Series.
        DataFrame.drop_duplicates : Remove duplicate values from DataFrame.

        Examples
        --------
        Consider dataset containing ramen rating.

        >>> df = pd.DataFrame({
        ...     'brand': ['Yum Yum', 'Yum Yum', 'Indomie', 'Indomie', 'Indomie'],
        ...     'style': ['cup', 'cup', 'cup', 'pack', 'pack'],
        ...     'rating': [4, 4, 3.5, 15, 5]
        ... })
        >>> df
             brand style  rating
        0  Yum Yum   cup     4.0
        1  Yum Yum   cup     4.0
        2  Indomie   cup     3.5
        3  Indomie  pack    15.0
        4  Indomie  pack     5.0

        By default, for each set of duplicated values, the first occurrence
        is set on False and all others on True.

        >>> df.duplicated()
        0    False
        1     True
        2    False
        3    False
        4    False
        dtype: bool

        By using 'last', the last occurrence of each set of duplicated values
        is set on False and all others on True.

        >>> df.duplicated(keep='last')
        0     True
        1    False
        2    False
        3    False
        4    False
        dtype: bool

        By setting ``keep`` on False, all duplicates are True.

        >>> df.duplicated(keep=False)
        0     True
        1     True
        2    False
        3    False
        4    False
        dtype: bool

        To find duplicates on specific column(s), use ``subset``.

        >>> df.duplicated(subset=['brand'])
        0    False
        1     True
        2    False
        3     True
        4     True
        dtype: bool
        """

    @property
    def empty():
        """
        Indicator whether the DataFrame is empty.

        True if the DataFrame is entirely empty (no items), meaning any of the axes are of length 0.

        Returns
        -------
        bool
        """

    @property
    def axes():
        """
        Return a list representing the axes of the DataFrame.

        It has the row axis labels and column axis labels as the only members.
        They are returned in that order.

        Examples
        --------
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> df.axes
        [Index([0, 1], dtype='int64'), Index(['col1', 'col2'], dtype='object')]
        """

    @property
    def shape(self):
        """
        Return a tuple representing the dimensionality of the ``DataFrame``.
        """

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

    def applymap():
        """
        Apply a function to a Dataframe elementwise.

        This method applies a function that accepts and returns a scalar
        to every element of a DataFrame.

        Parameters
        ----------
        func : callable
            Python function, returns a single value from a single value.
        na_action : {None, 'ignore'}, default None
            If ‘ignore’, propagate NaN values, without passing them to func.
        **kwargs
            Additional keyword arguments to pass as keywords arguments to
            `func`.

        Returns
        -------
        DataFrame
            Transformed DataFrame.

        See Also
        --------
        :func:`Series.apply <snowflake.snowpark.modin.pandas.Series.apply>` : For applying more complex functions on a Series.

        :func:`DataFrame.apply <snowflake.snowpark.modin.pandas.DataFrame.apply>` : Apply a function row-/column-wise.

        Examples
        --------
        >>> df = pd.DataFrame([[1, 2.12], [3.356, 4.567]])
        >>> df
               0      1
        0  1.000  2.120
        1  3.356  4.567

        >>> df.applymap(lambda x: len(str(x)))
           0  1
        0  3  4
        1  5  5

        When you use the applymap function, a user-defined function (UDF) is generated and
        applied to each column. However, in many cases, you can achieve the same results
        more efficiently by utilizing alternative dataframe operations instead of applymap.
        For example, You could square each number elementwise.

        >>> df.applymap(lambda x: x**2)
                   0          1
        0   1.000000   4.494400
        1  11.262736  20.857489

        But it's better to avoid applymap in that case.

        >>> df ** 2
                   0          1
        0   1.000000   4.494400
        1  11.262736  20.857489
        """

    _agg_examples_doc = dedent(
        """
    Examples
    --------
    >>> df = pd.DataFrame([[1, 2, 3],
    ...                    [4, 5, 6],
    ...                    [7, 8, 9],
    ...                    [np.nan, np.nan, np.nan]],
    ...                   columns=['A', 'B', 'C'])

    Aggregate these functions over the rows.

    >>> df.agg(['sum', 'min'])
            A     B     C
    sum  12.0  15.0  18.0
    min   1.0   2.0   3.0

    Different aggregations per column.

    >>> df.agg({'A' : ['sum', 'min'], 'B' : ['min', 'max']})
            A    B
    sum  12.0  NaN
    min   1.0  2.0
    max   NaN  8.0

    Aggregate over the columns.

    >>> df.agg("max", axis="columns")
    0    3.0
    1    6.0
    2    9.0
    3    NaN
    dtype: float64

    Different aggregations per row.

    >>> df.agg({ 0: ["sum"], 1: ["min"] }, axis=1)
       sum  min
    0  6.0  NaN
    1  NaN  4.0
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
        Apply a function along an axis of the DataFrame.

        Objects passed to the function are Series objects whose index is
        either the DataFrame's index (``axis=0``) or the DataFrame's columns
        (``axis=1``). By default (``result_type=None``), the final return type
        is inferred from the return type of the applied function. Otherwise,
        it depends on the `result_type` argument.

        Snowpark pandas currently only supports ``apply`` with ``axis=1`` and callable ``func``.

        Parameters
        ----------
        func : function
            A Python function object to apply to each column or row, or a Python function decorated with @udf.

        axis : {0 or 'index', 1 or 'columns'}, default 0
            Axis along which the function is applied:

            * 0 or 'index': apply function to each column.
            * 1 or 'columns': apply function to each row.

            Snowpark pandas does not yet support ``axis=0``.

        raw : bool, default False
            Determines if row or column is passed as a Series or ndarray object:

            * ``False`` : passes each row or column as a Series to the
              function.
            * ``True`` : the passed function will receive ndarray objects
              instead.

        result_type : {'expand', 'reduce', 'broadcast', None}, default None
            These only act when ``axis=1`` (columns):

            * 'expand' : list-like results will be turned into columns.
            * 'reduce' : returns a Series if possible rather than expanding
              list-like results. This is the opposite of 'expand'.
            * 'broadcast' : results will be broadcast to the original shape
              of the DataFrame, the original index and columns will be
              retained.

            Snowpark pandas does not yet support the ``result_type`` parameter.

        args : tuple
            Positional arguments to pass to `func` in addition to the
            array/series.

        **kwargs
            Additional keyword arguments to pass as keywords arguments to
            `func`.

        Returns
        -------
        Series or DataFrame
            Result of applying ``func`` along the given axis of the DataFrame.

        See Also
        --------
        :func:`Series.apply <snowflake.snowpark.modin.pandas.Series.apply>` : For applying more complex functions on a Series.

        :func:`DataFrame.applymap <snowflake.snowpark.modin.pandas.DataFrame.applymap>` : Apply a function elementwise on a whole DataFrame.

        Notes
        -----
        1. When ``func`` has a type annotation for its return value, the result will be cast
        to the corresponding dtype. When no type annotation is provided, data will be converted
        to VARIANT type in Snowflake, and the result will have ``dtype=object``. In this case, the return value must
        be JSON-serializable, which can be a valid input to ``json.dumps`` (e.g., ``dict`` and
        ``list`` objects are JSON-serializable, but ``bytes`` and ``datetime.datetime`` objects
        are not). The return type hint is used only when ``func`` is a series-to-scalar function.

        2. Under the hood, we use Snowflake Vectorized Python UDFs to implement apply() method with
        `axis=1`. You can find type mappings from Snowflake SQL types to pandas dtypes
        `here <https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-batch#type-support>`_.

        3. Snowflake supports two types of NULL values in variant data: `JSON NULL and SQL NULL <https://docs.snowflake.com/en/user-guide/semistructured-considerations#null-values>`_.
        When no type annotation is provided and Variant data is returned, Python ``None`` is translated to
        JSON NULL, and all other pandas missing values (np.nan, pd.NA, pd.NaT) are translated to SQL NULL.

        4. If ``func`` is a series-to-series function that can also be used as a scalar-to-scalar function
        (e.g., ``np.sqrt``, ``lambda x: x+1``), using ``df.applymap()`` to apply the function
        element-wise may give better performance.

        5. When ``func`` can return a series with different indices, e.g.,
        ``lambda x: pd.Series([1, 2], index=["a", "b"] if x.sum() > 2 else ["b", "c"])``,
        the values with the same label will be merged together.

        6. The index values of returned series from ``func`` must be JSON-serializable. For example,
        ``lambda x: pd.Series([1], index=[bytes(1)])`` will raise a SQL execption because python ``bytes``
        objects are not JSON-serializable.

        7. When ``func`` uses any first-party modules or third-party packages inside the function,
        you need to add these dependencies via ``session.add_import()`` and ``session.add_packages()``.
        Alternatively. specify third-party packages with the @udf decorator. When using the @udf decorator,
        annotations using PandasSeriesType or PandasDataFrameType are not supported.

        8. The Snowpark pandas module cannot currently be referenced inside the definition of
        ``func``. If you need to call a general pandas API like ``pd.Timestamp`` inside ``func``,
        please use the original ``pandas`` module (with ``import pandas``) as a workaround.

        Examples
        --------
        >>> df = pd.DataFrame([[2, 0], [3, 7], [4, 9]], columns=['A', 'B'])
        >>> df
           A  B
        0  2  0
        1  3  7
        2  4  9

        Using a reducing function on ``axis=1``:

        >>> df.apply(np.sum, axis=1)
        0     2
        1    10
        2    13
        dtype: int64

        Returning a list-like object will result in a Series:

        >>> df.apply(lambda x: [1, 2], axis=1)
        0    [1, 2]
        1    [1, 2]
        2    [1, 2]
        dtype: object

        To work with 3rd party packages, add them to the current session:

        >>> import scipy.stats
        >>> pd.session.custom_package_usage_config['enabled'] = True
        >>> pd.session.add_packages(['numpy', scipy])
        >>> df.apply(lambda x: np.dot(x * scipy.stats.norm.cdf(0), x * scipy.stats.norm.cdf(0)), axis=1)
        0     1.00
        1    14.50
        2    24.25
        dtype: float64

        or annotate the function
        with the @udf decorator from Snowpark https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.functions.udf.

        >>> from snowflake.snowpark.functions import udf
        >>> from snowflake.snowpark.types import DoubleType
        >>> @udf(packages=['statsmodels>0.12'], return_type=DoubleType())
        ... def autocorr(column):
        ...    import pandas as pd
        ...    import statsmodels.tsa.stattools
        ...    return pd.Series(statsmodels.tsa.stattools.pacf_ols(column.values)).mean()
        ...
        >>> df.apply(autocorr, axis=0)  # doctest: +SKIP
        A    0.857143
        B    0.428571
        dtype: float64
        """

    def assign():
        """
        Assign new columns to a ``DataFrame``.

        Returns a new object with all original columns in addition to new ones. Existing
        columns that are re-assigned will be overwritten.

        Parameters
        ----------
        **kwargs: dict of {str: callable or Series}
            The column names are the keywords. If the values are callable, they are computed
            on the DataFrame and assigned to the new columns. The callable must not change input
            DataFrame (though Snowpark pandas doesn't check it). If the values are not callable,
            (e.g. a Series, scalar, or array), they are simply assigned.

        Returns
        -------
        DataFrame
            A new DataFrame with the new columns in addition to all the existing columns.

        Notes
        -----
        - Assigning multiple columns within the same assign is possible. Later items in `**kwargs`
          may refer to newly created or modified columns in `df`; items are computed and assigned into `df` in order.

        - If an array that of the wrong length is passed in to assign, Snowpark pandas will either truncate the array, if it is too long,
          or broadcast the last element of the array until the array is the correct length if it is too short. This differs from native pandas,
          which will error out with a ValueError if the length of the array does not match the length of `df`.
          This is done to preserve Snowpark pandas' lazy evaluation paradigm.

        Examples
        --------
        >>> df = pd.DataFrame({'temp_c': [17.0, 25.0]},
        ...                   index=['Portland', 'Berkeley'])
        >>> df
                  temp_c
        Portland    17.0
        Berkeley    25.0

        >>> df.assign(temp_f=lambda x: x.temp_c * 9 / 5 + 32)
                  temp_c  temp_f
        Portland    17.0    62.6
        Berkeley    25.0    77.0

        >>> df.assign(temp_f=df['temp_c'] * 9 / 5 + 32)
                  temp_c  temp_f
        Portland    17.0    62.6
        Berkeley    25.0    77.0

        >>> df.assign(temp_f=lambda x: x['temp_c'] * 9 / 5 + 32,
        ...           temp_k=lambda x: (x['temp_f'] + 459.67) * 5 / 9)
                  temp_c  temp_f  temp_k
        Portland    17.0    62.6  290.15
        Berkeley    25.0    77.0  298.15

        >>> df = pd.DataFrame({'col1': [17.0, 25.0, 22.0]})
        >>> df
           col1
        0  17.0
        1  25.0
        2  22.0

        >>> df.assign(new_col=[10, 11])
           col1  new_col
        0  17.0       10
        1  25.0       11
        2  22.0       11

        >>> df.assign(new_col=[10, 11, 12, 13, 14])
           col1  new_col
        0  17.0       10
        1  25.0       11
        2  22.0       12
        """

    def groupby():
        """
        Group DataFrame using a mapper or by a Series of columns.

        Args:
            by: mapping, function, label, Snowpark pandas Series or a list of such. Used to determine the groups for the groupby.
                If by is a function, it’s called on each value of the object’s index. If a dict or Snowpark pandas Series is
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
                better performance can be achieved by turning sort off, this is not going to be true with
                SnowparkPandas. When sort=False, the performance will be no better than sort=True.

            group_keys: bool, default True
                    When calling apply and the by argument produces a like-indexed (i.e. a transform) result, add group
                    keys to index to identify pieces. By default group keys are not included when the result’s index
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
            Snowpark pandas DataFrameGroupBy: Returns a groupby object that contains information about the groups.

        Examples::

            >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
            ...                               'Parrot', 'Parrot'],
            ...                    'Max Speed': [380., 370., 24., 26.]})
            >>> df
               Animal  Max Speed
            0  Falcon      380.0
            1  Falcon      370.0
            2  Parrot       24.0
            3  Parrot       26.0

            >>> df.groupby(['Animal']).mean()   # doctest: +NORMALIZE_WHITESPACE
                    Max Speed
            Animal
            Falcon      375.0
            Parrot       25.0

            **Hierarchical Indexes**

            We can groupby different levels of a hierarchical index
            using the `level` parameter:

            >>> arrays = [['Falcon', 'Falcon', 'Parrot', 'Parrot'],
            ...           ['Captive', 'Wild', 'Captive', 'Wild']]
            >>> index = pd.MultiIndex.from_arrays(arrays, names=('Animal', 'Type'))
            >>> df = pd.DataFrame({'Max Speed': [390., 350., 30., 20.]},
            ...                   index=index)
            >>> df      # doctest: +NORMALIZE_WHITESPACE
                            Max Speed
            Animal Type
            Falcon Captive      390.0
                   Wild         350.0
            Parrot Captive       30.0
                   Wild          20.0

            >>> df.groupby(level=0).mean()      # doctest: +NORMALIZE_WHITESPACE
                    Max Speed
            Animal
            Falcon      370.0
            Parrot       25.0

            >>> df.groupby(level="Type").mean()     # doctest: +NORMALIZE_WHITESPACE
                     Max Speed
            Type
            Captive      210.0
            Wild         185.0
        """

    def keys():
        """
        Get columns of the ``DataFrame``.
        """

    def transform():
        """
        Call ``func`` on self producing a Snowpark pandas DataFrame with the same axis shape as self.

        Parameters
        ----------
        func : function, str, list-like or dict-like
            Function to use for transforming the data. If a function, must either work when passed
            a DataFrame or when passed to DataFrame.apply. If func is both list-like and dict-like,
            dict-like behavior takes precedence.

            Snowpark pandas currently only supports callable arguments, and does not yet
            support string, dict-like, or list-like arguments.

        axis : {0 or 'index', 1 or 'columns'}, default 0
            If 0 or 'index': apply function to each column. If 1 or 'columns': apply function to each row.

            Snowpark pandas currently only supports axis=1, and does not yet support axis=0.

        *args
            Positional arguments to pass to `func`.

        **kwargs
            Keyword arguments to pass to `func`.

        Examples
        --------
        Increment every value in DataFrame by 1.

        >>> d1 = {'col1': [1, 2, 3], 'col2': [3, 4, 5]}
        >>> df = pd.DataFrame(data=d1)
        >>> df
           col1  col2
        0     1     3
        1     2     4
        2     3     5
        >>> df.transform(lambda x: x + 1, axis=1)
           col1  col2
        0     2     4
        1     3     5
        2     4     6

        Apply a numpy ufunc to every value in the DataFrame.

        >>> df.transform(np.square, axis=1)
           col1  col2
        0     1     9
        1     4    16
        2     9    25
        """

    def transpose():
        """
        Transpose index and columns.

        Reflect the DataFrame over its main diagonal by writing rows as columns and vice-versa. The property T is an accessor to the method transpose().

        Args:
            *args tuple, optional
                Accepted for compatibility with NumPy.  Note these arguments are ignored in the snowpark pandas
                implementation unless go through a fallback path, in which case they may be used by the native
                pandas implementation.

            copy bool, default False
                Whether to copy the data after transposing, even for DataFrames with a single dtype.  The snowpark
                pandas implementation ignores this parameter.

            Note that a copy is always required for mixed dtype DataFrames, or for DataFrames with any extension types.

        Returns:
            DataFrame
                The transposed DataFrame.

        Examples::
            Square DataFrame with homogeneous dtype

            >>> d1 = {'col1': [1, 2], 'col2': [3, 4]}
            >>> df1 = pd.DataFrame(data=d1)
            >>> df1
               col1  col2
            0     1     3
            1     2     4

            >>> df1_transposed = df1.T  # or df1.transpose()
            >>> df1_transposed
                  0  1
            col1  1  2
            col2  3  4

            When the dtype is homogeneous in the original DataFrame, we get a transposed DataFrame with the same dtype:

            >>> df1.dtypes
            col1    int64
            col2    int64
            dtype: object

            >>> df1_transposed.dtypes
            0    int64
            1    int64
            dtype: object

            Non-square DataFrame with mixed dtypes

            >>> d2 = {'name': ['Alice', 'Bob'],
            ...      'score': [9.5, 8],
            ...      'employed': [False, True],
            ...       'kids': [0, 0]}
            >>> df2 = pd.DataFrame(data=d2)
            >>> df2
                name  score  employed  kids
            0  Alice    9.5     False     0
            1    Bob    8.0      True     0

            >>> df2_transposed = df2.T  # or df2.transpose()
            >>> df2_transposed
                          0     1
            name      Alice   Bob
            score       9.5   8.0
            employed  False  True
            kids          0     0

            When the DataFrame has mixed dtypes, we get a transposed DataFrame with the object dtype:

            >>> df2.dtypes
            name         object
            score       float64
            employed       bool
            kids          int64
            dtype: object

            >>> df2_transposed.dtypes
            0    object
            1    object
            dtype: object
        """

    T = property(transpose)

    def add():
        """
        Get addition of ``DataFrame`` and `other`, element-wise (binary operator `add`).
        """

    def boxplot():
        """
        Make a box plot from ``DataFrame`` columns.
        """

    def combine():
        """
        Perform column-wise combine with another ``DataFrame``.
        """

    def compare():
        """
        Compare to another ``DataFrame`` and show the differences.
        """

    def corr():
        """
        Compute pairwise correlation of columns, excluding NA/null values.

        Parameters
        ----------
        method : {‘pearson’, ‘kendall’, ‘spearman’} or callable
            Method of correlation:
            pearson : standard correlation coefficient
            kendall : Kendall Tau correlation coefficient
            spearman : Spearman rank correlation
            callable: callable with input two 1d ndarrays
                and returning a float. Note that the returned matrix from corr will have 1 along the diagonals and will be symmetric regardless of the callable’s behavior.

        min_periods : int, optional
            Minimum number of observations required per pair of columns to have a valid result. Currently only available for Pearson and Spearman correlation.

        numeric_only : bool, default False
            Include only float, int or boolean data.

        Returns
        -------
        DataFrame
            Correlation matrix.

        See also
        --------
        DataFrame.corrwith
            Compute pairwise correlation with another DataFrame or Series.
        Series.corr
            Compute the correlation between two Series.

        Notes
        -----
        Pearson, Kendall and Spearman correlation are currently computed using pairwise complete observations.

            Pearson correlation coefficient
            Kendall rank correlation coefficient
            Spearman’s rank correlation coefficient

        Examples
        --------
        >>> def histogram_intersection(a, b):
        ...     v = np.minimum(a, b).sum().round(decimals=1)
        ...     return v
        >>> df = pd.DataFrame([(.2, .3), (.0, .6), (.6, .0), (.2, .1)],
        ...                   columns=['dogs', 'cats'])
        >>> df.corr(method=histogram_intersection)  # doctest: +SKIP
              dogs  cats
        dogs   1.0   0.3
        cats   0.3   1.0

        >>> df = pd.DataFrame([(1, 1), (2, np.nan), (np.nan, 3), (4, 4)],
        ...                   columns=['dogs', 'cats'])
        >>> df.corr(min_periods=3)
              dogs  cats
        dogs   1.0   1.0
        cats   1.0   1.0
        """

    def corrwith():
        """
        Compute pairwise correlation.
        """

    def cov():
        pass

    def dot():
        """
        Compute the matrix multiplication between the ``DataFrame`` and `other`.
        """

    def eq():
        """
        Perform equality comparison of ``DataFrame`` and `other` (binary operator `eq`).
        """

    def equals():
        """
        Test whether two dataframes contain the same elements.

        This function allows two DataFrames to be compared against
        each other to see if they have the same shape and elements. NaNs in
        the same location are considered equal.

        The row/column index do not need to have the same type, as long
        as the values are considered equal. Corresponding columns and
        index must be of the same dtype. Note: int variants (int8, int16 etc) are
        considered equal dtype i.e int8 == int16. Similarly, float variants (float32,
        float64 etc) are considered equal dtype.

        Parameters
        ----------
        other : DataFrame
            The other DataFrame to be compared with the first.

        Returns
        -------
        bool
            True if all elements are the same in both dataframes, False
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
        >>> df = pd.DataFrame({1: [10], 2: [20]})
        >>> df
            1   2
        0  10  20

        DataFrames df and exactly_equal have the same types and values for
        their elements and column labels, which will return True.

        >>> exactly_equal = pd.DataFrame({1: [10], 2: [20]})
        >>> exactly_equal
            1   2
        0  10  20
        >>> df.equals(exactly_equal)
        True

        DataFrames df and different_column_type have the same element
        types and values, but have different types for the column labels,
        which will still return True.

        >>> different_column_type = pd.DataFrame({1.0: [10], 2.0: [20]})
        >>> different_column_type
           1.0  2.0
        0   10   20
        >>> df.equals(different_column_type)
        True

        DataFrames df and different_data_type have different types for the
        same values for their elements, and will return False even though
        their column labels are the same values and types.

        >>> different_data_type = pd.DataFrame({1: [10.0], 2: [20.0]})
        >>> different_data_type
              1     2
        0  10.0  20.0
        >>> df.equals(different_data_type)
        False
        """

    def eval():
        """
        Evaluate a string describing operations on ``DataFrame`` columns.
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

    def floordiv():
        """
        Get integer division of ``DataFrame`` and `other`, element-wise (binary operator `floordiv`).
        """

    @classmethod
    def from_dict():
        """
        Construct ``DataFrame`` from dict of array-like or dicts.
        """

    def from_records():
        """
        Convert structured or record ndarray to ``DataFrame``.
        """

    def ge():
        """
        Get greater than or equal comparison of ``DataFrame`` and `other`, element-wise (binary operator `ge`).
        """

    def gt():
        """
        Get greater than comparison of ``DataFrame`` and `other`, element-wise (binary operator `ge`).
        """

    def hist():
        """
        Make a histogram of the ``DataFrame``.
        """

    def info():
        """
        Print a concise summary of the ``DataFrame``. Snowflake
        DataFrames mirror the output of pandas df.info but with some
        specific limitations ( zeroed memory usage, no index information ).

        Parameters
        ----------
        verbose : bool, optional
            Whether to print the full summary. By default, the setting in
            ``pandas.options.display.max_info_columns`` is followed.
        buf : writable buffer, defaults to sys.stdout
            Where to send the output. By default, the output is printed to
            sys.stdout. Pass a writable buffer if you need to further process
            the output.
        max_cols : int, optional
            When to switch from the verbose to the truncated output. If the
            DataFrame has more than `max_cols` columns, the truncated output
            is used. By default, the setting in
            ``pandas.options.display.max_info_columns`` is used.
        memory_usage : bool, str, optional
            Displays 0 for memory usage, since the memory usage of a snowflake
            dataframe is remote and partially indeterminant.
        show_counts : bool, optional
            Whether to show the non-null counts. By default, this is shown
            only if the DataFrame is smaller than
            ``pandas.options.display.max_info_rows`` and
            ``pandas.options.display.max_info_columns``. A value of True always
            shows the counts, and False never shows the counts.

        Returns
        -------
        None
            This method prints a summary of a DataFrame and returns None.

        Examples
        --------
            >>> df = pd.DataFrame({'COL1': [1, 2, 3],
            ...                    'COL2': ['A', 'B', 'C']})

            >>> df.info() # doctest: +NORMALIZE_WHITESPACE
            <class 'snowflake.snowpark.modin.pandas.dataframe.DataFrame'>
            SnowflakeIndex
            Data columns (total 2 columns):
             #   Column  Non-Null Count  Dtype
            ---  ------  --------------  -----
             0   COL1    3 non-null      int64
             1   COL2    3 non-null      object
            dtypes: int64(1), object(1)
            memory usage: 0.0 bytes
        """

    def insert():
        """
        Insert column into DataFrame at specified location.

        Raises a ValueError if `column` is already contained in the DataFrame,
        unless `allow_duplicates` is set to True.

        Parameters
        ----------
        loc : int
            Insertion index. Must verify 0 <= loc <= len(columns).
        column : str, number, or hashable object
            Label of the inserted column.
        value : Scalar, Series, or array-like
        allow_duplicates : bool, optional, default lib.no_default

        See Also
        --------
        Index.insert : Insert new item by index.

        Examples
        --------
        >>> df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        >>> df
           col1  col2
        0     1     3
        1     2     4
        >>> df.insert(1, "newcol", [99, 99])
        >>> df
           col1  newcol  col2
        0     1      99     3
        1     2      99     4
        >>> df.insert(0, "col1", [100, 100], allow_duplicates=True)
        >>> df
           col1  col1  newcol  col2
        0   100     1      99     3
        1   100     2      99     4

        Notice that pandas uses index alignment in case of `value` from type `Series`:

        >>> df.insert(0, "col0", pd.Series([5, 6], index=[1, 2]))
        >>> df
           col0  col1  col1  newcol  col2
        0   NaN   100     1      99     3
        1   5.0   100     2      99     4
        """

    def interpolate():
        pass

    def iterrows():
        """
        Iterate over ``DataFrame`` rows as (index, ``Series``) pairs.

        Yields
        ------
        index : label or tuple of label
            The index of the row. A tuple for a `MultiIndex`.
        data : Series
            The data of the row as a Series.

        See Also
        --------
        DataFrame.itertuples : Iterate over DataFrame rows as namedtuples of the values.
        DataFrame.items : Iterate over (column name, Series) pairs.

        Notes
        -----
        1. Iterating over rows is an antipattern in Snowpark pandas and pandas. Use df.apply() or other aggregation
           methods when possible instead of iterating over a DataFrame. Iterators and for loops do not scale well.
        2. Because ``iterrows`` returns a Series for each row, it does **not** preserve dtypes across the rows (dtypes
           are preserved across columns for DataFrames).
        3. You should **never modify** something you are iterating over. This will not work. The iterator returns a copy
           of the data and writing to it will have no effect.

        Examples
        --------
        >>> df = pd.DataFrame([[1, 1.5], [2, 2.5], [3, 7.8]], columns=['int', 'float'])
        >>> df
           int  float
        0    1    1.5
        1    2    2.5
        2    3    7.8

        Print the first row's index and the row as a Series.
        >>> index_and_row = next(df.iterrows())
        >>> index_and_row
        (0, int      1.0
        float    1.5
        Name: 0, dtype: float64)

        Print the first row as a Series.
        >>> row = next(df.iterrows())[1]
        >>> row
        int      1.0
        float    1.5
        Name: 0, dtype: float64

        Pretty printing every row.
        >>> for row in df.iterrows():
        ...     print(row[1])
        ...
        int      1.0
        float    1.5
        Name: 0, dtype: float64
        int      2.0
        float    2.5
        Name: 1, dtype: float64
        int      3.0
        float    7.8
        Name: 2, dtype: float64

        >>> df = pd.DataFrame([[0, 2, 3], [0, 4, 1]], columns=['A', 'B', 'C'])
        >>> df
           A  B  C
        0  0  2  3
        1  0  4  1

        Pretty printing the results to distinguish index and Series.
        >>> for row in df.iterrows():
        ...     print(f"Index: {row[0]}")
        ...     print("Series:")
        ...     print(row[1])
        ...
        Index: 0
        Series:
        A    0
        B    2
        C    3
        Name: 0, dtype: int64
        Index: 1
        Series:
        A    0
        B    4
        C    1
        Name: 1, dtype: int64
        """

    def items():
        """
        Iterate over (column name, ``Series``) pairs.
        """

    def iteritems():
        """
        Iterate over (column name, ``Series``) pairs.
        """

    def itertuples():
        """
        Iterate over DataFrame rows as namedtuples.

        Parameters
        ----------
        index : bool, default True
            If True, return the index as the first element of the tuple.
        name : str or None, default "Pandas"
            The name of the returned namedtuples or None to return regular tuples.

        Returns
        -------
        iterator
            An object to iterate over namedtuples for each row in the DataFrame with the first field possibly being the
            index and following fields being the column values.

        See Also
        --------
        DataFrame.iterrows : Iterate over DataFrame rows as (index, Series) pairs.
        DataFrame.items : Iterate over (column name, Series) pairs.

        Notes
        -----
        1. Iterating over rows is an antipattern in Snowpark pandas and pandas. Use df.apply() or other aggregation
           methods when possible instead of iterating over a DataFrame. Iterators and for loops do not scale well.
        2. The column names will be renamed to positional names if they are invalid Python identifiers, repeated, or
           start with an underscore (follows namedtuple rules).

        Examples
        --------
        >>> df = pd.DataFrame({'num_legs': [4, 2], 'num_wings': [0, 2]}, index=['dog', 'hawk'])
        >>> df
              num_legs  num_wings
        dog          4          0
        hawk         2          2
        >>> for row in df.itertuples():
        ...     print(row)
        ...
        Pandas(Index='dog', num_legs=4, num_wings=0)
        Pandas(Index='hawk', num_legs=2, num_wings=2)

        By setting the `index` parameter to False we can remove the index as the first element of the tuple:
        >>> for row in df.itertuples(index=False):
        ...     print(row)
        ...
        Pandas(num_legs=4, num_wings=0)
        Pandas(num_legs=2, num_wings=2)

        >>> df = pd.DataFrame([[1, 2], [4, 5], [7, 8]],
        ...      index=['cobra', 'viper', 'sidewinder'],
        ...      columns=['max_speed', 'shield'])
        >>> df
                    max_speed  shield
        cobra               1       2
        viper               4       5
        sidewinder          7       8

        >>> for row in df.itertuples():
        ...     print(row)
        ...
        Pandas(Index='cobra', max_speed=1, shield=2)
        Pandas(Index='viper', max_speed=4, shield=5)
        Pandas(Index='sidewinder', max_speed=7, shield=8)

        Rename the namedtuple and create it without the index values.
        >>> for row in df.itertuples(name="NewName", index=False):
        ...     print(row)
        ...
        NewName(max_speed=1, shield=2)
        NewName(max_speed=4, shield=5)
        NewName(max_speed=7, shield=8)

        When name is None, return a regular tuple.
        >>> for row in df.itertuples(name=None):
        ...     print(row)
        ...
        ('cobra', 1, 2)
        ('viper', 4, 5)
        ('sidewinder', 7, 8)
        """

    def join():
        """
        Join columns of another DataFrame.

        Join columns with `other` DataFrame either on index or on a key
        column. Efficiently join multiple DataFrame objects by index at once by
        passing a list.

        Parameters
        ----------
        other : DataFrame, Series, or a list containing any combination of them
            Index should be similar to one of the columns in this one. If a
            Series is passed, its name attribute must be set, and that will be
            used as the column name in the resulting joined DataFrame.
        on : str, list of str, or array-like, optional
            Column or index level name(s) in the caller to join on the index
            in `other`, otherwise joins index-on-index. If multiple
            values given, the `other` DataFrame must have a MultiIndex. Can
            pass an array as the join key if it is not already contained in
            the calling DataFrame. Like an Excel VLOOKUP operation.
        how : {'left', 'right', 'outer', 'inner'}, default 'left'
            How to handle the operation of the two objects.

            * left: use calling frame's index (or column if on is specified)
            * right: use `other`'s index.
            * outer: form union of calling frame's index (or column if on is
              specified) with `other`'s index, and sort it.
              lexicographically.
            * inner: form intersection of calling frame's index (or column if
              on is specified) with `other`'s index, preserving the order
              of the calling's one.
            * cross: creates the cartesian product from both frames, preserves the order
              of the left keys.
        lsuffix : str, default ''
            Suffix to use from left frame's overlapping columns.
        rsuffix : str, default ''
            Suffix to use from right frame's overlapping columns.
        sort : bool, default False
            Order result DataFrame lexicographically by the join key. If False,
            the order of the join key depends on the join type (how keyword).
        validate : str, optional
            If specified, checks if join is of specified type.
            * "one_to_one" or "1:1": check if join keys are unique in both left
            and right datasets.
            * "one_to_many" or "1:m": check if join keys are unique in left dataset.
            * "many_to_one" or "m:1": check if join keys are unique in right dataset.
            * "many_to_many" or "m:m": allowed, but does not result in checks.

        Returns
        -------
        DataFrame
            A dataframe containing columns from both the caller and `other`.

        Notes
        -----
        Parameters `on`, `lsuffix`, and `rsuffix` are not supported when
        passing a list of `DataFrame` objects.

        Snowpark pandas API doesn't currently support distributed computation of join with
        'validate' argument.

        Examples
        --------
            >>> df = pd.DataFrame({'key': ['K0', 'K1', 'K2', 'K3', 'K4', 'K5'],
            ...                    'A': ['A0', 'A1', 'A2', 'A3', 'A4', 'A5']})

            >>> df
              key   A
            0  K0  A0
            1  K1  A1
            2  K2  A2
            3  K3  A3
            4  K4  A4
            5  K5  A5

            >>> other = pd.DataFrame({'key': ['K0', 'K1', 'K2'],
            ...                       'B': ['B0', 'B1', 'B2']})

            >>> other
              key   B
            0  K0  B0
            1  K1  B1
            2  K2  B2

            Join DataFrames using their indexes.

            >>> df.join(other, lsuffix='_caller', rsuffix='_other')
              key_caller   A key_other     B
            0         K0  A0        K0    B0
            1         K1  A1        K1    B1
            2         K2  A2        K2    B2
            3         K3  A3      None  None
            4         K4  A4      None  None
            5         K5  A5      None  None

            If we want to join using the key columns, we need to set key to be
            the index in both `df` and `other`. The joined DataFrame will have
            key as its index.

            >>> df.set_index('key').join(other.set_index('key'))  # doctest: +NORMALIZE_WHITESPACE
                  A     B
            key
            K0   A0    B0
            K1   A1    B1
            K2   A2    B2
            K3   A3  None
            K4   A4  None
            K5   A5  None

            Another option to join using the key columns is to use the `on`
            parameter. DataFrame.join always uses `other`'s index but we can use
            any column in `df`. This method preserves the original DataFrame's
            index in the result.

            >>> df.join(other.set_index('key'), on='key')
              key   A     B
            0  K0  A0    B0
            1  K1  A1    B1
            2  K2  A2    B2
            3  K3  A3  None
            4  K4  A4  None
            5  K5  A5  None

            Using non-unique key values shows how they are matched.

            >>> df = pd.DataFrame({'key': ['K0', 'K1', 'K1', 'K3', 'K0', 'K1'],
            ...                    'A': ['A0', 'A1', 'A2', 'A3', 'A4', 'A5']})

            >>> df
              key   A
            0  K0  A0
            1  K1  A1
            2  K1  A2
            3  K3  A3
            4  K0  A4
            5  K1  A5

            TODO: SNOW-890653 Enable this test

            >>> df.join(other.set_index('key'), on='key', validate='m:1')  # doctest: +SKIP
              key   A    B
            0  K0  A0   B0
            1  K1  A1   B1
            2  K1  A2   B1
            3  K3  A3  NaN
            4  K0  A4   B0
            5  K1  A5   B1
        """

    def isna():
        """
        Detect missing values.

        Return a boolean same-sized object indicating if the values are NA. NA values, such as None
        or `numpy.NaN`, gets mapped to True values. Everything else gets mapped to False values.
        Characters such as empty strings `''` or `numpy.inf` are not considered NA values.

        Returns
        -------
        DataFrame
            Mask of bool values for each element in DataFrame that indicates whether an element is an NA value.

        Examples
        --------
        >>> df = pd.DataFrame(dict(age=[5, 6, np.nan],
        ...                   born=[pd.NaT, pd.Timestamp('1939-05-27'),
        ...                         pd.Timestamp('1940-04-25')],
        ...                   name=['Alfred', 'Batman', ''],
        ...                   toy=[None, 'Batmobile', 'Joker']))
        >>> df
           age       born    name        toy
        0  5.0        NaT  Alfred       None
        1  6.0 1939-05-27  Batman  Batmobile
        2  NaN 1940-04-25              Joker

        >>> df.isna()
             age   born   name    toy
        0  False   True  False   True
        1  False  False  False  False
        2   True  False  False  False
        """

    def isnull():
        """
        `DataFrame.isnull` is an alias for `DataFrame.isna`.

        Detect missing values.

        Return a boolean same-sized object indicating if the values are NA. NA values, such as None
        or `numpy.NaN`, gets mapped to True values. Everything else gets mapped to False values.
        Characters such as empty strings `''` or `numpy.inf` are not considered NA values.

        Returns
        -------
        DataFrame
            Mask of bool values for each element in DataFrame that indicates whether an element is an NA value.

        Examples
        --------
        >>> df = pd.DataFrame(dict(age=[5, 6, np.nan],
        ...                   born=[pd.NaT, pd.Timestamp('1939-05-27'),
        ...                         pd.Timestamp('1940-04-25')],
        ...                   name=['Alfred', 'Batman', ''],
        ...                   toy=[None, 'Batmobile', 'Joker']))
        >>> df
           age       born    name        toy
        0  5.0        NaT  Alfred       None
        1  6.0 1939-05-27  Batman  Batmobile
        2  NaN 1940-04-25              Joker

        >>> df.isna()
             age   born   name    toy
        0  False   True  False   True
        1  False  False  False  False
        2   True  False  False  False
        """

    def isetitem():
        pass

    def le():
        """
        Get less than or equal comparison of ``DataFrame`` and `other`, element-wise (binary operator `le`).
        """

    def lt():
        """
        Get less than comparison of ``DataFrame`` and `other`, element-wise (binary operator `le`).
        """

    def melt():
        """
        Unpivot a ``DataFrame`` from wide to long format, optionally leaving identifiers set.

        Parameters
        ----------
        id_vars : list of identifiers to retain in the result
        value_vars : list of columns to unpivot on
               defaults to all columns, excluding the id_vars columns
        var_name : variable name, defaults to "variable"
        value_name : value name, defaults to "value"
        col_level : int, not implemented
        ignore_index : bool

        Returns
        -------
            DataFrame
                unpivoted on the value columns

        Examples
        --------
        >>> df = pd.DataFrame({'A': {0: 'a', 1: 'b', 2: 'c'},
        ...           'B': {0: 1, 1: 3, 2: 5},
        ...           'C': {0: 2, 1: 4, 2: 6}})
        >>> df
           A  B  C
        0  a  1  2
        1  b  3  4
        2  c  5  6

        >>> df.melt()
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
        >>> df.melt(id_vars=['A'], value_vars=['B'], var_name='myVarname', value_name='myValname')
           A myVarname  myValname
        0  a         B          1
        1  b         B          3
        2  c         B          5

        """

    def memory_usage():
        """
        Return the memory usage of each column in bytes.
        """

    def mod():
        """
        Get modulo of ``DataFrame`` and `other`, element-wise (binary operator `mod`).
        """

    def mul():
        """
        Get multiplication of ``DataFrame`` and `other`, element-wise (binary operator `mul`).
        """

    multiply = mul

    def rmul():
        """
        Get multiplication of ``DataFrame`` and `other`, element-wise (binary operator `mul`).
        """

    def ne():
        """
        Get not equal comparison of ``DataFrame`` and `other`, element-wise (binary operator `ne`).
        """

    def nlargest():
        """
        Return the first `n` rows ordered by `columns` in descending order.

        Return the first `n` rows with the largest values in `columns`, in
        descending order. The columns that are not specified are returned as
        well, but not used for ordering.

        This method is equivalent to
        ``df.sort_values(columns, ascending=False).head(n)``

        Parameters
        ----------
        n : int
            Number of rows to return.
        columns : label or list of labels
            Column label(s) to order by.
        keep : {'first', 'last', 'all'}, default 'first'
            Where there are duplicate values:

            - ``first`` : prioritize the first occurrence(s)
            - ``last`` : prioritize the last occurrence(s)
            - ``all`` : keep all the ties of the smallest item even if it means
              selecting more than ``n`` items.

        Returns
        -------
        DataFrame
            The first `n` rows ordered by the given columns in descending
            order.

        See Also
        --------
        DataFrame.nsmallest : Return the first `n` rows ordered by `columns` in
            ascending order.
        DataFrame.sort_values : Sort DataFrame by the values.
        DataFrame.head : Return the first `n` rows without re-ordering.

        Examples
        --------
        >>> df = pd.DataFrame({'population': [59000000, 65000000, 434000,
        ...                                   434000, 434000, 337000, 11300,
        ...                                   11300, 11300],
        ...                    'GDP': [1937894, 2583560 , 12011, 4520, 12128,
        ...                            17036, 182, 38, 311],
        ...                    'alpha-2': ["IT", "FR", "MT", "MV", "BN",
        ...                                "IS", "NR", "TV", "AI"]},
        ...                   index=["Italy", "France", "Malta",
        ...                          "Maldives", "Brunei", "Iceland",
        ...                          "Nauru", "Tuvalu", "Anguilla"])
        >>> df
                  population      GDP alpha-2
        Italy       59000000  1937894      IT
        France      65000000  2583560      FR
        Malta         434000    12011      MT
        Maldives      434000     4520      MV
        Brunei        434000    12128      BN
        Iceland       337000    17036      IS
        Nauru          11300      182      NR
        Tuvalu         11300       38      TV
        Anguilla       11300      311      AI

        In the following example, we will use ``nlargest`` to select the three
        rows having the largest values in column "population".

        >>> df.nlargest(3, 'population')
                population      GDP alpha-2
        France    65000000  2583560      FR
        Italy     59000000  1937894      IT
        Malta       434000    12011      MT

        When using ``keep='last'``, ties are resolved in reverse order:

        >>> df.nlargest(3, 'population', keep='last')
                population      GDP alpha-2
        France    65000000  2583560      FR
        Italy     59000000  1937894      IT
        Brunei      434000    12128      BN

        When using ``keep='all'``, the number of element kept can go beyond ``n``
        if there are duplicate values for the smallest element, all the
        ties are kept:

        >>> df.nlargest(3, 'population', keep='all')  # doctest: +SKIP
                  population      GDP alpha-2
        France      65000000  2583560      FR
        Italy       59000000  1937894      IT
        Malta         434000    12011      MT
        Maldives      434000     4520      MV
        Brunei        434000    12128      BN

        However, ``nlargest`` does not keep ``n`` distinct largest elements:

        >>> df.nlargest(5, 'population', keep='all')  # doctest: +SKIP
                  population      GDP alpha-2
        France      65000000  2583560      FR
        Italy       59000000  1937894      IT
        Malta         434000    12011      MT
        Maldives      434000     4520      MV
        Brunei        434000    12128      BN

        To order by the largest values in column "population" and then "GDP",
        we can specify multiple columns like in the next example.

        >>> df.nlargest(3, ['population', 'GDP'])
                population      GDP alpha-2
        France    65000000  2583560      FR
        Italy     59000000  1937894      IT
        Brunei      434000    12128      BN
        """

    def nsmallest():
        """
        Return the first `n` rows ordered by `columns` in ascending order.

        Return the first `n` rows with the smallest values in `columns`, in
        ascending order. The columns that are not specified are returned as
        well, but not used for ordering.

        This method is equivalent to
        ``df.sort_values(columns, ascending=True).head(n)``

        Parameters
        ----------
        n : int
            Number of items to retrieve.
        columns : list or str
            Column name or names to order by.
        keep : {'first', 'last', 'all'}, default 'first'
            Where there are duplicate values:

            - ``first`` : take the first occurrence.
            - ``last`` : take the last occurrence.
            - ``all`` : keep all the ties of the largest item even if it means
              selecting more than ``n`` items.

        Returns
        -------
        DataFrame

        See Also
        --------
        DataFrame.nlargest : Return the first `n` rows ordered by `columns` in
            descending order.
        DataFrame.sort_values : Sort DataFrame by the values.
        DataFrame.head : Return the first `n` rows without re-ordering.

        Examples
        --------
        >>> df = pd.DataFrame({'population': [59000000, 65000000, 434000,
        ...                                   434000, 434000, 337000, 337000,
        ...                                   11300, 11300],
        ...                    'GDP': [1937894, 2583560 , 12011, 4520, 12128,
        ...                            17036, 182, 38, 311],
        ...                    'alpha-2': ["IT", "FR", "MT", "MV", "BN",
        ...                                "IS", "NR", "TV", "AI"]},
        ...                   index=["Italy", "France", "Malta",
        ...                          "Maldives", "Brunei", "Iceland",
        ...                          "Nauru", "Tuvalu", "Anguilla"])
        >>> df
                  population      GDP alpha-2
        Italy       59000000  1937894      IT
        France      65000000  2583560      FR
        Malta         434000    12011      MT
        Maldives      434000     4520      MV
        Brunei        434000    12128      BN
        Iceland       337000    17036      IS
        Nauru         337000      182      NR
        Tuvalu         11300       38      TV
        Anguilla       11300      311      AI

        In the following example, we will use ``nsmallest`` to select the
        three rows having the smallest values in column "population".

        >>> df.nsmallest(3, 'population')
                  population    GDP alpha-2
        Tuvalu         11300     38      TV
        Anguilla       11300    311      AI
        Iceland       337000  17036      IS

        When using ``keep='last'``, ties are resolved in reverse order:

        >>> df.nsmallest(3, 'population', keep='last')
                  population  GDP alpha-2
        Anguilla       11300  311      AI
        Tuvalu         11300   38      TV
        Nauru         337000  182      NR

        When using ``keep='all'``, the number of element kept can go beyond ``n``.
        if there are duplicate values for the largest element, all the
        ties are kept.

        >>> df.nsmallest(3, 'population', keep='all')  # doctest: +SKIP
                  population    GDP alpha-2
        Tuvalu         11300     38      TV
        Anguilla       11300    311      AI
        Iceland       337000  17036      IS
        Nauru         337000    182      NR

        However, ``nsmallest`` does not keep ``n`` distinct
        smallest elements:

        >>> df.nsmallest(4, 'population', keep='all')  # doctest: +SKIP
                  population    GDP alpha-2
        Tuvalu         11300     38      TV
        Anguilla       11300    311      AI
        Iceland       337000  17036      IS
        Nauru         337000    182      NR

        To order by the smallest values in column "population" and then "GDP", we can
        specify multiple columns like in the next example.

        >>> df.nsmallest(3, ['population', 'GDP'])
                  population  GDP alpha-2
        Tuvalu         11300   38      TV
        Anguilla       11300  311      AI
        Nauru         337000  182      NR
        """

    def unstack():
        """
        Pivot a level of the (necessarily hierarchical) index labels.
        """

    def pivot():
        """
        Return reshaped DataFrame organized by given index / column values.

        Reshape data (produce a "pivot" table) based on column values. Uses unique values from
        specified index / columns to form axes of the resulting DataFrame. This function does not
        support data aggregation, multiple values will result in a MultiIndex in the columns.

        Parameters
        ----------
        columns : str or object or a list of str
            Column to use to make new frame’s columns.
        index : str or object or a list of str, optional
            Column to use to make new frame’s index. If not given, uses existing index.
        values : str, object or a list of the previous, optional
            Column(s) to use for populating new frame’s values. If not specified, all remaining columns
            will be used and the result will have hierarchically indexed columns.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.DataFrame`

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
        >>> df.pivot(index='foo', columns='bar', values='baz')  # doctest: +NORMALIZE_WHITESPACE
        bar  A  B  C
        foo
        one  1  2  3
        two  4  5  6
        >>> df.pivot(index='foo', columns='bar')['baz']  # doctest: +NORMALIZE_WHITESPACE
        bar  A  B  C
        foo
        one  1  2  3
        two  4  5  6
        >>> df.pivot(index='foo', columns='bar', values=['baz', 'zoo'])  # doctest: +NORMALIZE_WHITESPACE
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
        >>> df.pivot(index="lev1", columns=["lev2", "lev3"], values="values")  # doctest: +NORMALIZE_WHITESPACE
        lev2  1       2
        lev3  1  2    1    2
        lev1
        1     0  1  2.0  NaN
        2     4  3  NaN  5.0
        >>> df.pivot(index=["lev1", "lev2"], columns=["lev3"], values="values")  # doctest: +NORMALIZE_WHITESPACE
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
            If True: only show observed values for categorical groupers.
            If False: show all values for categorical groupers.

        sort : bool, default True
            Specifies if the result should be sorted.

        Returns
        -------
        DataFrame
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

        >>> table = df.pivot_table(values='D', index=['A', 'B'],
        ...                        columns=['C'], aggfunc="sum")
        >>> table  # doctest: +NORMALIZE_WHITESPACE
        C        large  small
        A   B
        bar one    4.0      5
            two    7.0      6
        foo one    4.0      1
            two    NaN      6

        We can also fill missing values using the `fill_value` parameter.

        >>> table = df.pivot_table(values='D', index=['A', 'B'],
        ...                        columns=['C'], aggfunc="sum", fill_value=0)
        >>> table  # doctest: +NORMALIZE_WHITESPACE
        C        large  small
        A   B
        bar one    4.0      5
            two    7.0      6
        foo one    4.0      1
            two    NaN      6

        >>> table = df.pivot_table(values=['D', 'E'], index=['A', 'C'],
        ...                        aggfunc={'D': "mean", 'E': "mean"})
        >>> table  # doctest: +NORMALIZE_WHITESPACE
                          D         E
                          D         E
        A   C
        bar large  5.500000  7.500000
            small  5.500000  8.500000
        foo large  2.000000  4.500000
            small  2.333333  4.333333

        >>> table = df.pivot_table(values=['D', 'E'], index=['A', 'C'],
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

    def plot():
        """
        Make plots of ``DataFrame``.
        """

    def pow():
        """
        Get exponential power of ``DataFrame`` and `other`, element-wise (binary operator `pow`).

        Note:
            Native pandas doesn't allow ``a ** b``, where ``a`` is an integer and ``b``
            is a negative integer. However, Snowpark pandas API allows it and return the correct result.
            For example, ``pd.DataFrame([5]).pow(-7)`` is allowed, whereas it will raise an
            exception in native pandas.
        """

    def prod():
        """
        Return the product of the values over the requested axis.
        """

    product = prod

    def quantile():
        """
        Return values at the given quantile over requested axis.

        Parameters
        ----------
        q: float or array-like of float, default 0.5
            Value between 0 <= q <= 1, the quantile(s) to compute.
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

            Snowpark pandas currently only supports "linear" and "nearest".

        method: {"single", "table"}, default "single"
            Whether to compute quantiles per-column ("single") or over all columns ("table").
            When "table", the only allowed interpolation methods are "nearest", "lower", and "higher".

        Returns
        -------
        Series or DataFrame
            If ``q`` is an array, a DataFrame will be returned where the index is ``q``, the columns
            are the columns of ``self``, and the values are the quantiles.
            If ``q`` is a float, a Series will be returned where the index is the columns of
            ``self`` and the values are the quantiles.

        Examples
        --------
        >>> df = pd.DataFrame(np.array([[1, 1], [2, 10], [3, 100], [4, 100]]), columns=['a', 'b'])

        With a scalar q:

        >>> df.quantile(.1) # doctest: +NORMALIZE_WHITESPACE
        a    1.3
        b    3.7
        Name: 0.1, dtype: float64

        With a list q:

        >>> df.quantile([.1, .5]) # doctest: +NORMALIZE_WHITESPACE
               a     b
        0.1  1.3   3.7
        0.5  2.5  55.0

        Values considered NaN do not affect the result:

        >>> df = pd.DataFrame({"a": [None, 0, 25, 50, 75, 100, np.nan]})
        >>> df.quantile([0, 0.25, 0.5, 0.75, 1]) # doctest: +NORMALIZE_WHITESPACE
                  a
        0.00    0.0
        0.25   25.0
        0.50   50.0
        0.75   75.0
        1.00  100.0

        Notes
        -----
        Currently only supports calls with axis=0.

        Also, unsupported if q is a Snowpandas DataFrame or Series.
        """

    def query():
        """
        Query the columns of a ``DataFrame`` with a boolean expression.
        """

    def rename():
        """
        Rename columns or index labels.

        Function / dict values must be unique (1-to-1). Labels not contained in
        a dict / Series will be left as-is. Extra labels listed don't throw an
        error.

        Parameters
        ----------
        mapper : dict-like or function
            Dict-like or function transformations to apply to
            that axis' values. Use either ``mapper`` and ``axis`` to
            specify the axis to target with ``mapper``, or ``index`` and
            ``columns``.
        index : dict-like or function
            Alternative to specifying axis (``mapper, axis=0``
            is equivalent to ``index=mapper``).
        columns : dict-like or function
            Alternative to specifying axis (``mapper, axis=1``
            is equivalent to ``columns=mapper``).
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Axis to target with ``mapper``. Can be either the axis name
            ('index', 'columns') or number (0, 1). The default is 'index'.
        copy : bool, default True
            Also copy underlying data. copy has been ignored with Snowflake execution engine.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
            If True then value of copy is ignored.
        level : int or level name, default None
            In case of a MultiIndex, only rename labels in the specified
            level.
        errors : {'ignore', 'raise'}, default 'ignore'
            If 'raise', raise a `KeyError` when a dict-like `mapper`, `index`,
            or `columns` contains labels that are not present in the Index
            being transformed.
            If 'ignore', existing keys will be renamed and extra keys will be
            ignored.

        Returns
        -------
        DataFrame or None
            DataFrame with the renamed axis labels or None if ``inplace=True``.

        Raises
        ------
        KeyError
            If any of the labels is not found in the selected axis and
            "errors='raise'".

        See Also
        --------
        DataFrame.rename_axis : Set the name of the axis.

        Examples
        --------
        ``DataFrame.rename`` supports two calling conventions

        * ``(index=index_mapper, columns=columns_mapper, ...)``
        * ``(mapper, axis={'index', 'columns'}, ...)``

        We *highly* recommend using keyword arguments to clarify your
        intent.

        Rename columns using a mapping:

        >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        >>> df.rename(columns={"A": "a", "B": "c"})
           a  c
        0  1  4
        1  2  5
        2  3  6

        Rename index using a mapping:

        >>> df.rename(index={0: "x", 1: "y", 2: "z"})
           A  B
        x  1  4
        y  2  5
        z  3  6

        Cast index labels to a different type:

        >>> df.index
        Index([0, 1, 2], dtype='int64')

        >>> df.rename(columns={"A": "a", "B": "b", "C": "c"}, errors="raise")
        Traceback (most recent call last):
          ...
        KeyError: "['C'] not found in axis"

        Using axis-style parameters:

        >>> df.rename(str.lower, axis='columns')
           a  b
        0  1  4
        1  2  5
        2  3  6

        >>> df.rename({1: 2, 2: 4}, axis='index')
           A  B
        0  1  4
        2  2  5
        4  3  6
        """

    def reindex():
        """
        Conform DataFrame to new index with optional filling logic.

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
        DataFrame
            DataFrame with changed index.

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

        >>> df = pd.DataFrame({'A': [0, 1, 2, 3, 4], 'B': [5, 6, 7, 8, 9]})
        >>> df.replace(0, 5)
           A  B
        0  5  5
        1  1  6
        2  2  7
        3  3  8
        4  4  9

        **List-like `to_replace`**

        >>> df.replace([0, 1, 2, 3], 4)
           A  B
        0  4  5
        1  4  6
        2  4  7
        3  4  8
        4  4  9

        >>> df.replace([0, 1, 2, 3], [4, 3, 2, 1])
           A  B
        0  4  5
        1  3  6
        2  2  7
        3  1  8
        4  4  9

        **dict-like `to_replace`**

        >>> df.replace({0: 10, 1: 100})
             A  B
        0   10  5
        1  100  6
        2    2  7
        3    3  8
        4    4  9

        >>> df = pd.DataFrame({'A': [0, 1, 2, 3, 4], 'B': [5, 6, 7, 8, 9], 'C': ['a', 'b', 'c', 'd', 'e']})
        >>> df.replace({'A': 0, 'B': 5}, 100)
             A    B  C
        0  100  100  a
        1    1    6  b
        2    2    7  c
        3    3    8  d
        4    4    9  e

        >>> df.replace({'A': {0: 100, 4: 400}})
             A  B  C
        0  100  5  a
        1    1  6  b
        2    2  7  c
        3    3  8  d
        4  400  9  e

        **Regular expression `to_replace`**

        >>> df = pd.DataFrame({'A': ['bat', 'foo', 'bait'],
        ...                    'B': ['abc', 'bar', 'xyz']})
        >>> df.replace(to_replace=r'^ba.$', value='new', regex=True)
              A    B
        0   new  abc
        1   foo  new
        2  bait  xyz

        >>> df.replace({'A': r'^ba.$'}, {'A': 'new'}, regex=True)
              A    B
        0   new  abc
        1   foo  bar
        2  bait  xyz

        >>> df.replace(regex=r'^ba.$', value='new')
              A    B
        0   new  abc
        1   foo  new
        2  bait  xyz

        >>> df.replace(regex={r'^ba.$': 'new', 'foo': 'xyz'})
              A    B
        0   new  abc
        1   xyz  new
        2  bait  xyz

        >>> df.replace(regex=[r'^ba.$', 'foo'], value='new')
              A    B
        0   new  abc
        1   new  new
        2  bait  xyz

        When ``regex=True``, ``value`` is not ``None`` and `to_replace` is a string,
        the replacement will be applied in all columns of the DataFrame.

        >>> df = pd.DataFrame({'A': [0, 1, 2, 3, 4],
        ...                    'B': ['a', 'b', 'c', 'd', 'e'],
        ...                    'C': ['f', 'g', 'h', 'i', 'j']})

        >>> df.replace(to_replace='^[a-g]', value='e', regex=True)
             A  B  C
        0  0.0  e  e
        1  1.0  e  e
        2  2.0  e  h
        3  3.0  e  i
        4  4.0  e  j

        If ``value`` is not ``None`` and `to_replace` is a dictionary, the dictionary
        keys will be the DataFrame columns that the replacement will be applied.

        >>> df.replace(to_replace={'B': '^[a-c]', 'C': '^[h-j]'}, value='e', regex=True)
           A  B  C
        0  0  e  f
        1  1  e  g
        2  2  e  e
        3  3  d  e
        4  4  e  e
        """

    def rfloordiv():
        """
        Get integer division of ``DataFrame`` and `other`, element-wise (binary operator `rfloordiv`).
        """

    def radd():
        """
        Get addition of ``DataFrame`` and `other`, element-wise (binary operator `radd`).
        """

    def rmod():
        """
        Get modulo of ``DataFrame`` and `other`, element-wise (binary operator `rmod`).
        """

    def round():
        """
        Round a DataFrame to a variable number of decimal places.

        Parameters
        ----------
        decimals : int, dict, Series
            Number of decimal places to round each column to. If an int is given, round each column to the same number of places. Otherwise dict and Series round to variable numbers of places. Column names should be in the keys if decimals is a dict-like, or in the index if decimals is a Series. Any columns not included in decimals will be left as is. Elements of decimals which are not columns of the input will be ignored.
        *args
            Additional keywords have no effect but might be accepted for compatibility with numpy.
        **kwargs
            Additional keywords have no effect but might be accepted for compatibility with numpy.

        Returns
        -------
        DataFrame
            A DataFrame with the affected columns rounded to the specified number of decimal places.

        See Also
        --------
        numpy.around : Round a numpy array to the given number of decimals.
        Series.round : Round a Series to the given number of decimals.

        Examples
        --------
        >>> df = pd.DataFrame([(.21, .32), (.01, .67), (.66, .03), (.21, .18)], columns=['dogs', 'cats'])
        >>> df
           dogs  cats
        0  0.21  0.32
        1  0.01  0.67
        2  0.66  0.03
        3  0.21  0.18

        By providing an integer each column is rounded to the same number of decimal places

        >>> df.round(1)
           dogs  cats
        0   0.2   0.3
        1   0.0   0.7
        2   0.7   0.0
        3   0.2   0.2

        With a dict, the number of places for specific columns can be specified with the column names as key and the number of decimal places as value

        >>> df.round({'dogs': 1, 'cats': 0})
           dogs  cats
        0   0.2   0.0
        1   0.0   1.0
        2   0.7   0.0
        3   0.2   0.0
        """

    def rpow():
        """
        Get exponential power of ``DataFrame`` and `other`, element-wise (binary operator `rpow`).
        """

    def rsub():
        """
        Get subtraction of ``DataFrame`` and `other`, element-wise (binary operator `rsub`).
        """

    def rtruediv():
        """
        Get floating division of ``DataFrame`` and `other`, element-wise (binary operator `rtruediv`).
        """

    rdiv = rtruediv

    def select_dtypes():
        """
        Return a subset of the ``DataFrame``'s columns based on the column dtypes.
        At least one of the parameters must be specified, and the include/exclude lists must not overlap.

        Parameters
        ----------
        include : Optional[ListLike | type], default None
            A list of dtypes to include in the result.
        exclude : Optional[ListLike | type], default None
            A list of dtypes to exclude from the result.

        Returns
        -------
        DataFrame
            The subset of the frame including the dtypes in `include` and excluding the dtypes in
            `exclude`. If a column's dtype is a subtype of a type in both `include` and `exclude` (such
            as if `include=[np.number]` and `exclude=[int]`), then the column will be excluded.

        Examples
        --------
        >>> df = pd.DataFrame({'a': [1, 2] * 3,
        ...                    'b': [True, False] * 3,
        ...                    'c': [1.0, 2.0] * 3})

        >>> df.dtypes  # doctest: +NORMALIZE_WHITESPACE
        a      int64
        b       bool
        c    float64
        dtype: object

        Including all number columns:

        >>> df.select_dtypes("number")  # doctest: +NORMALIZE_WHITESPACE
           a    c
        0  1  1.0
        1  2  2.0
        2  1  1.0
        3  2  2.0
        4  1  1.0
        5  2  2.0

        Including only bool columns:

        >>> df.select_dtypes(include=[bool])  # doctest: +NORMALIZE_WHITESPACE
               b
        0   True
        1  False
        2   True
        3  False
        4   True
        5  False

        Excluding int columns:

        >>> df.select_dtypes(exclude=[int])  # doctest: +NORMALIZE_WHITESPACE
               b    c
        0   True  1.0
        1  False  2.0
        2   True  1.0
        3  False  2.0
        4   True  1.0
        5  False  2.0
        """

    def shift():
        """
        Shift data by desired number of periods along axis and replace columns with fill_value (default: None).

        Snowpark pandas does not support `freq` currently.

        Parameters
        ----------
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

        Returns
        -------
        DataFrame
            Copy of input object, shifted.

        Examples
        --------
        >>> df = pd.DataFrame({"Col1": [10, 20, 15, 30, 45],
        ...                    "Col2": [13, 23, 18, 33, 48],
        ...                    "Col3": [17, 27, 22, 37, 52]},
        ...                   index=pd.date_range("2020-01-01", "2020-01-05"))
        >>> df
                    Col1  Col2  Col3
        2020-01-01    10    13    17
        2020-01-02    20    23    27
        2020-01-03    15    18    22
        2020-01-04    30    33    37
        2020-01-05    45    48    52

        >>> df.shift(periods=3)
                    Col1  Col2  Col3
        2020-01-01   NaN   NaN   NaN
        2020-01-02   NaN   NaN   NaN
        2020-01-03   NaN   NaN   NaN
        2020-01-04  10.0  13.0  17.0
        2020-01-05  20.0  23.0  27.0

        >>> df.shift(periods=1, axis="columns")
                    Col1  Col2  Col3
        2020-01-01  None    10    13
        2020-01-02  None    20    23
        2020-01-03  None    15    18
        2020-01-04  None    30    33
        2020-01-05  None    45    48

        >>> df.shift(periods=3, fill_value=0)
                    Col1  Col2  Col3
        2020-01-01     0     0     0
        2020-01-02     0     0     0
        2020-01-03     0     0     0
        2020-01-04    10    13    17
        2020-01-05    20    23    27

        """

    def set_index():
        """
        Set the DataFrame index using existing columns.

        Set the DataFrame index (row labels) using one or more existing
        columns or arrays (of the correct length). The index can replace the
        existing index or expand on it.

        Parameters
        ----------
        keys : label or array-like or list of labels/arrays
            This parameter can be either a single column key, a single array of
            the same length as the calling DataFrame, or a list containing an
            arbitrary combination of column keys and arrays. Here, "array"
            encompasses :class:`Series`, :class:`Index`, ``np.ndarray``, and
            instances of :class:`~collections.abc.Iterator`.
        drop : bool, default True
            Delete columns to be used as the new index.
        append : bool, default False
            Whether to append columns to existing index.
        inplace : bool, default False
            Whether to modify the DataFrame rather than creating a new one.
        verify_integrity : bool, default False
            Check the new index for duplicates. Otherwise, defer the check until
            necessary. Setting to False will improve the performance of this
            method.

        Returns
        -------
        DataFrame or None
            Changed row labels or None if ``inplace=True``.

        See Also
        --------
        DataFrame.reset_index : Opposite of set_index.
        DataFrame.reindex : Change to new indices or expand indices.
        DataFrame.reindex_like : Change to same indices as other DataFrame.

        Examples
        --------
        >>> df = pd.DataFrame({'month': [1, 4, 7, 10],
        ...                    'year': [2012, 2014, 2013, 2014],
        ...                    'sale': [55, 40, 84, 31]})
        >>> df
           month  year  sale
        0      1  2012    55
        1      4  2014    40
        2      7  2013    84
        3     10  2014    31

        Set the index to become the 'month' column:

        >>> df.set_index('month')  # doctest: +NORMALIZE_WHITESPACE
               year  sale
        month
        1      2012    55
        4      2014    40
        7      2013    84
        10     2014    31

        Create a MultiIndex using columns 'year' and 'month':

        >>> df.set_index(['year', 'month'])  # doctest: +NORMALIZE_WHITESPACE
                    sale
        year month
        2012 1        55
        2014 4        40
        2013 7        84
        2014 10       31

        Create a MultiIndex using an Index and a column:

        >>> df.set_index([pd.Index([1, 2, 3, 4]), 'year']) # doctest: +NORMALIZE_WHITESPACE
                 month  sale
           year
        1  2012  1      55
        2  2014  4      40
        3  2013  7      84
        4  2014  10     31

        Create a MultiIndex using two Series:

        >>> s = pd.Series([1, 2, 3, 4])
        >>> df.set_index([s, s**2]) # doctest: +NORMALIZE_WHITESPACE
                month  year  sale
        1 1.0       1  2012    55
        2 4.0       4  2014    40
        3 9.0       7  2013    84
        4 16.0     10  2014    31
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
        >>> even_primes  # doctest: +SKIP
        0    2
        dtype: int64

        >>> even_primes.squeeze()  # doctest: +SKIP
        2

        Squeezing objects with more than one value in every axis does nothing:

        >>> odd_primes = primes[primes % 2 == 1]  # doctest: +SKIP
        >>> odd_primes  # doctest: +SKIP
        1    3
        2    5
        3    7
        dtype: int64

        >>> odd_primes.squeeze()  # doctest: +SKIP
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

    def stack():
        """
        Stack the prescribed level(s) from columns to index.

        Return a reshaped DataFrame or Series having a multi-level index with one
        or more new inner-most levels compared to the current DataFrame. The new inner-most
        levels are created by pivoting the columns of the current dataframe.
        If the columns have a single level, the output is a Series.
        If the columns have multiple levels, the new index level(s) is (are)
        taken from the prescribed level(s) and the output is a DataFrame.

        Parameters
        ----------
        level : int, str, list, default -1
            Level(s) to stack from the column axis onto the index axis,
            defined as one index or label, or a list of indices or labels.

        dropna : bool, default True
            Whether to drop rows in the resulting Frame/Series with missing values. Stacking a
            column level onto the index axis can create combinations of index and column values
            that are missing from the original dataframe.

        sort : bool, default True
            Whether to sort the levels of the resulting MultiIndex.

        future_stack : bool, default False
            This argument is ignored in Snowpark pandas.

        Returns
        -------
        DataFrame or Series
            Stacked dataframe or series.

        Notes
        -----
        level != -1 and MultiIndex dataframes are not yet supported by Snowpark pandas.

        See Also
        --------
        DataFrame.unstack : Unstack prescribed level(s) from index axis onto column axis.
        DataFrame.pivot : Reshape dataframe from long format to wide format.
        DataFrame.pivot_table : Create a spreadsheet-style pivot table as a DataFrame.

        Examples
        --------
        >>> df_single_level_cols = pd.DataFrame([[0, 1], [2, 3]], index=['cat', 'dog'], columns=['weight', 'height'])
        >>> df_single_level_cols
             weight  height
        cat       0       1
        dog       2       3
        >>> df_single_level_cols.stack()
        cat  weight    0
             height    1
        dog  weight    2
             height    3
        dtype: int64
        """

    def sub():
        """
        Get subtraction of ``DataFrame`` and `other`, element-wise (binary operator `sub`).
        """

    subtract = sub

    def to_feather():
        """
        Write a ``DataFrame`` to the binary Feather format.
        """

    def to_gbq():
        """
        Write a ``DataFrame`` to a Google BigQuery table.
        """

    def to_orc():
        pass

    def to_html():
        """
        Render a ``DataFrame`` as an HTML table.
        """

    def to_parquet():
        pass

    def to_period():
        """
        Convert ``DataFrame`` from ``DatetimeIndex`` to ``PeriodIndex``.
        """

    def to_records():
        """
        Convert ``DataFrame`` to a NumPy record array.
        """

    def to_stata():
        pass

    def to_xml():
        pass

    def to_dict():
        """
        Convert the DataFrame to a dictionary.
        Note that this method will pull the data to the client side.

        The type of the key-value pairs can be customized with the parameters
        (see below).

        Parameters
        ----------
        orient : str {'dict', 'list', 'series', 'split', 'tight', 'records', 'index'}
            Determines the type of the values of the dictionary.

            - 'dict' (default) : dict like {column -> {index -> value}}
            - 'list' : dict like {column -> [values]}
            - 'series' : dict like {column -> Series(values)}. Note that the result will be native pandas Series
            - 'split' : dict like
              {'index' -> [index], 'columns' -> [columns], 'data' -> [values]}
            - 'tight' : dict like
              {'index' -> [index], 'columns' -> [columns], 'data' -> [values],
              'index_names' -> [index.names], 'column_names' -> [column.names]}
            - 'records' : list like
              [{column -> value}, ... , {column -> value}]
            - 'index' : dict like {index -> {column -> value}}

        into : class, default dict
            The collections.abc.Mapping subclass used for all Mappings
            in the return value.  Can be the actual class or an empty
            instance of the mapping type you want.  If you want a
            collections.defaultdict, you must pass it initialized.

        Returns
        -------
        dict, list or collections.abc.Mapping
            Return a collections.abc.Mapping object representing the DataFrame.
            The resulting transformation depends on the `orient` parameter.

        Examples
        --------
        >>> df = pd.DataFrame({'col1': [1, 2],
        ...                    'col2': [0.5, 0.75]},
        ...                   index=['row1', 'row2'])
        >>> df
              col1  col2
        row1     1  0.50
        row2     2  0.75
        >>> df.to_dict()  # doctest: +NORMALIZE_WHITESPACE
        {'col1': {'row1': 1, 'row2': 2}, 'col2': {'row1': 0.5, 'row2': 0.75}}

        You can specify the return orientation.

        >>> df.to_dict('series')  # doctest: +NORMALIZE_WHITESPACE
        {'col1': row1    1
                 row2    2
        Name: col1, dtype: int64,
        'col2': row1    0.50
                row2    0.75
        Name: col2, dtype: float64}

        >>> df.to_dict('split')  # doctest: +NORMALIZE_WHITESPACE
        {'index': ['row1', 'row2'], 'columns': ['col1', 'col2'],
         'data': [[1, 0.5], [2, 0.75]]}

        >>> df.to_dict('records')  # doctest: +NORMALIZE_WHITESPACE
        [{'col1': 1, 'col2': 0.5}, {'col1': 2, 'col2': 0.75}]

        >>> df.to_dict('index')  # doctest: +NORMALIZE_WHITESPACE
        {'row1': {'col1': 1, 'col2': 0.5}, 'row2': {'col1': 2, 'col2': 0.75}}

        >>> df.to_dict('tight')  # doctest: +NORMALIZE_WHITESPACE
        {'index': ['row1', 'row2'], 'columns': ['col1', 'col2'],
         'data': [[1, 0.5], [2, 0.75]], 'index_names': [None], 'column_names': [None]}

        You can also specify the mapping type.

        >>> from collections import OrderedDict, defaultdict
        >>> df.to_dict(into=OrderedDict)  # doctest: +NORMALIZE_WHITESPACE
        OrderedDict([('col1', OrderedDict([('row1', 1), ('row2', 2)])),
                     ('col2', OrderedDict([('row1', 0.5), ('row2', 0.75)]))])

        If you want a `defaultdict`, you need to initialize it:

        >>> dd = defaultdict(list)
        >>> df.to_dict('records', into=dd)  # doctest: +NORMALIZE_WHITESPACE
        [defaultdict(<class 'list'>, {'col1': 1, 'col2': 0.5}),
         defaultdict(<class 'list'>, {'col1': 2, 'col2': 0.75})]
        """

    def to_timestamp():
        """
        Cast to DatetimeIndex of timestamps, at *beginning* of period.
        """

    def truediv():
        """
        Get floating division of ``DataFrame`` and `other`, element-wise (binary operator `truediv`).
        """

    div = divide = truediv

    def update():
        """
        Modify in place using non-NA values from another ``DataFrame``.
        """

    def diff():
        """
        First discrete difference of element.

        Calculates the difference of a DataFrame element compared with another element
        in the DataFrame (default is element in previous row).

        Parameters
        ----------
        periods : int, default 1
            Periods to shift for calculating difference, accepts negative values.

        axis : {0 or 'index', 1 or 'columns'}, default 0
            Take difference over rows (0) or columns (1).

        Returns
        -------
        DataFrame
            DataFrame with the first differences of the Series.

        Notes
        -----
        For boolean dtypes, this uses operator.xor() rather than operator.sub(). The result
        is calculated according to current dtype in DataFrame, however dtype of the result
        is always float64.

        Examples
        --------
        >>> df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6],
        ...                    'b': [1, 1, 2, 3, 5, 8],
        ...                    'c': [1, 4, 9, 16, 25, 36]})
        >>> df # doctest: +NORMALIZE_WHITESPACE
           a  b   c
        0  1  1   1
        1  2  1   4
        2  3  2   9
        3  4  3  16
        4  5  5  25
        5  6  8  36

        Difference with previous row

        >>> df.diff() # doctest: +NORMALIZE_WHITESPACE
            a    b     c
        0  NaN  NaN   NaN
        1  1.0  0.0   3.0
        2  1.0  1.0   5.0
        3  1.0  1.0   7.0
        4  1.0  2.0   9.0
        5  1.0  3.0  11.0

        Difference with previous column

        >>> df.diff(axis=1) # doctest: +NORMALIZE_WHITESPACE
            a   b   c
        0 None  0   0
        1 None -1   3
        2 None -1   7
        3 None -1  13
        4 None  0  20
        5 None  2  28

        Difference with 3rd previous row

        >>> df.diff(periods=3) # doctest: +NORMALIZE_WHITESPACE
            a    b     c
        0  NaN  NaN   NaN
        1  NaN  NaN   NaN
        2  NaN  NaN   NaN
        3  3.0  2.0  15.0
        4  3.0  4.0  21.0
        5  3.0  6.0  27.0

        Difference with following row

        >>> df.diff(periods=-1) # doctest: +NORMALIZE_WHITESPACE
            a    b     c
        0 -1.0  0.0  -3.0
        1 -1.0 -1.0  -5.0
        2 -1.0 -1.0  -7.0
        3 -1.0 -2.0  -9.0
        4 -1.0 -3.0 -11.0
        5  NaN  NaN   NaN
        """

    def drop():
        """
        Drop specified labels from rows or columns.

        Remove rows or columns by specifying label names and corresponding
        axis, or by specifying directly index or column names. When using a
        multi-index, labels on different levels can be removed by specifying
        the level. See the `user guide <advanced.shown_levels>`
        for more information about the now unused levels.

        Parameters
        ----------
        labels : single label or list-like
            Index or column labels to drop. A tuple will be used as a single
            label and not treated as a list-like.
        axis : {0 or 'index', 1 or 'columns'}, default 0
            Whether to drop labels from the index (0 or 'index') or
            columns (1 or 'columns').
        index : single label or list-like
            Alternative to specifying axis (``labels, axis=0``
            is equivalent to ``index=labels``).
        columns : single label or list-like
            Alternative to specifying axis (``labels, axis=1``
            is equivalent to ``columns=labels``).
        level : int or level name, optional
            For MultiIndex, level from which the labels will be removed.
        inplace : bool, default False
            If False, return a copy. Otherwise, do operation
            inplace and return None.
        errors : {'ignore', 'raise'}, default 'raise'
            If 'ignore', suppress error and only existing labels are
            dropped.

        Returns
        -------
        DataFrame or None
            DataFrame without the removed index or column labels or
            None if ``inplace=True``.

        Raises
        ------
        KeyError
            If any of the labels is not found in the selected axis.


        Examples
        --------
        >>> df = pd.DataFrame(np.arange(12).reshape(3, 4),
        ...                   columns=['A', 'B', 'C', 'D'])
        >>> df
           A  B   C   D
        0  0  1   2   3
        1  4  5   6   7
        2  8  9  10  11

        Drop columns

        >>> df.drop(['B', 'C'], axis=1)
           A   D
        0  0   3
        1  4   7
        2  8  11

        >>> df.drop(columns=['B', 'C'])
           A   D
        0  0   3
        1  4   7
        2  8  11

        Drop a row by index

        >>> df.drop([0, 1])
           A  B   C   D
        2  8  9  10  11

        Drop columns and/or rows of MultiIndex DataFrame

        >>> midx = pd.MultiIndex(levels=[['lama', 'cow', 'falcon'],
        ...                              ['speed', 'weight', 'length']],
        ...                      codes=[[0, 0, 0, 1, 1, 1, 2, 2, 2],
        ...                             [0, 1, 2, 0, 1, 2, 0, 1, 2]])
        >>> df = pd.DataFrame(index=midx, columns=['big', 'small'],
        ...                   data=[[45, 30], [200, 100], [1.5, 1], [30, 20],
        ...                         [250, 150], [1.5, 0.8], [320, 250],
        ...                         [1, 0.8], [0.3, 0.2]])
        >>> df
                         big  small
        lama   speed    45.0   30.0
               weight  200.0  100.0
               length    1.5    1.0
        cow    speed    30.0   20.0
               weight  250.0  150.0
               length    1.5    0.8
        falcon speed   320.0  250.0
               weight    1.0    0.8
               length    0.3    0.2

        Drop a specific index combination from the MultiIndex
        DataFrame, i.e., drop the combination ``'falcon'`` and
        ``'weight'``, which deletes only the corresponding row

        >>> df.drop(index=('falcon', 'weight'))
                         big  small
        lama   speed    45.0   30.0
               weight  200.0  100.0
               length    1.5    1.0
        cow    speed    30.0   20.0
               weight  250.0  150.0
               length    1.5    0.8
        falcon speed   320.0  250.0
               length    0.3    0.2

        >>> df.drop(index='cow', columns='small')
                         big
        lama   speed    45.0
               weight  200.0
               length    1.5
        falcon speed   320.0
               weight    1.0
               length    0.3

        >>> df.drop(index='length', level=1)
                         big  small
        lama   speed    45.0   30.0
               weight  200.0  100.0
        cow    speed    30.0   20.0
               weight  250.0  150.0
        falcon speed   320.0  250.0
               weight    1.0    0.8
        """

    def value_counts():
        """
        Return a Series containing the frequency of each distinct row in the Dataframe.

        Parameters
        ----------
        subset : label or list of labels, optional
            Columns to use when counting unique combinations.
        normalize : bool, default False
            Return proportions rather than frequencies. Being different from native pandas,
            Snowpark pandas will return a Series with `decimal.Decimal` values.
        sort : bool, default True
            Sort by frequencies when True. Sort by DataFrame column values when False.
            When there is a tie between counts, the order is still deterministic, but
            may be different from the result from native pandas.
        ascending : bool, default False
            Sort in ascending order.
        dropna : bool, default True
            Don't include counts of rows that contain NA values.

        Returns
        -------
        Series

        See Also
        --------
        :func:`Series.value_counts <snowflake.snowpark.modin.pandas.Series.value_counts>` : Equivalent method on Series.

        Notes
        -----
        The returned Series will have a MultiIndex with one level per input
        column but an Index (non-multi) for a single label. By default, rows
        that contain any NA values are omitted from the result. By default,
        the resulting Series will be in descending order so that the first
        element is the most frequently-occurring row.

        Examples
        --------
        >>> df = pd.DataFrame({'num_legs': [2, 4, 4, 6],
        ...                    'num_wings': [2, 0, 0, 0]},
        ...                   index=['falcon', 'dog', 'cat', 'ant'])
        >>> df
                num_legs  num_wings
        falcon         2          2
        dog            4          0
        cat            4          0
        ant            6          0

        >>> df.value_counts()
        num_legs  num_wings
        4         0            2
        2         2            1
        6         0            1
        Name: count, dtype: int64

        >>> df.value_counts(sort=False)
        num_legs  num_wings
        2         2            1
        4         0            2
        6         0            1
        Name: count, dtype: int64

        >>> df.value_counts(ascending=True)
        num_legs  num_wings
        2         2            1
        6         0            1
        4         0            2
        Name: count, dtype: int64

        >>> df.value_counts(normalize=True)
        num_legs  num_wings
        4         0            0.50
        2         2            0.25
        6         0            0.25
        Name: proportion, dtype: float64

        With `dropna` set to `False` we can also count rows with NA values.

        >>> df = pd.DataFrame({'first_name': ['John', 'Anne', 'John', 'Beth'],
        ...                    'middle_name': ['Smith', None, None, 'Louise']})
        >>> df
          first_name middle_name
        0       John       Smith
        1       Anne        None
        2       John        None
        3       Beth      Louise

        >>> df.value_counts()
        first_name  middle_name
        John        Smith          1
        Beth        Louise         1
        Name: count, dtype: int64

        >>> df.value_counts(dropna=False)
        first_name  middle_name
        John        Smith          1
        Anne        NaN            1
        John        NaN            1
        Beth        Louise         1
        Name: count, dtype: int64

        >>> df.value_counts("first_name")
        first_name
        John    2
        Anne    1
        Beth    1
        Name: count, dtype: int64
        """

    def mask():
        """
        Replace values where the condition is True.

        Args:
            cond: bool Series/DataFrame, array-like, or callable
                Where cond is False, keep the original value. Where True, replace with corresponding value from other.
                If cond is callable, it is computed on the Series/DataFrame and should return boolean Series/DataFrame
                or array. The callable must not change input Series/DataFrame (though pandas doesn’t check it).

            other: scalar, Series/DataFrame, or callable
                Entries where cond is True are replaced with corresponding value from other. If other is callable,
                it is computed on the Series/DataFrame and should return scalar or Series/DataFrame. The callable must
                not change input Series/DataFrame (though pandas doesn’t check it).

            inplace: bool, default False
                Whether to perform the operation in place on the data.

            axis: int, default None
                Alignment axis if needed. For Series this parameter is unused and defaults to 0.

            level: int, default None
                Alignment level if needed.

        Returns:
            Same type as caller or None if inplace=True.

        See Also:
            DataFrame.where : Replace values where the condition is False.

        Notes:
            The mask method is an application of the if-then idiom. For each element in the calling DataFrame, if cond
            is False the element is used; otherwise the corresponding element from the DataFrame other is used. If the
            axis of other does not align with axis of cond Series/DataFrame, the misaligned index positions will be
            filled with True.

            The signature for DataFrame.where() differs from numpy.where(). Roughly df1.where(m, df2) is equivalent to
            np.where(m, df1, df2).

            For further details and examples see the mask documentation in indexing.

            The dtype of the object takes precedence. The fill value is cast to the object’s dtype, if this can be
            done losslessly.

        Examples::
        >>> df = pd.DataFrame(np.arange(10).reshape(-1, 2), columns=['A', 'B'])
        >>> df   # doctest: +NORMALIZE_WHITESPACE
        A  B
        0  0  1
        1  2  3
        2  4  5
        3  6  7
        4  8  9

        >>> m = df % 3 == 0
        >>> df.mask(m, -df)   # doctest: +NORMALIZE_WHITESPACE
           A  B
        0  0  1
        1  2 -3
        2  4  5
        3 -6  7
        4  8 -9

        Snowpark pandas `DataFrame.mask` behaves the same as `numpy.where`.

        >>> data = np.where(~m, df, -df)
        >>> df.mask(m, -df) == pd.DataFrame(data, columns=['A', 'B'])  # doctest: +NORMALIZE_WHITESPACE
            A     B
        0  True  True
        1  True  True
        2  True  True
        3  True  True
        4  True  True

        >>> df.mask(m, -df) == df.where(~m, -df)  # doctest: +NORMALIZE_WHITESPACE
            A     B
        0  True  True
        1  True  True
        2  True  True
        3  True  True
        4  True  True
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

            The dtype of the object takes precedence. The fill value is cast to the object’s dtype, if this can be
            done losslessly.

        Examples::

        >>> df = pd.DataFrame(np.arange(10).reshape(-1, 2), columns=['A', 'B'])

        >>> df  # doctest: +NORMALIZE_WHITESPACE
           A  B
        0  0  1
        1  2  3
        2  4  5
        3  6  7
        4  8  9

        >>> m = df % 3 == 0
        >>> df.where(m, -df)  # doctest: +NORMALIZE_WHITESPACE
           A  B
        0  0 -1
        1 -2  3
        2 -4 -5
        3  6 -7
        4 -8  9

        Snowpark pandas `DataFrame.where` behaves the same as `numpy.where`.

        >>> data = np.where(m, df, -df)
        >>> df.where(m, -df) == pd.DataFrame(data, columns=['A', 'B'])  # doctest: +NORMALIZE_WHITESPACE
              A     B
        0  True  True
        1  True  True
        2  True  True
        3  True  True
        4  True  True

        >>> df.where(m, -df) == df.mask(~m, -df)  # doctest: +SKIP
              A     B
        0  True  True
        1  True  True
        2  True  True
        3  True  True
        4  True  True
        """

    def xs():
        """
        Return cross-section from the ``DataFrame``.
        """

    def set_axis():
        """
        Assign desired index to given axis.

        Parameters
        ----------
        labels : list-like, Index, MultiIndex
            The values for the new index.
        axis : {index (0), rows(0), columns (1)}
            Axis for the function to be applied on.
        copy : bool, default True
            To maintain compatibility with pandas, does nothing.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> df = pd.DataFrame({
        ... "Videogame": ["Dark Souls", "Cruelty Squad", "Stardew Valley"],
        ... "Genre": ["Souls-like", "Immersive-sim", "Farming-sim"],
        ... "Rating": [9.5, 9.0, 8.7]})
        >>> df.set_axis(['a', 'b', 'c'], axis="index") # doctest: +NORMALIZE_WHITESPACE
                Videogame          Genre  Rating
        a      Dark Souls     Souls-like     9.5
        b   Cruelty Squad  Immersive-sim     9.0
        c  Stardew Valley    Farming-sim     8.7

        >>> df.set_axis(["Name", "Sub-genre", "Rating out of 10"], axis=1) # doctest: +NORMALIZE_WHITESPACE
                     Name      Sub-genre  Rating out of 10
        0      Dark Souls     Souls-like               9.5
        1   Cruelty Squad  Immersive-sim               9.0
        2  Stardew Valley    Farming-sim               8.7

        >>> columns = pd.MultiIndex.from_tuples([("Gas", "Toyota"), ("Gas", "Ford"), ("Electric", "Tesla"), ("Electric", "Nio"),])
        >>> data = [[100, 300, 900, 400], [200, 500, 300, 600]]
        >>> df = pd.DataFrame(columns=columns, data=data)
        >>> df.set_axis([2010, 2015], axis="rows") # doctest: +NORMALIZE_WHITESPACE
                Gas      Electric
             Toyota Ford    Tesla  Nio
        2010    100  300      900  400
        2015    200  500      300  600
        """

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
        try to get `key` from ``DataFrame`` fields.
        """

    def __setattr__():
        """
        Set attribute `value` identified by `key`.

        Parameters
        ----------
        key : hashable
            Key to set.
        value : Any
            Value to set.
        """

    def __setitem__():
        """
        Set attribute `value` identified by `key`.

        Args:
            key: Key to set
            value:  Value to set

        Note:
            In the case where value is any list like or array, pandas checks the array length against the number of rows
            of the input dataframe. If there is a mismatch, a ValueError is raised. Snowpark pandas indexing won't throw
            a ValueError because knowing the length of the current dataframe can trigger eager evaluations; instead if
            the array is longer than the number of rows we ignore the additional values. If the array is shorter, we use
            enlargement filling with the last value in the array.

        Returns:
            None
        """

    def abs():
        """
        Return a DataFrame with absolute numeric value of each element.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> df = pd.DataFrame({'a': [1,-2,3], 'b': [-4.33, 5, 6]})
        >>> df
           a     b
        0  1 -4.33
        1 -2  5.00
        2  3  6.00

        >>> abs(df)
           a     b
        0  1  4.33
        1  2  5.00
        2  3  6.00
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

    def __neg__():
        """
        Returns a DataFrame with the sign changed for each element.

        Returns
        -------
        DataFrame

        Examples
        --------
        >>> df = pd.DataFrame({'a': [1,-2,3], 'b': [-4.33, 5, 6]})
        >>> df
           a     b
        0  1 -4.33
        1 -2  5.00
        2  3  6.00

        >>> - df
           a     b
        0 -1  4.33
        1  2 -5.00
        2 -3 -6.00
        """

    def __iter__():
        """
        Iterate over info axis.

        Returns
        -------
        iterable
            Iterator of the columns names.
        """

    def __contains__():
        """
        Check if `key` in the ``DataFrame.columns``.

        Parameters
        ----------
        key : hashable
            Key to check the presence in the columns.

        Returns
        -------
        bool
        """

    def __round__():
        """
        Round each value in a ``DataFrame`` to the given number of decimals.

        Parameters
        ----------
        decimals : int, default: 0
            Number of decimal places to round to.

        Returns
        -------
        DataFrame
        """

    def __delitem__():
        """
        Delete item identified by `key` label.

        Parameters
        ----------
        key : hashable
            Key to delete.
        """

    __add__ = add
    __iadd__ = add
    __radd__ = radd
    __mul__ = mul
    __imul__ = mul
    __rmul__ = rmul
    __pow__ = pow
    __ipow__ = pow
    __rpow__ = rpow
    __sub__ = sub
    __isub__ = sub
    __rsub__ = rsub
    __floordiv__ = floordiv
    __ifloordiv__ = floordiv
    __rfloordiv__ = rfloordiv
    __truediv__ = truediv
    __itruediv__ = truediv
    __rtruediv__ = rtruediv
    __mod__ = mod
    __imod__ = mod
    __rmod__ = rmod
    __rdiv__ = rdiv

    def __dataframe__():
        """
        Get a Modin DataFrame that implements the dataframe exchange protocol.

        See more about the protocol in https://data-apis.org/dataframe-protocol/latest/index.html.

        Parameters
        ----------
        nan_as_null : bool, default: False
            A keyword intended for the consumer to tell the producer
            to overwrite null values in the data with ``NaN`` (or ``NaT``).
            This currently has no effect; once support for nullable extension
            dtypes is added, this value should be propagated to columns.
        allow_copy : bool, default: True
            A keyword that defines whether or not the library is allowed
            to make a copy of the data. For example, copying data would be necessary
            if a library supports strided buffers, given that this protocol
            specifies contiguous buffers. Currently, if the flag is set to ``False``
            and a copy is needed, a ``RuntimeError`` will be raised.

        Returns
        -------
        ProtocolDataframe
            A dataframe object following the dataframe protocol specification.
        """

    @property
    def attrs():
        pass

    @property
    def style():
        pass

    def isin():
        """
        Whether each element in the DataFrame is contained in values.

        Parameters
        ----------
        values : list-like, Series, DataFrame or dict
            The result will only be true at a location if all the
            labels match. If `values` is a Series, that's the index. If
            `values` is a dict, the keys must be the column names,
            which must match. If `values` is a DataFrame,
            then both the index and column labels must match.

            Snowpark pandas assumes that in the case of values being Series or DataFrame that the index of
            values is unique (i.e., values.index.is_unique() = True)

        Returns
        -------
        DataFrame
            DataFrame of booleans showing whether each element in the DataFrame
            is contained in values.

        Examples
        --------
        >>> df = pd.DataFrame({'num_legs': [2, 4], 'num_wings': [2, 0]},
        ...                   index=['falcon', 'dog'])
        >>> df
                num_legs  num_wings
        falcon         2          2
        dog            4          0

        When ``values`` is a list check whether every value in the DataFrame
        is present in the list (which animals have 0 or 2 legs or wings)

        >>> df.isin([0, 2])
                num_legs  num_wings
        falcon      True       True
        dog        False       True

        To check if ``values`` is *not* in the DataFrame, use the ``~`` operator:

        >>> ~df.isin([0, 2])
                num_legs  num_wings
        falcon     False      False
        dog         True      False

        When ``values`` is a dict, we can pass values to check for each
        column separately:

        >>> df.isin({'num_wings': [0, 3]})
                num_legs  num_wings
        falcon     False      False
        dog        False       True

        When ``values`` is a Series or DataFrame the index and column must
        match. Note that 'falcon' does not match based on the number of legs
        in other.

        >>> other = pd.DataFrame({'num_legs': [8, 3], 'num_wings': [0, 2]},
        ...                      index=['spider', 'falcon'])
        >>> df.isin(other)
                num_legs  num_wings
        falcon     False       True
        dog        False      False

        Caution
        -------
        Snowpark pandas does not perform a check for the case that values is a DataFrame or Series nor does it check
        whether the index is unique. Snowpark pandas preserves NULL values; if the DataFrame contains NULL in
        a cell the output cell will be NULL.
        """

    def __reduce__(self):
        pass

    def _set_axis_name():
        """
        Alter the name or names of the axis.

        Parameters
        ----------
        name : str or list of str
            Name for the Index, or list of names for the MultiIndex.
        axis : str or int, default: 0
            The axis to set the label.
            0 or 'index' for the index, 1 or 'columns' for the columns.
        inplace : bool, default: False
            Whether to modify `self` directly or return a copy.

        Returns
        -------
        DataFrame or None
        """
        # we need to override the docstring for this private method because
        # defines it and includes doctests that fail for Snowpark pandas.

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
        right : DataFrame or named Series
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
            This argument is ignored in Snowpark pandas.
        indicator : bool or str, default False
            If True, adds a column to the output DataFrame called "_merge" with
            information on the source of each row. The column can be given a different
            name by providing a string argument. The column will have a Categorical
            type with the value of "left_only" for observations whose merge key only
            appears in the left DataFrame, "right_only" for observations
            whose merge key only appears in the right DataFrame, and "both"
            if the observation's merge key is found in both DataFrames.

        validate : str, optional
            This argument is not yet supported in Snowpark pandas.

        Returns
        -------
        DataFrame
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

    def to_csv():
        """
        Write object to a comma-separated values (csv) file. This can write csv file
        either to local filesystem or to snowflake stage. Filepath staring with `@` is
        treated as snowflake stage location.

        Note: Writing to local filesystem supports all parameters but writing to
        snowflake stage does not support float_format, mode, encoding, quoting,
        quotechar, lineterminator, doublequote and decimal parameters. Also when
        writing to snowflake stage, chuncksize, errors and storage_options parameters
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
        columns : sequence, optional
            Columns to write.
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
        Create 'out.csv' containing 'df' without indices

        >>> df = pd.DataFrame({'name': ['Raphael', 'Donatello'],
        ...                    'mask': ['red', 'purple'],
        ...                    'weapon': ['sai', 'bo staff']})
        >>> df.to_csv('out.csv', index=False)  # doctest: +SKIP

        Create 'out.zip' containing 'out.csv'

        >>> df.to_csv(index=False)  # doctest: +SKIP
        >>> compression_opts = dict(method='zip',
        ...                         archive_name='out.csv')  # doctest: +SKIP
        >>> df.to_csv('out.zip', index=False,
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
