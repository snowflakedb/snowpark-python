#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This module contains Resampler docstrings that override modin's docstrings."""

from textwrap import dedent


class Resampler:
    def __getitem__(self, key):
        """
        Get ``Resampler`` based on `key` columns of original dataframe.

        Parameters
        ----------
        key : str or list
            String or list of selections.

        Returns
        -------
        modin.pandas.BasePandasDataset
            New ``Resampler`` based on `key` columns subset
            of the original dataframe.
        """

    @property
    def groups():
        pass

    @property
    def indices():
        pass

    def get_group():
        pass

    _shared_docs = dedent(
        """
    Aggregate using one or more operations over the specified axis.

    Parameters
    ----------
    func : function, str, list or dict
        Function to use for aggregating the data. If a function, must either
        work when passed a {klass} or when passed to {klass}.apply.

        Accepted combinations are:

        - function
        - string function name
        - list of functions and/or function names, e.g. ``[np.sum, 'mean']``
        - dict of axis labels -> functions, function names or list of such.

    *args
        Positional arguments to pass to `func`.
    **kwargs
        Keyword arguments to pass to `func`.
    {axis}
    Returns
    -------
    scalar, Series or DataFrame

        The return can be:

        * scalar : when Series.agg is called with single function
        * Series : when DataFrame.agg is called with a single function
        * DataFrame : when DataFrame.agg is called with several functions

        Return scalar, Series or DataFrame.
    {see_also}
    Notes
    -----
    `agg` is an alias for `aggregate`. Use the alias.

    A passed user-defined-function will be passed a Series for evaluation.
    {examples}"""
    )

    _agg_see_also_doc = dedent(
        """
    See Also
    --------
    DataFrame.groupby.aggregate : Aggregate using callable, string, dict,
        or list of string/callables.
    DataFrame.resample.transform : Transforms the Series on each group
        based on the given function.
    DataFrame.aggregate: Aggregate using one or more
        operations over the specified axis.
    """
    )

    _agg_examples_doc = dedent(
        """
    Examples
    --------

    >>> s = pd.Series([1, 2, 3, 4, 5],
    ...               index=pd.date_range('20130101', periods=5, freq='s'))
    >>> s
    2013-01-01 00:00:00    1
    2013-01-01 00:00:01    2
    2013-01-01 00:00:02    3
    2013-01-01 00:00:03    4
    2013-01-01 00:00:04    5
    Freq: None, dtype: int64

    >>> r = s.resample('2s')

    >>> r.agg(np.sum)
    2013-01-01 00:00:00    3
    2013-01-01 00:00:02    7
    2013-01-01 00:00:04    5
    Freq: None, dtype: int8

    >>> r.agg(['sum', 'mean', 'max'])
                         sum  mean  max
    2013-01-01 00:00:00    3   1.5    2
    2013-01-01 00:00:02    7   3.5    4
    2013-01-01 00:00:04    5   5.0    5

    >>> r.agg({'result': lambda x: x.mean() / x.std(),
    ...        'total': np.sum})
                           result  total
    2013-01-01 00:00:00  2.121320      3
    2013-01-01 00:00:02  4.949747      7
    2013-01-01 00:00:04       NaN      5

    """
    )

    # TODO - SNOW-1420892 API not implemented, uncomment when done.
    # @doc(
    #     _shared_docs,
    #     see_also=_agg_see_also_doc,
    #     examples=_agg_examples_doc,
    #     klass="DataFrame",
    #     axis="",
    # )
    def apply():
        pass

    # TODO - SNOW-1420825 API not implemented, uncomment when done.
    # @doc(
    #     _shared_docs,
    #     see_also=_agg_see_also_doc,
    #     examples=_agg_examples_doc,
    #     klass="DataFrame",
    #     axis="",
    # )
    def aggregate():
        pass

    agg = aggregate

    def transform():
        pass

    def pipe():
        pass

    def ffill():
        """
        Forward fill values for missing resample bins.

        Parameters
        ----------
        limit : int, optional
            This parameter is not supported and will raise NotImplementedError.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            A DataFrame with values forward filled for missing resample bins.

        Examples
        --------
        For Series:
        >>> lst1 = pd.to_datetime(['2020-01-03', '2020-01-04', '2020-01-05', '2020-01-07', '2020-01-08'])
        >>> ser1 = pd.Series([1, 2, 3, 4, 5], index=lst1)
        >>> ser1
        2020-01-03    1
        2020-01-04    2
        2020-01-05    3
        2020-01-07    4
        2020-01-08    5
        Freq: None, dtype: int64

        >>> ser1.resample('1D').ffill()
        2020-01-03    1
        2020-01-04    2
        2020-01-05    3
        2020-01-06    3
        2020-01-07    4
        2020-01-08    5
        Freq: None, dtype: int64

        >>> ser1.resample('3D').ffill()
        2020-01-03    1
        2020-01-06    3
        Freq: None, dtype: int64

        >>> lst2 = pd.to_datetime(['2023-01-03 1:00:00', '2023-01-04', '2023-01-05 23:00:00', '2023-01-06', '2023-01-07 2:00:00', '2023-01-10'])
        >>> ser2 = pd.Series([1, 2, 3, 4, None, 6], index=lst2)
        >>> ser2
        2023-01-03 01:00:00    1.0
        2023-01-04 00:00:00    2.0
        2023-01-05 23:00:00    3.0
        2023-01-06 00:00:00    4.0
        2023-01-07 02:00:00    NaN
        2023-01-10 00:00:00    6.0
        Freq: None, dtype: float64

        >>> ser2.resample('1D').ffill()
        2023-01-03    NaN
        2023-01-04    2.0
        2023-01-05    2.0
        2023-01-06    4.0
        2023-01-07    4.0
        2023-01-08    NaN
        2023-01-09    NaN
        2023-01-10    6.0
        Freq: None, dtype: float64

        >>> ser2.resample('2D').ffill()
        2023-01-03    NaN
        2023-01-05    2.0
        2023-01-07    4.0
        2023-01-09    NaN
        Freq: None, dtype: float64

        For DataFrame:

        >>> index1 = pd.to_datetime(['2020-01-03', '2020-01-04', '2020-01-05', '2020-01-07', '2020-01-08'])
        >>> df1 = pd.DataFrame({'a': range(len(index1)),
        ... 'b': range(len(index1) + 10, len(index1) * 2 + 10)},
        ...  index=index1)
        >>> df1
                    a   b
        2020-01-03  0  15
        2020-01-04  1  16
        2020-01-05  2  17
        2020-01-07  3  18
        2020-01-08  4  19

        >>> df1.resample('1D').ffill()
                    a   b
        2020-01-03  0  15
        2020-01-04  1  16
        2020-01-05  2  17
        2020-01-06  2  17
        2020-01-07  3  18
        2020-01-08  4  19

        >>> df1.resample('3D').ffill()
                    a   b
        2020-01-03  0  15
        2020-01-06  2  17

        >>> index2 = pd.to_datetime(['2023-01-03 1:00:00', '2023-01-04', '2023-01-05 23:00:00', '2023-01-06', '2023-01-07 2:00:00', '2023-01-10'])
        >>> df2 = pd.DataFrame({'a': range(len(index2)),
        ... 'b': range(len(index2) + 10, len(index2) * 2 + 10)},
        ...  index=index2)
        >>> df2
                             a   b
        2023-01-03 01:00:00  0  16
        2023-01-04 00:00:00  1  17
        2023-01-05 23:00:00  2  18
        2023-01-06 00:00:00  3  19
        2023-01-07 02:00:00  4  20
        2023-01-10 00:00:00  5  21

        >>> df2.resample('1D').ffill()
                      a     b
        2023-01-03  NaN   NaN
        2023-01-04  1.0  17.0
        2023-01-05  1.0  17.0
        2023-01-06  3.0  19.0
        2023-01-07  3.0  19.0
        2023-01-08  4.0  20.0
        2023-01-09  4.0  20.0
        2023-01-10  5.0  21.0

        >>> df2.resample('2D').ffill()
                      a     b
        2023-01-03  NaN   NaN
        2023-01-05  1.0  17.0
        2023-01-07  3.0  19.0
        2023-01-09  4.0  20.0
        """

    def backfill():
        pass

    def bfill():
        pass

    def pad():
        pass

    def nearest():
        pass

    def fillna():
        pass

    def asfreq():
        pass

    def interpolate():
        pass

    def count():
        """
        Compute count of resample bins, exclude missing values.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed count of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').count()
        2020-01-01    2
        2020-01-03    2
        Freq: None, dtype: int64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').count()
        2020-01-01 00:00:00    2
        2020-01-01 00:00:02    1
        Freq: None, dtype: int64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').count()
                    a  b  c
        2020-01-01  2  2  2
        2020-01-03  2  2  2
        """

    def nunique():
        pass

    def first():
        """
        Compute the first entry of each column within each group.

        Defaults to skipping NA elements.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default -1
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` valid values are present the result will be NA.

        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            First values within each group.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').first()
        2020-01-01    1
        2020-01-03    3
        Freq: None, dtype: int64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').first()
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').first()
                    a  b  c
        2020-01-01  1  8  2
        2020-01-03  2  5  8
        """

    def last():
        """
        Compute the last entry of each column within each group.

        Defaults to skipping NA elements.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default -1
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` valid values are present the result will be NA.

        skipna : bool, default True
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Last values within each group.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').last()
        2020-01-01    2
        2020-01-03    4
        Freq: None, dtype: int64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').last()
        2020-01-01 00:00:00    2.0
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').last()
                    a  b  c
        2020-01-01  1  2  5
        2020-01-03  2  6  9
        """

    def max():
        """
        Compute maximum of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed maximum of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').max()
        2020-01-01    2
        2020-01-03    4
        Freq: None, dtype: int64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').max()
        2020-01-01 00:00:00    2.0
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8], [1, 2], [2, 5], [2, 6]]
        >>> df1 = pd.DataFrame(data,
        ... columns=["a", "b"],
        ... index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df1
                    a  b
        2020-01-01  1  8
        2020-01-02  1  2
        2020-01-03  2  5
        2020-01-04  2  6

        >>> df1.resample('2D').max()
                    a  b
        2020-01-01  1  8
        2020-01-03  2  6

        >>> df2 = pd.DataFrame(
        ... {'A': [1, 2, 3, np.nan], 'B': [np.nan, np.nan, 3, 4]},
        ... index=pd.date_range('2020-01-01', periods=4, freq='1S'))
        >>> df2
                               A    B
        2020-01-01 00:00:00  1.0  NaN
        2020-01-01 00:00:01  2.0  NaN
        2020-01-01 00:00:02  3.0  3.0
        2020-01-01 00:00:03  NaN  4.0

        >>> df2.resample('2S').max()
                               A    B
        2020-01-01 00:00:00  2.0  NaN
        2020-01-01 00:00:02  3.0  4.0
        """

    def mean():
        """
        Compute mean of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed mean of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').mean()
        2020-01-01    1.5
        2020-01-03    3.5
        Freq: None, dtype: float64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').mean()
        2020-01-01 00:00:00    1.5
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> df1 = pd.DataFrame(
        ... {'A': [1, 1, 2, 1, 2], 'B': [np.nan, 2, 3, 4, 5]},
        ... index=pd.date_range('2020-01-01', periods=5, freq='1D'))
        >>> df1
                    A    B
        2020-01-01  1  NaN
        2020-01-02  1  2.0
        2020-01-03  2  3.0
        2020-01-04  1  4.0
        2020-01-05  2  5.0

        >>> df1.resample('2D').mean()
                      A    B
        2020-01-01  1.0  2.0
        2020-01-03  1.5  3.5
        2020-01-05  2.0  5.0

        >>> df1.resample('2D')['B'].mean()
        2020-01-01    2.0
        2020-01-03    3.5
        2020-01-05    5.0
        Freq: None, Name: B, dtype: float64

        >>> df2 = pd.DataFrame(
        ... {'A': [1, 2, 3, np.nan], 'B': [np.nan, np.nan, 3, 4]},
        ... index=pd.date_range('2020-01-01', periods=4, freq='1S'))
        >>> df2
                               A    B
        2020-01-01 00:00:00  1.0  NaN
        2020-01-01 00:00:01  2.0  NaN
        2020-01-01 00:00:02  3.0  3.0
        2020-01-01 00:00:03  NaN  4.0

        >>> df2.resample('2S').mean()
                               A    B
        2020-01-01 00:00:00  1.5  NaN
        2020-01-01 00:00:02  3.0  3.5
        """

    def median():
        """
        Compute median of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed median of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').median()
        2020-01-01    1.5
        2020-01-03    3.5
        Freq: None, dtype: float64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').median()
        2020-01-01 00:00:00    1.5
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').median()
                      a    b    c
        2020-01-01  1.0  5.0  3.5
        2020-01-03  2.0  5.5  8.5
        """

    def min():
        """
        Compute minimum of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed minimum of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').min()
        2020-01-01    1
        2020-01-03    3
        Freq: None, dtype: int64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').min()
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').min()
                    a  b  c
        2020-01-01  1  2  2
        2020-01-03  2  5  8
        """

    def ohlc():
        pass

    def prod():
        pass

    def size():
        """
        Compute group sizes.

        Parameters
        ----------
        None

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Number of rows in each group as a Series if ``as_index`` is True or a DataFrame if ``as_index`` is False.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').size()
        2020-01-01    2
        2020-01-03    2
        Freq: None, dtype: int64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').size()
        2020-01-01 00:00:00    2
        2020-01-01 00:00:02    2
        Freq: None, dtype: int64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').size()
        2020-01-01    2
        2020-01-03    2
        Freq: None, dtype: int64
        """

    def sem():
        pass

    def std():
        """
        Compute standard deviation of resample bins.

        Parameters
        ----------
        ddof : int, default 1
            Delta Degrees of Freedom. The divisor used in calculations is N - ddof,
            where N represents the number of elements.

        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present, the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed standard deviation of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').std()
        2020-01-01    0.707107
        2020-01-03    0.707107
        Freq: None, dtype: float64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').std()
        2020-01-01 00:00:00    0.707107
        2020-01-01 00:00:02         NaN
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').std()
                      a         b         c
        2020-01-01  0.0  4.242641  2.121320
        2020-01-03  0.0  0.707107  0.707107
        """

    def sum():
        """
        Compute sum of resample bins.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` and non-NA values are present, the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed sum of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').sum()
        2020-01-01    3
        2020-01-03    7
        Freq: None, dtype: int64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').sum()
        2020-01-01 00:00:00    3.0
        2020-01-01 00:00:02    4.0
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').sum()
                    a   b   c
        2020-01-01  2  10   7
        2020-01-03  4  11  17
        """

    def var():
        """
        Compute variance of resample bins.

        Parameters
        ----------
        ddof : int, default 1
            Delta Degrees of Freedom. The divisor used in calculations is N - ddof,
            where N represents the number of elements.

        numeric_only : bool, default False
            Include only float, int, boolean columns.

        min_count : int, default 0
            The required number of valid values to perform the operation. If fewer
            than ``min_count`` non-NA values are present, the result will be NA.

        engine : str, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        engine_kwargs : dict, default None
            **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

        Returns
        -------
        :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
            Computed variance of values within each resample bin.

        Examples
        --------
        For Series:

        >>> lst1 = pd.date_range('2020-01-01', periods=4, freq='1D')
        >>> ser1 = pd.Series([1, 2, 3, 4], index=lst1)
        >>> ser1
        2020-01-01    1
        2020-01-02    2
        2020-01-03    3
        2020-01-04    4
        Freq: None, dtype: int64

        >>> ser1.resample('2D').var()
        2020-01-01    0.5
        2020-01-03    0.5
        Freq: None, dtype: float64

        >>> lst2 = pd.date_range('2020-01-01', periods=4, freq='S')
        >>> ser2 = pd.Series([1, 2, np.nan, 4], index=lst2)
        >>> ser2
        2020-01-01 00:00:00    1.0
        2020-01-01 00:00:01    2.0
        2020-01-01 00:00:02    NaN
        2020-01-01 00:00:03    4.0
        Freq: None, dtype: float64

        >>> ser2.resample('2S').var()
        2020-01-01 00:00:00    0.5
        2020-01-01 00:00:02    NaN
        Freq: None, dtype: float64

        For DataFrame:

        >>> data = [[1, 8, 2], [1, 2, 5], [2, 5, 8], [2, 6, 9]]
        >>> df = pd.DataFrame(data,
        ...      columns=["a", "b", "c"],
        ...      index=pd.date_range('2020-01-01', periods=4, freq='1D'))
        >>> df
                    a  b  c
        2020-01-01  1  8  2
        2020-01-02  1  2  5
        2020-01-03  2  5  8
        2020-01-04  2  6  9

        >>> df.resample('2D').var()
                      a     b    c
        2020-01-01  0.0  18.0  4.5
        2020-01-03  0.0   0.5  0.5
        """

    def quantile():
        pass
