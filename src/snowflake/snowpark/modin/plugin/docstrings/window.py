#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This module contains Rolling docstrings that override modin's docstrings."""


from textwrap import dedent

from pandas.util._decorators import doc

_window_agg_method_engine_template = """
Compute the {win_type} {fname}.

Parameters
----------
numeric_only : bool, default {no}
    Include only float, int, boolean columns.

{args}

engine : str, default None {e}
    * ``'cython'`` : Runs the operation through C-extensions from cython.
    * ``'numba'`` : Runs the operation through JIT compiled code from numba.
    * ``None`` : Defaults to ``'cython'`` or globally setting ``compute.use_numba``

    **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

engine_kwargs : dict, default None {ek}
    * For ``'cython'`` engine, there are no accepted ``engine_kwargs``
    * For ``'numba'`` engine, the engine can accept ``nopython``, ``nogil``
        and ``parallel`` dictionary keys. The values must either be ``True`` or
        ``False``. The default ``engine_kwargs`` for the ``'numba'`` engine is
        ``{{'nopython': True, 'nogil': False, 'parallel': False}}``.

    **This parameter is ignored in Snowpark pandas. The execution engine will always be Snowflake.**

{kwargs}

Returns
-------
:class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
    Computed {win_type} {fname} of values.

Examples
--------
{example}
"""

_rolling_aggregate_method_doc_template = """
Rolling aggregate using one or more operations.

Parameters
----------
func : function, str, list, or dict
    Function to use for aggregating the data.
    Accepted combinations are:
    - function
    - string function name
    - list of functions and/or function names, e.g. ``[np.sum, 'mean']``
    - dict of axis labels -> functions, function names or list of such.

*args : tuple
    Positional arguments to pass to func.

**kwargs : dict
    Keyword arguments to be passed into func.

Returns
-------
Scalar
    Case when `Series.agg` is called with a single function.
:class:`~snowflake.snowpark.modin.pandas.Series`
    Case when `DataFrame.agg` is called with a single function.
:class:`~snowflake.snowpark.modin.pandas.DataFrame`
    Case when `DataFrame.agg` is called with several functions.

{examples}
"""


_aggregate_examples_rolling_doc = dedent(
    """
Examples
--------
>>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
>>> df
     B
0  0.0
1  1.0
2  2.0
3  NaN
4  4.0
>>> df.rolling(2, min_periods=1).aggregate("mean")
     B
0  0.0
1  0.5
2  1.5
3  2.0
4  4.0
>>> df.rolling(2, min_periods=1).aggregate(["min", "max"])
          B
   min  max
0  0.0  0.0
1  0.0  1.0
2  1.0  2.0
3  2.0  2.0
4  4.0  4.0
"""
)


class Rolling:

    """
    Compute the rolling count.

    Parameters
    ----------
    numeric_only : bool, default False
        Include only float, int, boolean columns.

    *args : tuple
        Positional arguments to pass to func.

    **kwargs : dict
        Keyword arguments to be passed into func.

    Returns
    -------
    :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
        Computed rolling count of values.

    Examples
    --------
    >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
    >>> df
         B
    0  0.0
    1  1.0
    2  2.0
    3  NaN
    4  4.0
    >>> df.rolling(2, min_periods=1).count()
       B
    0  1
    1  2
    2  2
    3  1
    4  1
    >>> df.rolling(2, min_periods=2).count()
         B
    0  NaN
    1  2.0
    2  2.0
    3  1.0
    4  1.0
    >>> df.rolling(3, min_periods=1, center=True).count()
       B
    0  2
    1  3
    2  2
    3  2
    4  1
    """

    def count():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="rolling",
        fname="sum",
        no=False,
        args=dedent(
            """\
        *args : tuple
            Positional arguments to pass to func."""
        ),
        kwargs=dedent(
            """\
        **kwargs : dict
            Keyword arguments to be passed into func."""
        ),
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.rolling(2, min_periods=1).sum()
             B
        0  0.0
        1  1.0
        2  3.0
        3  2.0
        4  4.0
        >>> df.rolling(2, min_periods=2).sum()
             B
        0  NaN
        1  1.0
        2  3.0
        3  NaN
        4  NaN
        >>> df.rolling(3, min_periods=1, center=True).sum()
             B
        0  1.0
        1  3.0
        2  3.0
        3  6.0
        4  4.0"""
        ),
    )
    def sum():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="rolling",
        fname="mean",
        args=dedent(
            """\
        *args : tuple
            Positional arguments to pass to func."""
        ),
        kwargs=dedent(
            """\
        **kwargs : dict
            Keyword arguments to be passed into func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.rolling(2, min_periods=1).mean()
             B
        0  0.0
        1  0.5
        2  1.5
        3  2.0
        4  4.0
        >>> df.rolling(2, min_periods=2).mean()
             B
        0  NaN
        1  0.5
        2  1.5
        3  NaN
        4  NaN
        >>> df.rolling(3, min_periods=1, center=True).mean()
             B
        0  0.5
        1  1.0
        2  1.5
        3  3.0
        4  4.0"""
        ),
    )
    def mean():
        pass

    # TODO: SNOW-1419071 API not implemented - uncomment when done.
    # @doc(
    #     _window_agg_method_engine_template,
    #     win_type="rolling",
    #     fname="median",
    #     args=None,
    #     kwargs=dedent(
    #         """\
    #     **kwargs : dict
    #         Keyword arguments to be passed into func."""
    #     ),
    #     no=False,
    #     e=None,
    #     ek=None,
    #     example=dedent(
    #         """\
    #     >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
    #     >>> df
    #          B
    #     0  0.0
    #     1  1.0
    #     2  2.0
    #     3  NaN
    #     4  4.0
    #     >>> df.rolling(2, min_periods=1).median()
    #          B
    #     0  0.0
    #     1  0.5
    #     2  1.5
    #     3  2.0
    #     4  4.0"""
    #     ),
    # )
    def median():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="rolling",
        fname="var",
        args=dedent(
            """\
        *args : tuple
            Positional arguments to pass to func."""
        ),
        kwargs=dedent(
            """\
        **kwargs : dict
            Keyword arguments to be passed into func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.rolling(2, min_periods=1).var()
             B
        0  NaN
        1  0.5
        2  0.5
        3  NaN
        4  NaN
        >>> df.rolling(2, min_periods=1).var(ddof=0)
              B
        0  0.00
        1  0.25
        2  0.25
        3  0.00
        4  0.00
        >>> df.rolling(3, min_periods=1, center=True).var()
             B
        0  0.5
        1  1.0
        2  0.5
        3  2.0
        4  NaN"""
        ),
    )
    def var():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="rolling",
        fname="std",
        args=dedent(
            """\
        *args : tuple
            Positional arguments to pass to func."""
        ),
        kwargs=dedent(
            """\
        **kwargs : dict
            Keyword arguments to be passed into func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.rolling(2, min_periods=1).std()
                  B
        0       NaN
        1  0.707107
        2  0.707107
        3       NaN
        4       NaN
        >>> df.rolling(2, min_periods=1).std(ddof=0)
             B
        0  0.0
        1  0.5
        2  0.5
        3  0.0
        4  0.0
        >>> df.rolling(3, min_periods=1, center=True).std()
                  B
        0  0.707107
        1  1.000000
        2  0.707107
        3  1.414214
        4       NaN"""
        ),
    )
    def std():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="rolling",
        fname="min",
        args=dedent(
            """\
        *args : tuple
            Positional arguments to pass to func."""
        ),
        kwargs=dedent(
            """\
        **kwargs : dict
            Keyword arguments to be passed into func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.rolling(2, min_periods=1).min()
             B
        0  0.0
        1  0.0
        2  1.0
        3  2.0
        4  4.0"""
        ),
    )
    def min():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="rolling",
        fname="max",
        args=dedent(
            """\
        *args : tuple
            Positional arguments to pass to func."""
        ),
        kwargs=dedent(
            """\
        **kwargs : dict
            Keyword arguments to be passed into func."""
        ),
        no=False,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.rolling(2, min_periods=1).max()
             B
        0  0.0
        1  1.0
        2  2.0
        3  2.0
        4  4.0"""
        ),
    )
    def max():
        pass

    def corr():
        pass

    def cov():
        pass

    def skew():
        pass

    def kurt():
        pass

    def apply():
        pass

    # TODO: SNOW-1419104 API not implemented - uncomment when done.
    # @doc(
    #     _rolling_aggregate_method_doc_template, examples=_aggregate_examples_rolling_doc
    # )
    def aggregate():
        pass

    agg = aggregate

    def quantile():
        pass

    def sem():
        pass

    def rank():
        pass


class Expanding:

    """
    Compute the expanding count.

    Parameters
    ----------
    numeric_only : bool, default False
        Include only float, int, boolean columns.

    Returns
    -------
    :class:`~snowflake.snowpark.modin.pandas.Series` or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
        Computed expanding count of values.

    Examples
    --------
    >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
    >>> df
         B
    0  0.0
    1  1.0
    2  2.0
    3  NaN
    4  4.0
    >>> df.expanding(2).count()
         B
    0  NaN
    1  2.0
    2  3.0
    3  3.0
    4  4.0
    """

    def count():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="expanding",
        fname="sum",
        no=False,
        args=None,
        kwargs=None,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.expanding(2).sum()
             B
        0  NaN
        1  1.0
        2  3.0
        3  3.0
        4  7.0"""
        ),
    )
    def sum():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="expanding",
        fname="mean",
        no=False,
        args=None,
        kwargs=None,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.expanding(2).mean()
              B
        0   NaN
        1  0.50
        2  1.00
        3  1.00
        4  1.75"""
        ),
    )
    def mean():
        pass

    def median():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="expanding",
        fname="var",
        no=False,
        args=None,
        kwargs=None,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.expanding(2).var()
                  B
        0       NaN
        1  0.500000
        2  1.000000
        3  1.000000
        4  2.916667"""
        ),
    )
    def var():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="expanding",
        fname="std",
        no=False,
        args=None,
        kwargs=None,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.expanding(2).std()
                  B
        0       NaN
        1  0.707107
        2  1.000000
        3  1.000000
        4  1.707825"""
        ),
    )
    def std():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="expanding",
        fname="std",
        no=False,
        args=None,
        kwargs=None,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.expanding(2).min()
             B
        0  NaN
        1  0.0
        2  0.0
        3  0.0
        4  0.0"""
        ),
    )
    def min():
        pass

    @doc(
        _window_agg_method_engine_template,
        win_type="expanding",
        fname="std",
        no=False,
        args=None,
        kwargs=None,
        e=None,
        ek=None,
        example=dedent(
            """\
        >>> df = pd.DataFrame({'B': [0, 1, 2, np.nan, 4]})
        >>> df
             B
        0  0.0
        1  1.0
        2  2.0
        3  NaN
        4  4.0
        >>> df.expanding(2).max()
             B
        0  NaN
        1  1.0
        2  2.0
        3  2.0
        4  4.0"""
        ),
    )
    def max():
        pass

    def corr():
        pass

    def cov():
        pass

    def skew():
        pass

    def kurt():
        pass

    def apply():
        pass

    def aggregate():
        pass

    def quantile():
        pass

    def sem():
        pass

    def rank():
        pass
