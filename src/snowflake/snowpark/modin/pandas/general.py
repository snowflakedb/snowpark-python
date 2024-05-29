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

"""Implement pandas general API."""
from __future__ import annotations

from collections.abc import Hashable, Iterable, Mapping, Sequence
from datetime import date, datetime, tzinfo
from logging import getLogger
from typing import TYPE_CHECKING, Any, Literal, Union

import numpy as np
import pandas
import pandas.core.common as common
from pandas import IntervalIndex, NaT, Timestamp
from pandas._libs import NaTType, lib
from pandas._libs.tslibs import to_offset
from pandas._typing import (
    AnyArrayLike,
    Axis,
    DateTimeErrorChoices,
    IndexLabel,
    IntervalClosedType,
    Scalar,
    Suffixes,
)
from pandas.core.arrays import datetimelike
from pandas.core.arrays.datetimes import (
    _infer_tz_from_endpoints,
    _maybe_normalize_endpoints,
)
from pandas.core.dtypes.common import is_list_like
from pandas.core.dtypes.inference import is_array_like
from pandas.core.tools.datetimes import (
    ArrayConvertible,
    DatetimeScalar,
    DatetimeScalarOrArrayConvertible,
    DictConvertible,
)
from pandas.util._validators import validate_inclusive

# add this line to make doctests runnable
from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.pandas.base import BasePandasDataset
from snowflake.snowpark.modin.pandas.dataframe import DataFrame
from snowflake.snowpark.modin.pandas.series import Series
from snowflake.snowpark.modin.pandas.utils import (
    is_scalar,
    raise_if_native_pandas_objects,
)
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_standalone_function_decorator,
)
from snowflake.snowpark.modin.plugin._internal.timestamp_utils import (
    VALID_TO_DATETIME_UNIT,
)
from snowflake.snowpark.modin.plugin._typing import ListLike, ListLikeOfFloats
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    pandas_module_level_function_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import _inherit_docstrings, to_pandas

if TYPE_CHECKING:
    # To prevent cross-reference warnings when building documentation and prevent erroneously
    # linking to `snowflake.snowpark.DataFrame`, we need to explicitly
    # qualify return types in this file with `snowflake.snowpark.modin.pandas.DataFrame`.
    # SNOW-1233342: investigate how to fix these links without using absolute paths
    from modin.core.storage_formats import BaseQueryCompiler  # pragma: no cover

    import snowflake  # pragma: no cover

_logger = getLogger(__name__)

VALID_DATE_TYPE = Union[
    np.integer, float, str, date, datetime, np.datetime64, pd.Timestamp
]


@snowpark_pandas_telemetry_standalone_function_decorator
@_inherit_docstrings(pandas.isna, apilink="pandas.isna")
def isna(obj):  # noqa: PR01, RT01, D200
    """
    Detect missing values for an array-like object.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if isinstance(obj, BasePandasDataset):
        return obj.isna()
    else:
        return pandas.isna(obj)


isnull = isna


@snowpark_pandas_telemetry_standalone_function_decorator
@_inherit_docstrings(pandas.notna, apilink="pandas.notna")
def notna(obj):  # noqa: PR01, RT01, D200
    """
    Detect non-missing values for an array-like object.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if isinstance(obj, BasePandasDataset):
        return obj.notna()
    else:
        return pandas.notna(obj)


notnull = notna


@snowpark_pandas_telemetry_standalone_function_decorator
def merge(
    left: snowflake.snowpark.modin.pandas.DataFrame | Series,
    right: snowflake.snowpark.modin.pandas.DataFrame | Series,
    how: str | None = "inner",
    on: IndexLabel | None = None,
    left_on: None
    | (Hashable | AnyArrayLike | Sequence[Hashable | AnyArrayLike]) = None,
    right_on: None
    | (Hashable | AnyArrayLike | Sequence[Hashable | AnyArrayLike]) = None,
    left_index: bool | None = False,
    right_index: bool | None = False,
    sort: bool | None = False,
    suffixes: Suffixes | None = ("_x", "_y"),
    copy: bool | None = True,
    indicator: bool | str | None = False,
    validate: str | None = None,
):
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
    left : :class:`~snowflake.snowpark.modin.pandas.DataFrame` or named Series
    right : :class:`~snowflake.snowpark.modin.pandas.DataFrame` or named Series
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
    :class:`~snowflake.snowpark.modin.pandas.DataFrame`
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
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    # Raise error if 'left' or 'right' is native pandas object.
    raise_if_native_pandas_objects(left)
    raise_if_native_pandas_objects(right)

    if isinstance(left, Series):
        if left.name is None:
            raise ValueError("Cannot merge a Series without a name")
        else:
            left = left.to_frame()

    if not isinstance(left, DataFrame):
        raise TypeError(
            f"Can only merge Series or DataFrame objects, a {type(left)} was passed"
        )

    return left.merge(
        right,
        how=how,
        on=on,
        left_on=left_on,
        right_on=right_on,
        left_index=left_index,
        right_index=right_index,
        sort=sort,
        suffixes=suffixes,
        copy=copy,
        indicator=indicator,
        validate=validate,
    )


@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
@_inherit_docstrings(pandas.merge_ordered, apilink="pandas.merge_ordered")
def merge_ordered(
    left,
    right,
    on=None,
    left_on=None,
    right_on=None,
    left_by=None,
    right_by=None,
    fill_method=None,
    suffixes=("_x", "_y"),
    how: str = "outer",
) -> DataFrame:  # noqa: PR01, RT01, D200
    """
    Perform a merge for ordered data with optional filling/interpolation.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(left, DataFrame):
        raise ValueError(f"can not merge DataFrame with instance of type {type(right)}")
    if isinstance(right, DataFrame):
        right = to_pandas(right)
    return DataFrame(
        pandas.merge_ordered(
            to_pandas(left),
            right,
            on=on,
            left_on=left_on,
            right_on=right_on,
            left_by=left_by,
            right_by=right_by,
            fill_method=fill_method,
            suffixes=suffixes,
            how=how,
        )
    )


@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
@_inherit_docstrings(pandas.merge_asof, apilink="pandas.merge_asof")
def merge_asof(
    left,
    right,
    on=None,
    left_on=None,
    right_on=None,
    left_index: bool = False,
    right_index: bool = False,
    by=None,
    left_by=None,
    right_by=None,
    suffixes=("_x", "_y"),
    tolerance=None,
    allow_exact_matches: bool = True,
    direction: str = "backward",
) -> DataFrame:  # noqa: PR01, RT01, D200
    """
    Perform a merge by key distance.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(left, DataFrame):
        raise ValueError(f"can not merge DataFrame with instance of type {type(right)}")

    # As of pandas 1.2 these should raise an error; before that it did
    # something likely random:
    if (
        (on and (left_index or right_index))
        or (left_on and left_index)
        or (right_on and right_index)
    ):
        raise ValueError("Can't combine left/right_index with left/right_on or on.")

    if on is not None:
        if left_on is not None or right_on is not None:
            raise ValueError("If 'on' is set, 'left_on' and 'right_on' can't be set.")
        left_on = on
        right_on = on

    if by is not None:
        if left_by is not None or right_by is not None:
            raise ValueError("Can't have both 'by' and 'left_by' or 'right_by'")
        left_by = right_by = by

    if left_on is None and not left_index:
        raise ValueError("Must pass on, left_on, or left_index=True")

    if right_on is None and not right_index:
        raise ValueError("Must pass on, right_on, or right_index=True")

    return DataFrame(
        query_compiler=left._query_compiler.merge_asof(
            right._query_compiler,
            left_on,
            right_on,
            left_index,
            right_index,
            left_by,
            right_by,
            suffixes,
            tolerance,
            allow_exact_matches,
            direction,
        )
    )


@snowpark_pandas_telemetry_standalone_function_decorator
def pivot_table(
    data,
    values=None,
    index=None,
    columns=None,
    aggfunc="mean",
    fill_value=None,
    margins=False,
    dropna=True,
    margins_name="All",
    observed=False,
    sort=True,
):
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
    Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`
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
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(data, DataFrame):
        raise ValueError(
            f"can not create pivot table with instance of type {type(data)}"
        )

    return data.pivot_table(
        values=values,
        index=index,
        columns=columns,
        aggfunc=aggfunc,
        fill_value=fill_value,
        margins=margins,
        dropna=dropna,
        margins_name=margins_name,
        sort=sort,
    )


@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
@_inherit_docstrings(pandas.pivot, apilink="pandas.pivot")
def pivot(data, index=None, columns=None, values=None):  # noqa: PR01, RT01, D200
    """
    Return reshaped DataFrame organized by given index / column values.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(data, DataFrame):
        raise ValueError(f"can not pivot with instance of type {type(data)}")
    return data.pivot(index=index, columns=columns, values=values)


@snowpark_pandas_telemetry_standalone_function_decorator
def to_numeric(
    arg: Scalar | Series | ArrayConvertible,
    errors: Literal["ignore", "raise", "coerce"] = "raise",
    downcast: Literal["integer", "signed", "unsigned", "float"] | None = None,
) -> Series | Scalar | None:
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
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    raise_if_native_pandas_objects(arg)
    if errors not in ("ignore", "raise", "coerce"):
        raise ValueError("invalid error value specified")
    if downcast is not None:
        WarningMessage.ignored_argument(
            operation="to_numeric",
            argument="downcast",
            message="downcast is ignored in Snowflake backend",
        )
    # convert arg to series
    arg_is_scalar = is_scalar(arg)

    if (
        not arg_is_scalar
        and not isinstance(arg, (list, tuple, Series))
        and not (is_array_like(arg) and arg.ndim == 1)
    ):
        raise TypeError("arg must be a list, tuple, 1-d array, or Series")

    if arg_is_scalar:
        arg = Series([arg])

    if not isinstance(arg, Series):
        name = None
        # keep index name
        if isinstance(arg, pandas.Index):
            name = arg.name
        arg = Series(arg, name=name)

    ret = arg._to_numeric(errors=errors)
    if arg_is_scalar:
        # use squeeze to turn the series result into a scalar
        ret = ret.squeeze()
    return ret


@snowpark_pandas_telemetry_standalone_function_decorator
def unique(values) -> np.ndarray:
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
    array([Timestamp('2015-12-31 21:00:00-0800', tz='America/Los_Angeles')],
          dtype=object)

    >>> pd.unique([("a", "b"), ("b", "a"), ("a", "c"), ("b", "a")])
    array([list(['a', 'b']), list(['b', 'a']), list(['a', 'c'])], dtype=object)

    >>> pd.unique([None, np.nan, 2])
    array([nan,  2.])
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if is_list_like(values) and not isinstance(values, dict):
        return Series(values).unique()
    else:
        raise TypeError("Only list-like objects can be used with unique()")


# Adding docstring since pandas docs don't have web section for this function.
@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
def value_counts(
    values, sort=True, ascending=False, normalize=False, bins=None, dropna=True
):
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
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    return Series(values).value_counts(
        sort=sort,
        ascending=ascending,
        normalize=normalize,
        bins=bins,
        dropna=dropna,
    )


@snowpark_pandas_telemetry_standalone_function_decorator
def concat(
    objs: (
        Iterable[snowflake.snowpark.modin.pandas.DataFrame | Series]
        | Mapping[Hashable, snowflake.snowpark.modin.pandas.DataFrame | Series]
    ),
    axis: Axis = 0,
    join: str = "outer",
    ignore_index: bool = False,
    keys: Sequence[Hashable] = None,
    levels: list[Sequence[Hashable]] = None,
    names: list[Hashable] = None,
    verify_integrity: bool = False,
    sort: bool = False,
    copy: bool = True,
) -> snowflake.snowpark.modin.pandas.DataFrame | Series:
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
        When concatenating all Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` along the index (axis=0),
        a Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.Series` is returned. When ``objs`` contains at least
        one Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame`,
        a Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` is returned. When concatenating along
        the columns (axis=1), a Snowpark pandas :class:`~snowflake.snowpark.modin.pandas.DataFrame` is returned.

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
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    # Raise error if native pandas objects are passed.
    raise_if_native_pandas_objects(objs)

    # In native pandas 'concat' API is expected to work with all types of iterables like
    # tuples, list, generators, custom iterators, deque etc.
    # Few exceptions are 'DataFrame', 'Series', 'str', these are also technically
    # iterables, but they are not iterables of pandas objects.
    # Note other iterables can also have non pandas objects as element in them, but it's
    # not possible to know that in advance without iterating over all objects, so we
    # also individual element later.

    # Raise error if 'objs' is not an iterable or an iterable of non-pandas objects.
    if not isinstance(objs, Iterable) or isinstance(
        objs, (pd.DataFrame, pd.Series, str)
    ):
        # Same error as native pandas.
        raise TypeError(
            "first argument must be an iterable of pandas "
            f'objects, you passed an object of type "{type(objs).__name__}"'
        )

    if isinstance(objs, dict):
        if keys is None:
            keys = list(objs.keys())
        # if 'keys' is not none, filter out additional objects from mapping.
        objs = [objs[k] for k in keys]
    else:
        # Native pandas also supports generators as input, that can only be iterated
        # only once so first create a list from 'objs'.
        objs = list(objs)

    for obj in objs:
        # Raise error if native pandas objects are passed.
        raise_if_native_pandas_objects(obj)

    if join not in ("inner", "outer"):
        # Same error as native pandas.
        raise ValueError(
            "Only can inner (intersect) or outer (union) join the other axis"
        )

    axis = pandas.DataFrame()._get_axis_number(axis)

    if len(objs) == 0:
        # Same error as native pandas.
        raise ValueError("No objects to concatenate")

    # Filter out None objects
    if keys is None:
        objs = [o for o in objs if o is not None]
    else:
        tuples = [(k, v) for k, v in zip(keys, objs) if v is not None]
        # convert list of tuples to tuples of list.
        keys, objs = list(map(list, zip(*tuples))) if tuples else ([], [])

    if len(objs) == 0:
        # Same error as native pandas.
        raise ValueError("All objects passed were None")

    for obj in objs:
        # Same error as native pandas.
        if not isinstance(obj, (Series, DataFrame)):
            raise TypeError(
                f"cannot concatenate object of type '{type(obj)}'; "
                "only Series and DataFrame objs are valid"
            )

    # Assign names to unnamed series - the names function as column labels for Series.
    # If all Series have no name, use the keys as names.
    if (
        axis == 1
        and keys is not None
        and all(isinstance(obj, Series) and obj.name is None for obj in objs)
    ):
        for i, obj in enumerate(objs):
            objs[i] = obj.rename(keys[i])

    # If only some Series have names, give them temporary names.
    series_name = 0
    for i, obj in enumerate(objs):
        if isinstance(obj, pd.Series) and obj.name is None:
            objs[i] = obj.rename(series_name)
            series_name = series_name + 1

    # Check if all objects are of Series types.
    all_series = all([isinstance(obj, pd.Series) for obj in objs])
    # When concatenating Series objects on axis 0, pandas tries to preserve name from
    # input if all have same name otherwise set it to None.
    if all_series and axis == 0:
        unique_names = {obj.name for obj in objs}
        name = objs[0].name if len(unique_names) == 1 else None
        objs = [obj.rename(name) for obj in objs]

    if not copy:
        WarningMessage.ignored_argument(
            operation="concat",
            argument="copy",
            message="copy parameter has been ignored with Snowflake execution engine",
        )

    # For the edge case where concatenation is done on the columns where all the objects are series,
    # need to prevent a second column level from being created - therefore, keys is None.
    keys = None if axis == 1 and all(isinstance(obj, Series) for obj in objs) else keys

    result = objs[0]._query_compiler.concat(
        axis,
        [o._query_compiler for o in objs[1:]],
        join=join,
        ignore_index=ignore_index,
        keys=keys,
        levels=levels,
        names=names,
        verify_integrity=verify_integrity,
        sort=sort,
    )
    # If all objects are series and concat axis=0, return Series else return DataFrame.
    if all_series and axis == 0:
        return Series(query_compiler=result)
    return DataFrame(query_compiler=result)


@snowpark_pandas_telemetry_standalone_function_decorator
def to_datetime(
    arg: DatetimeScalarOrArrayConvertible
    | DictConvertible
    | snowflake.snowpark.modin.pandas.DataFrame
    | Series,
    errors: DateTimeErrorChoices = "raise",
    dayfirst: bool = False,
    yearfirst: bool = False,
    utc: bool = False,
    format: str | None = None,
    exact: bool | lib.NoDefault = lib.no_default,
    unit: str | None = None,
    infer_datetime_format: lib.NoDefault | bool = lib.no_default,
    origin: Any = "unix",
    cache: bool = True,
) -> Series | DatetimeScalar | NaTType | None:
    """
    Convert argument to datetime.

    This function converts a scalar, array-like, :class:`~snowflake.snowpark.modin.pandas.Series` or
    :class:`~snowflake.snowpark.modin.pandas.DataFrame`/dict-like to a pandas datetime object.

    Parameters
    ----------
    arg : int, float, str, datetime, list, tuple, 1-d array, Series, :class:`~snowflake.snowpark.modin.pandas.DataFrame`/dict-like
        The object to convert to a datetime. If a :class:`~snowflake.snowpark.modin.pandas.DataFrame` is provided, the
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
          UTC-localized :class:`Timestamp`, :class:`~snowflake.snowpark.modin.pandas.Series` or
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
        - array-like: :class:`~snowflake.snowpark.modin.pandas.Series` with :class:`datetime64` dtype containing
          :class:`datetime.datetime` (or
          :class: :class:`~snowflake.snowpark.modin.pandas.Series` of :class:`object` dtype containing
          :class:`datetime.datetime`)
        - Series: :class:`~snowflake.snowpark.modin.pandas.Series` of :class:`datetime64` dtype (or
          :class: :class:`~snowflake.snowpark.modin.pandas.Series` of :class:`object` dtype containing
          :class:`datetime.datetime`)
        - DataFrame: :class:`~snowflake.snowpark.modin.pandas.Series` of :class:`datetime64` dtype (or
          :class:`~snowflake.snowpark.modin.pandas.Series` of :class:`object` dtype containing
          :class:`datetime.datetime`)

    Raises
    ------
    ParserError
        When parsing a date from string fails.
    ValueError
        When another datetime conversion error happens. For example when one
        of 'year', 'month', day' columns is missing in a :class:`~snowflake.snowpark.modin.pandas.DataFrame`, or
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

    - **Series** are converted to :class:`~snowflake.snowpark.modin.pandas.Series` with :class:`datetime64`
      dtype when possible, otherwise they are converted to :class:`~snowflake.snowpark.modin.pandas.Series` with
      :class:`object` dtype, containing :class:`datetime.datetime`. None/NaN/null
      entries are converted to :const:`NaT` in both cases.

    - **DataFrame/dict-like** are converted to :class:`~snowflake.snowpark.modin.pandas.Series` with
      :class:`datetime64` dtype. For each row a datetime is created from assembling
      the various dataframe columns. Column keys can be common abbreviations
      like [‘year’, ‘month’, ‘day’, ‘minute’, ‘second’, ‘ms’, ‘us’, ‘ns’]) or
      plurals of the same.

    The following causes are responsible for :class:`datetime.datetime` objects
    being returned (possibly inside an :class:`Index` or a :class:`~snowflake.snowpark.modin.pandas.Series` with
    :class:`object` dtype) instead of a proper pandas designated type
    (:class:`Timestamp` or :class:`~snowflake.snowpark.modin.pandas.Series` with :class:`datetime64` dtype):

    - when any input element is before :const:`Timestamp.min` or after
      :const:`Timestamp.max`, see `timestamp limitations
      <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html
      #timeseries-timestamp-limits>`_.

    - when ``utc=False`` (default) and the input is an array-like or
      :class:`~snowflake.snowpark.modin.pandas.Series` containing mixed naive/aware datetime, or aware with mixed
      time offsets. Note that this happens in the (quite frequent) situation when
      the timezone has a daylight savings policy. In that case you may wish to
      use ``utc=True``.

    Examples
    --------

    **Handling various input formats**

    Assembling a datetime from multiple columns of a :class:`~snowflake.snowpark.modin.pandas.DataFrame`. The keys
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
    0   1960-01-02
    1   1960-01-03
    2   1960-01-04
    dtype: datetime64[ns]


    **Non-convertible date/times**

    If a date does not meet the `timestamp limitations
    <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html
    #timeseries-timestamp-limits>`_, passing ``errors='ignore'``
    will return the original input instead of raising any exception.

    Passing ``errors='coerce'`` will force an out-of-bounds date to :const:`NaT`,
    in addition to forcing non-dates (or non-parseable dates) to :const:`NaT`.

    >>> pd.to_datetime(['13000101', 'abc'], format='%Y%m%d', errors='coerce')
    0   NaT
    1   NaT
    dtype: datetime64[ns]


    .. _to_datetime_tz_examples:

    **Timezones and time offsets**

    The default behaviour (``utc=False``) is as follows:

    - Timezone-naive inputs are converted to timezone-naive :class:`~snowflake.snowpark.modin.pandas.Series`:

    >>> pd.to_datetime(['2018-10-26 12:00', '2018-10-26 13:00:15'])
    0   2018-10-26 12:00:00
    1   2018-10-26 13:00:15
    dtype: datetime64[ns]

    - Timezone-aware inputs *with constant time offset* are still converted to
      timezone-naive :class:`~snowflake.snowpark.modin.pandas.Series` by default.

    >>> pd.to_datetime(['2018-10-26 12:00:00 -0500', '2018-10-26 13:00:00 -0500'])
    0   2018-10-26 12:00:00
    1   2018-10-26 13:00:00
    dtype: datetime64[ns]

    - Use right format to convert to timezone-aware type (Note that when call Snowpark
      pandas API to_pandas() the timezone-aware output will always be converted to session timezone):

    >>> pd.to_datetime(['2018-10-26 12:00:00 -0500', '2018-10-26 13:00:00 -0500'], format="%Y-%m-%d %H:%M:%S %z")
    0   2018-10-26 10:00:00-07:00
    1   2018-10-26 11:00:00-07:00
    dtype: datetime64[ns, America/Los_Angeles]

    - Timezone-aware inputs *with mixed time offsets* (for example
      issued from a timezone with daylight savings, such as Europe/Paris):

    >>> pd.to_datetime(['2020-10-25 02:00:00 +0200', '2020-10-25 04:00:00 +0100'])
    0   2020-10-25 02:00:00
    1   2020-10-25 04:00:00
    dtype: datetime64[ns]

    >>> pd.to_datetime(['2020-10-25 02:00:00 +0200', '2020-10-25 04:00:00 +0100'], format="%Y-%m-%d %H:%M:%S %z")
    0   2020-10-24 17:00:00-07:00
    1   2020-10-24 20:00:00-07:00
    dtype: datetime64[ns, America/Los_Angeles]

    Setting ``utc=True`` makes sure always convert to timezone-aware outputs:

    - Timezone-naive inputs are *localized* based on the session timezone

    >>> pd.to_datetime(['2018-10-26 12:00', '2018-10-26 13:00'], utc=True)
    0   2018-10-26 12:00:00-07:00
    1   2018-10-26 13:00:00-07:00
    dtype: datetime64[ns, America/Los_Angeles]

    - Timezone-aware inputs are *converted* to session timezone

    >>> pd.to_datetime(['2018-10-26 12:00:00 -0530', '2018-10-26 12:00:00 -0500'],
    ...                utc=True)
    0   2018-10-26 10:30:00-07:00
    1   2018-10-26 10:00:00-07:00
    dtype: datetime64[ns, America/Los_Angeles]
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    raise_if_native_pandas_objects(arg)

    if arg is None:
        return None  # same as pandas
    if unit and unit not in VALID_TO_DATETIME_UNIT:
        raise ValueError(f"Unrecognized unit {unit}")

    if not cache:
        WarningMessage.ignored_argument(
            operation="to_datetime",
            argument="cache",
            message="cache parameter is ignored with Snowflake backend, i.e., no caching will be applied",
        )
    arg_is_scalar = is_scalar(arg)
    # handle empty array, list, dict
    if not arg_is_scalar and not isinstance(arg, (DataFrame, Series)) and len(arg) == 0:
        return arg if isinstance(arg, Series) else Series(arg)  # always return a Series
    if not isinstance(arg, (DataFrame, Series)):
        # turn dictionary like arg into DataFrame and list like or scalar to Series
        if isinstance(arg, dict):
            arg = DataFrame(arg)  # pragma: no cover
        else:
            name = None
            # keep index name
            if isinstance(arg, pd.Index):
                name = arg.name
            arg = Series(arg)
            arg.name = name

    series = arg._to_datetime(
        errors=errors,
        dayfirst=dayfirst,
        yearfirst=yearfirst,
        utc=utc,
        format=format,
        exact=exact,
        unit=unit,
        infer_datetime_format=infer_datetime_format,
        origin=origin,
    )
    if arg_is_scalar:
        # Calling squeeze directly on Snowpark pandas Series makes an unnecessary
        # count sql call. To avoid that we convert Snowpark pandas Series to Native
        # pandas seris first.
        return series.to_pandas().squeeze()
    return series


@snowpark_pandas_telemetry_standalone_function_decorator
def get_dummies(
    data,
    prefix=None,
    prefix_sep="_",
    dummy_na=False,
    columns=None,
    sparse=False,
    drop_first=False,
    dtype=None,
):  # noqa: PR01, RT01, D200
    """
    Convert categorical variable into dummy/indicator variables.

    Parameters
    ----------
    data : array-like, Series, or :class:`~snowflake.snowpark.modin.pandas.DataFrame`
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
    :class:`~snowflake.snowpark.modin.pandas.DataFrame`
        Dummy-coded data.

    Examples
    --------
    >>> s = pd.Series(list('abca'))

    >>> pd.get_dummies(s)
       a  b  c
    0  1  0  0
    1  0  1  0
    2  0  0  1
    3  1  0  0

    >>> df = pd.DataFrame({'A': ['a', 'b', 'a'], 'B': ['b', 'a', 'c'],
    ...                    'C': [1, 2, 3]})

    >>> pd.get_dummies(df, prefix=['col1', 'col2'])
       C  col1_a  col1_b  col2_a  col2_b  col2_c
    0  1       1       0       0       1       0
    1  2       0       1       1       0       0
    2  3       1       0       0       0       1

    >>> pd.get_dummies(pd.Series(list('abcaa')))
       a  b  c
    0  1  0  0
    1  0  1  0
    2  0  0  1
    3  1  0  0
    4  1  0  0
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    new_qc = data._query_compiler.get_dummies(
        columns=columns,
        prefix=prefix,
        prefix_sep=prefix_sep,
        dummy_na=dummy_na,
        drop_first=drop_first,
        dtype=dtype,
        is_series=not data._is_dataframe,
    )
    return DataFrame(query_compiler=new_qc)


@snowpark_pandas_telemetry_standalone_function_decorator
def melt(
    frame,
    id_vars=None,
    value_vars=None,
    var_name=None,
    value_name="value",
    col_level=None,
    ignore_index: bool = True,
):  # noqa: PR01, RT01, D200
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
    :class:`~snowflake.snowpark.modin.pandas.DataFrame`
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
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    return frame.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name=var_name,
        value_name=value_name,
        col_level=col_level,
        ignore_index=ignore_index,
    )


@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
@_inherit_docstrings(pandas.crosstab, apilink="pandas.crosstab")
def crosstab(
    index,
    columns,
    values=None,
    rownames=None,
    colnames=None,
    aggfunc=None,
    margins=False,
    margins_name: str = "All",
    dropna: bool = True,
    normalize=False,
) -> DataFrame:  # noqa: PR01, RT01, D200
    """
    Compute a simple cross tabulation of two (or more) factors.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    pandas_crosstab = pandas.crosstab(
        index,
        columns,
        values,
        rownames,
        colnames,
        aggfunc,
        margins,
        margins_name,
        dropna,
        normalize,
    )
    return DataFrame(pandas_crosstab)


# Adding docstring since pandas docs don't have web section for this function.
@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
def lreshape(data: DataFrame, groups, dropna=True, label=None):
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
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(data, DataFrame):
        raise ValueError(f"can not lreshape with instance of type {type(data)}")
    return DataFrame(
        pandas.lreshape(to_pandas(data), groups, dropna=dropna, label=label)
    )


@_inherit_docstrings(pandas.wide_to_long, apilink="pandas.wide_to_long")
@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
def wide_to_long(
    df: DataFrame, stubnames, i, j, sep: str = "", suffix: str = r"\d+"
) -> DataFrame:  # noqa: PR01, RT01, D200
    """
    Unpivot a DataFrame from wide to long format.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if not isinstance(df, DataFrame):
        raise ValueError(f"can not wide_to_long with instance of type {type(df)}")
    # ErrorMessage.default_to_pandas("`wide_to_long`")
    return DataFrame(
        pandas.wide_to_long(to_pandas(df), stubnames, i, j, sep=sep, suffix=suffix)
    )


def _determine_name(objs: Iterable[BaseQueryCompiler], axis: int | str):
    """
    Determine names of index after concatenation along passed axis.

    Parameters
    ----------
    objs : iterable of QueryCompilers
        Objects to concatenate.
    axis : int or str
        The axis to concatenate along.

    Returns
    -------
    list with single element
        Computed index name, `None` if it could not be determined.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    axis = pandas.DataFrame()._get_axis_number(axis)

    def get_names(obj):
        return obj.columns.names if axis else obj.index.names

    names = np.array([get_names(obj) for obj in objs])

    # saving old name, only if index names of all objs are the same
    if np.all(names == names[0]):
        # we must do this check to avoid this calls `list(str_like_name)`
        return list(names[0]) if is_list_like(names[0]) else [names[0]]
    else:
        return None


@_inherit_docstrings(pandas.to_datetime, apilink="pandas.to_timedelta")
@snowpark_pandas_telemetry_standalone_function_decorator
@pandas_module_level_function_not_implemented()
def to_timedelta(arg, unit=None, errors="raise"):  # noqa: PR01, RT01, D200
    """
    Convert argument to timedelta.

    Accepts str, timedelta, list-like or Series for arg parameter.
    Returns a Series if and only if arg is provided as a Series.
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py
    if isinstance(arg, Series):
        query_compiler = arg._query_compiler.to_timedelta(unit=unit, errors=errors)
        return Series(query_compiler=query_compiler)
    return pandas.to_timedelta(arg, unit=unit, errors=errors)


@snowpark_pandas_telemetry_standalone_function_decorator
def date_range(
    start: VALID_DATE_TYPE | None = None,
    end: VALID_DATE_TYPE | None = None,
    periods: int | None = None,
    freq: str | pd.DateOffset | None = None,
    tz: str | tzinfo | None = None,
    normalize: bool = False,
    name: Hashable | None = None,
    inclusive: IntervalClosedType = "both",
    **kwargs,
) -> Series:
    """
    Return a fixed frequency series.

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

        .. versionadded:: 1.4.0
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
    ``tz`` is not supported.

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
    0   2018-01-01
    1   2018-01-02
    2   2018-01-03
    3   2018-01-04
    4   2018-01-05
    5   2018-01-06
    6   2018-01-07
    7   2018-01-08
    dtype: datetime64[ns]

    Specify `start` and `periods`, the number of periods (days).

    >>> pd.date_range(start='1/1/2018', periods=8)
    0   2018-01-01
    1   2018-01-02
    2   2018-01-03
    3   2018-01-04
    4   2018-01-05
    5   2018-01-06
    6   2018-01-07
    7   2018-01-08
    dtype: datetime64[ns]

    Specify `end` and `periods`, the number of periods (days).

    >>> pd.date_range(end='1/1/2018', periods=8)
    0   2017-12-25
    1   2017-12-26
    2   2017-12-27
    3   2017-12-28
    4   2017-12-29
    5   2017-12-30
    6   2017-12-31
    7   2018-01-01
    dtype: datetime64[ns]

    Specify `start`, `end`, and `periods`; the frequency is generated
    automatically (linearly spaced).

    >>> pd.date_range(start='2018-04-24', end='2018-04-27', periods=3)
    0   2018-04-24 00:00:00
    1   2018-04-25 12:00:00
    2   2018-04-27 00:00:00
    dtype: datetime64[ns]

    **Other Parameters**

    Changed the `freq` (frequency) to ``'ME'`` (month end frequency).

    >>> pd.date_range(start='1/1/2018', periods=5, freq='ME')
    0   2018-01-31
    1   2018-02-28
    2   2018-03-31
    3   2018-04-30
    4   2018-05-31
    dtype: datetime64[ns]

    Multiples are allowed

    >>> pd.date_range(start='1/1/2018', periods=5, freq='3ME')
    0   2018-01-31
    1   2018-04-30
    2   2018-07-31
    3   2018-10-31
    4   2019-01-31
    dtype: datetime64[ns]

    `freq` can also be specified as an Offset object.

    >>> pd.date_range(start='1/1/2018', periods=5, freq=pd.offsets.MonthEnd(3))
    0   2018-01-31
    1   2018-04-30
    2   2018-07-31
    3   2018-10-31
    4   2019-01-31
    dtype: datetime64[ns]

    `inclusive` controls whether to include `start` and `end` that are on the
    boundary. The default, "both", includes boundary points on either end.

    >>> pd.date_range(start='2017-01-01', end='2017-01-04', inclusive="both")
    0   2017-01-01
    1   2017-01-02
    2   2017-01-03
    3   2017-01-04
    dtype: datetime64[ns]

    Use ``inclusive='left'`` to exclude `end` if it falls on the boundary.

    >>> pd.date_range(start='2017-01-01', end='2017-01-04', inclusive='left')
    0   2017-01-01
    1   2017-01-02
    2   2017-01-03
    dtype: datetime64[ns]

    Use ``inclusive='right'`` to exclude `start` if it falls on the boundary, and
    similarly ``inclusive='neither'`` will exclude both `start` and `end`.

    >>> pd.date_range(start='2017-01-01', end='2017-01-04', inclusive='right')
    0   2017-01-02
    1   2017-01-03
    2   2017-01-04
    dtype: datetime64[ns]
    """
    # TODO: SNOW-1063345: Modin upgrade - modin.pandas functions in general.py

    if freq is None and common.any_none(periods, start, end):
        freq = "D"

    if common.count_not_none(start, end, periods, freq) != 3:
        raise ValueError(
            "Of the four parameters: start, end, periods, and freq, exactly three must be specified"
        )

    # Validation code is mostly copied from pandas code DatetimeArray._generate_range and it will cast it to an integer
    periods = datetimelike.validate_periods(periods)

    # Return DateOffset object from string or datetime.timedelta object
    freq = to_offset(freq)

    if freq is None and periods < 0:
        raise ValueError("Number of samples, %s, must be non-negative." % periods)

    if start is not None:
        start = Timestamp(start)

    if end is not None:
        end = Timestamp(end)

    if start is NaT or end is NaT:
        raise ValueError("Neither `start` nor `end` can be NaT")

    # Check that the `inclusive` argument is among {"both", "neither", "left", "right"}
    left_inclusive, right_inclusive = validate_inclusive(inclusive)

    # If normalize is needed, set start and end time to midnight
    start, end = _maybe_normalize_endpoints(start, end, normalize)

    # If a timezone is not explicitly given via `tz`, see if one can be inferred from the `start` and `end` endpoints.
    # If more than one of these inputs provides a timezone, require that they all agree.
    tz = _infer_tz_from_endpoints(start, end, tz)

    qc = SnowflakeQueryCompiler.from_date_range(
        start=start,
        end=end,
        periods=periods,
        freq=freq,
        tz=tz,
        left_inclusive=left_inclusive,
        right_inclusive=right_inclusive,
    )
    s = Series(query_compiler=qc)
    s.name = name
    return s


@snowpark_pandas_telemetry_standalone_function_decorator
def qcut(
    x: np.ndarray | Series,
    q: int | ListLikeOfFloats,
    labels: ListLike | bool | None = None,
    retbins: bool = False,
    precision: int = 3,
    duplicates: Literal["raise"] | Literal["drop"] = "raise",
) -> Series:
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

    kwargs = {
        "labels": labels,
        "retbins": retbins,
        "precision": precision,
        "duplicates": duplicates,
    }

    # For numpy or list, call to native pandas.
    if not isinstance(x, Series):
        return pandas.qcut(x, q, **kwargs)

    # Check that labels is supported as in pandas.
    if not (labels is None or labels is False or is_list_like(labels)):
        raise ValueError(
            "Bin labels must either be False, None or passed in as a list-like argument"
        )

    # Carry out check that for the list-like case quantiles are (monotonically) increasing,
    # if not the case throw pandas compatible error.
    if not isinstance(q, int) and np.all(np.diff(q) < 0):
        # Note: Pandas 2.x changed the error message here, using Pandas 2.x behavior here.
        raise ValueError("left side of interval must be <= right side")

        # remove duplicates (input like [0.5, 0.5] is ok)
        q = sorted(list(set(q)))

    if labels is not False:
        # Labels require categorical, not yet supported. Use native pandas conversion here to compute result.
        ErrorMessage.not_implemented(
            "Snowpark pandas API qcut method supports only labels=False, if you need support"
            " for labels consider calling pandas.qcut(x.to_pandas(), q, ...)"
        )

    ans = x._qcut(q, retbins, duplicates)

    if isinstance(q, int) and q != 1 and len(ans) == 1:
        if duplicates == "raise":
            # We issue a count query since if q !=1 and x is a Series/list-like containing
            # a single element, an error will be produced  ValueError: Bin edges must be unique: array([0., 0., 0., 0., 0., 0., 0., 0., 0., 0., 0.]).
            #                You can drop duplicate edges by setting the 'duplicates' kwarg.
            # With qcut being an API that requires conversion, we can mimick this behavior here.

            # Produce raising error.
            raise ValueError(
                f"Bin edges must be unique: {repr(np.array([0.] * q))}.\nYou can drop duplicate edges by setting the 'duplicates' kwarg."
            )
        else:
            # The result will always be NaN because no unique bin could be found.
            return pd.Series([np.nan])

    return ans


@snowpark_pandas_telemetry_standalone_function_decorator
def cut(
    x: AnyArrayLike,
    bins: int | Sequence[Scalar] | IntervalIndex,
    right: bool = True,
    labels=None,
    retbins: bool = False,
    precision: int = 3,
    include_lowest: bool = False,
    duplicates: str = "raise",
    ordered: bool = True,
):
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

    if retbins is True:
        ErrorMessage.not_implemented("retbins not supported.")

    # Execute other supported objects via native pandas.
    if not isinstance(x, Series):
        return pandas.cut(
            x,
            bins,
            right=right,
            labels=labels,
            retbins=retbins,
            precision=precision,
            include_lowest=include_lowest,
            duplicates=duplicates,
            ordered=ordered,
        )

    # Produce pandas-compatible error if ordered=False and labels are not specified.
    # No error is raised when labels are not desired (labels=False).
    if ordered is False and labels is None:
        raise ValueError("'labels' must be provided if 'ordered = False'")

    bins, qc = x._query_compiler.cut(
        bins,
        right=right,
        labels=labels,
        precision=precision,
        include_lowest=include_lowest,
        duplicates=duplicates,
    )

    # Depending on setting, reconstruct bins and convert qc to the correct result.
    if labels is False:
        return pd.Series(query_compiler=qc)
    else:
        # Raise NotImplemented Error as categorical is not supported.
        ErrorMessage.not_implemented("categorical not supported in Snowpark pandas API")

        # Following code would produce correct result, uncomment once categorical is supported.
        # Convert to pandas categorical and return as Series.
        # Note: In the future, once we support CategoricalType we could keep this lazily around. For now,
        # match what pandas does here. In the future, change pandas -> pd and everything should work out-of-the box.
        # arr = qc.to_numpy().ravel()
        # return pandas.Series(
        #    pandas.Categorical(values=arr, categories=labels, ordered=ordered)
        # )
