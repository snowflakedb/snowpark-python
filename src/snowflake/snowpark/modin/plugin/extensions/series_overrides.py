#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing Series APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `Series.memory_usage`.
"""

from __future__ import annotations

import pandas as native_pd
from pandas._libs.lib import NoDefault, no_default
from pandas._typing import Axis, IndexLabel

from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.pandas import Series
from snowflake.snowpark.modin.pandas.api.extensions import register_series_accessor
from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.modin.utils import _inherit_docstrings


@_inherit_docstrings(native_pd.Series.memory_usage, apilink="pandas.Series")
@register_series_accessor("memory_usage")
def memory_usage(self, index: bool = True, deep: bool = False) -> int:
    """
    Return zero bytes for memory_usage
    """
    # TODO: SNOW-1264697: push implementation down to query compiler
    return 0


@_inherit_docstrings(native_pd.Series.isin, apilink="pandas.Series")
@register_series_accessor("isin")
def isin(self, values: set | ListLike) -> Series:
    """
    Whether elements in Series are contained in `values`.

    Return a boolean Series showing whether each element in the Series
    matches an element in the passed sequence of `values`.

    Caution
    -------
    Snowpark pandas deviates from pandas here with respect to NA values: when the value is considered NA or
    values contains at least one NA, None is returned instead of a boolean value.

    Parameters
    ----------
    values : set or list-like
        The sequence of values to test. Passing in a single string will
        raise a ``TypeError``. Instead, turn a single string into a
        list of one element.

    Returns
    -------
    Series
        Series of booleans indicating if each element is in values.

    Examples
    --------
    >>> s = pd.Series(['lama', 'cow', 'lama', 'beetle', 'lama',
    ...                'hippo'], name='animal')
    >>> s.isin(['cow', 'lama'])
    0     True
    1     True
    2     True
    3    False
    4     True
    5    False
    Name: animal, dtype: bool

    To invert the boolean values, use the ``~`` operator:

    >>> ~s.isin(['cow', 'lama'])
    0    False
    1    False
    2    False
    3     True
    4    False
    5     True
    Name: animal, dtype: bool

    Passing a single string as ``s.isin('lama')`` will raise an error. Use
    a list of one element instead:

    >>> s.isin(['lama'])
    0     True
    1    False
    2     True
    3    False
    4     True
    5    False
    Name: animal, dtype: bool

    >>> pd.Series([1]).isin(['1'])
    0    False
    dtype: bool

    >>> pd.Series([1, 2, None]).isin([2])
    0    False
    1     True
    2     None
    dtype: object
    """

    # pandas compatible TypeError
    if isinstance(values, str):
        raise TypeError(
            "only list-like objects are allowed to be passed to isin(), you passed a [str]"
        )

    # convert to list if given as set
    if isinstance(values, set):
        values = list(values)

    return super(Series, self).isin(values)


@register_series_accessor("groupby")
def groupby(
    self,
    by=None,
    axis: Axis = 0,
    level: IndexLabel | None = None,
    as_index: bool = True,
    sort: bool = True,
    group_keys: bool = True,
    observed: bool | NoDefault = no_default,
    dropna: bool = True,
):
    """
    Group Series using a mapper or by a Series of columns.
    """
    from snowflake.snowpark.modin.pandas.groupby import (
        SeriesGroupBy,
        validate_groupby_args,
    )

    validate_groupby_args(by, level, observed)

    if not as_index:
        raise TypeError("as_index=False only valid with DataFrame")

    axis = self._get_axis_number(axis)
    return SeriesGroupBy(
        self,
        by,
        axis,
        level,
        as_index,
        sort,
        group_keys,
        idx_name=None,
        observed=observed,
        dropna=dropna,
    )


@register_series_accessor("cat")
@property
def cat(self):  # noqa: RT01, D200
    """
    Accessor object for categorical properties of the Series values.
    """
    # TODO: SNOW-1063347: Modin upgrade - modin.pandas.Series functions
    from snowflake.snowpark.modin.pandas.series_utils import CategoryMethods

    return CategoryMethods(self)


# seems to be issues with CachedAccessor from upstream


@register_series_accessor("dt")
@property
def dt(self):
    from modin.pandas.series_utils import DatetimeProperties

    return DatetimeProperties(self)


@register_series_accessor("str")
@property
def str(self):
    from modin.pandas.series_utils import StringMethods

    return StringMethods(self)


# modin 0.28.1 doesn't define type annotations on properties, so we override this
# to satisfy test_type_annotations.py
@register_series_accessor("empty")
@property
def empty(self) -> bool:
    return super().empty
