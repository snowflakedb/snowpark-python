#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing Series APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `Series.memory_usage`.
"""

from typing import Union

import pandas as native_pd

from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.pandas import Series
from snowflake.snowpark.modin.pandas.api.extensions import register_series_accessor
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_method_decorator,
)
from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.modin.plugin.utils.error_message import series_not_implemented
from snowflake.snowpark.modin.utils import _inherit_docstrings


@_inherit_docstrings(native_pd.Series.memory_usage, apilink="pandas.Series")
@register_series_accessor("memory_usage")
@snowpark_pandas_telemetry_method_decorator
def memory_usage(self, index: bool = True, deep: bool = False) -> int:
    """
    Return zero bytes for memory_usage
    """
    # TODO: SNOW-1264697: push implementation down to query compiler
    return 0


@_inherit_docstrings(native_pd.Series.infer_objects, apilink="pandas.Series")
@register_series_accessor("infer_objects")
@snowpark_pandas_telemetry_method_decorator
@series_not_implemented()
def infer_objects(self) -> Series:  # pragma: no cover # noqa: RT01, D200
    """
    Attempt to infer better dtypes for object columns.
    """
    return self.__constructor__(query_compiler=self._query_compiler.infer_objects())


@_inherit_docstrings(native_pd.Series.nunique, apilink="pandas.Series")
@register_series_accessor("nunique")
@snowpark_pandas_telemetry_method_decorator
def nunique(self, dropna: bool = True) -> int:
    """
    Return number of unique elements in the series.

    Excludes NA values by default. Snowpark pandas API does not distinguish between different NaN types like None,
    pd.NA or np.nan and treats them as the same

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
    dtype: int8

    >>> s.nunique()
    4

    >>> s = pd.Series([pd.NaT, np.nan, pd.NA, None, 1])
    >>> s.nunique()
    1

    >>> s.nunique(dropna=False)
    2

    """
    # TODO: SNOW-1264688: remove this override
    return super(Series, self).nunique(dropna=dropna)


@_inherit_docstrings(native_pd.Series.isin, apilink="pandas.Series")
@register_series_accessor("isin")
@snowpark_pandas_telemetry_method_decorator
def isin(self, values: Union[set, ListLike]) -> Series:
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
