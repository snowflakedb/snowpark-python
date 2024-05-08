#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing DataFrame APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `DataFrame.memory_usage`.
"""

from typing import Any

import pandas as native_pd

from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.pandas import DataFrame, Series
from snowflake.snowpark.modin.pandas.api.extensions import register_dataframe_accessor
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_method_decorator,
)
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    dataframe_not_implemented,
)
from snowflake.snowpark.modin.utils import _inherit_docstrings


@_inherit_docstrings(native_pd.DataFrame.memory_usage, apilink="pandas.DataFrame")
@register_dataframe_accessor("memory_usage")
@snowpark_pandas_telemetry_method_decorator
def memory_usage(self, index: bool = True, deep: bool = False) -> Any:
    """
    Memory Usage (Dummy Information)

    The memory usage of a snowflake dataframe is not fully implemented.
    This method returns a series like the pandas dataframe to maintain
    compatibility with code which calls this for logging purposes, but
    the values are 0.

    Args:
        index: return dummy index memory usage
        deep: ignored

    Returns:
        Series with zeros for each index and column in the dataframe

    Examples:

        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]})
        >>> df.memory_usage()
        Index        0
        Animal       0
        Max Speed    0
        dtype: int64

        >>> df.memory_usage(index=False)
        Animal       0
        Max Speed    0
        dtype: int64
    """
    # TODO: SNOW-1264697: push implementation down to query compiler
    columns = (["Index"] if index else []) + self._get_columns().array.tolist()
    return native_pd.Series([0] * len(columns), index=columns)


@_inherit_docstrings(native_pd.DataFrame.infer_objects, apilink="pandas.DataFrame")
@register_dataframe_accessor("infer_objects")
@snowpark_pandas_telemetry_method_decorator
@dataframe_not_implemented()
def infer_objects(
    self,
) -> SnowflakeQueryCompiler:  # pragma: no cover # noqa: RT01, D200
    """
    Attempt to infer better dtypes for object columns.
    """
    return self.__constructor__(query_compiler=self._query_compiler.infer_objects())


@_inherit_docstrings(native_pd.DataFrame.nunique, apilink="pandas.DataFrame")
@register_dataframe_accessor("nunique")
@snowpark_pandas_telemetry_method_decorator
def nunique(self, axis: int = 0, dropna: bool = True) -> Series:
    """
    Count number of distinct elements in specified axis.

    Return Series with number of distinct elements. Can ignore NaN
    values. Snowpark pandas API does not distinguish between NaN values and treats them all as the same.

    Parameters
    ----------
    axis : {0 or 'index', 1 or 'columns'}, default 0
        The axis to use. 0 or 'index' for row-wise, 1 or 'columns' for
        column-wise.
    dropna : bool, default True
        Don't include NaN in the counts.

    Returns
    -------
    Series

    Examples
    --------
    >>> import snowflake.snowpark.modin.pandas as pd
    >>> df = pd.DataFrame({'A': [4, 5, 6], 'B': [4, 1, 1]})
    >>> df.nunique()
    A    3
    B    2
    dtype: int8

    >>> df.nunique(axis=1)
    0    1
    1    2
    2    2
    dtype: int8

    >>> df = pd.DataFrame({'A': [None, pd.NA, None], 'B': [1, 2, 1]})
    >>> df.nunique()
    A    0
    B    2
    dtype: int8

    >>> df.nunique(dropna=False)
    A    1
    B    2
    dtype: int8

    """
    # TODO: SNOW-1264688: remove this override
    return super(DataFrame, self).nunique(axis=axis, dropna=dropna)
