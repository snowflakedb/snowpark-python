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
from snowflake.snowpark.modin.pandas.api.extensions import register_dataframe_accessor
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_method_decorator,
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
