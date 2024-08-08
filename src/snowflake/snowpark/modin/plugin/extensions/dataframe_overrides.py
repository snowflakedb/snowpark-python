#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing DataFrame APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `DataFrame.memory_usage`.
"""

from typing import Any

import pandas as native_pd
from modin.pandas import DataFrame
from pandas._typing import Axis, PythonFuncType
from pandas.core.dtypes.common import is_dict_like, is_list_like

from snowflake.snowpark.modin.pandas.api.extensions import register_dataframe_accessor
from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    is_snowflake_agg_func,
)
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_method_decorator,
)
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
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


@register_dataframe_accessor("plot")
@property
@snowpark_pandas_telemetry_method_decorator
def plot(
    self,
    x=None,
    y=None,
    kind="line",
    ax=None,
    subplots=False,
    sharex=None,
    sharey=False,
    layout=None,
    figsize=None,
    use_index=True,
    title=None,
    grid=None,
    legend=True,
    style=None,
    logx=False,
    logy=False,
    loglog=False,
    xticks=None,
    yticks=None,
    xlim=None,
    ylim=None,
    rot=None,
    fontsize=None,
    colormap=None,
    table=False,
    yerr=None,
    xerr=None,
    secondary_y=False,
    sort_columns=False,
    **kwargs,
):  # noqa: PR01, RT01, D200
    """
    Make plots of ``DataFrame``. Materializes data into memory and uses the
    existing pandas PlotAccessor
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    WarningMessage.single_warning(
        "DataFrame.plot materializes data to the local machine for plotting."
    )
    return self._to_pandas().plot


@register_dataframe_accessor("transform")
@snowpark_pandas_telemetry_method_decorator
def transform(
    self, func: PythonFuncType, axis: Axis = 0, *args: Any, **kwargs: Any
) -> DataFrame:  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if is_list_like(func) or is_dict_like(func):
        ErrorMessage.not_implemented(
            "dict and list parameters are not supported for transform"
        )
    # throw the same error as pandas for cases where the function type is
    # invalid.
    if not isinstance(func, str) and not callable(func):
        raise TypeError(f"{type(func)} object is not callable")

    # if the function is an aggregation function, we'll produce
    # some bogus results while pandas will throw the error the
    # code below is throwing. So we do the same.
    if is_snowflake_agg_func(func):
        raise ValueError("Function did not transform")

    return self.apply(func, axis, False, args=args, **kwargs)
