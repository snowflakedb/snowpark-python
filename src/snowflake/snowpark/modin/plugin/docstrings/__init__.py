#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""This module contains objects whose documentation will override Modin's documentation."""

from snowflake.snowpark.modin.plugin.docstrings.base import BasePandasDataset
from snowflake.snowpark.modin.plugin.docstrings.dataframe import DataFrame
from snowflake.snowpark.modin.plugin.docstrings.datetime_index import DatetimeIndex
from snowflake.snowpark.modin.plugin.docstrings.general import *  # noqa: F401,F403
from snowflake.snowpark.modin.plugin.docstrings.groupby import (
    DataFrameGroupBy,
    SeriesGroupBy,
)
from snowflake.snowpark.modin.plugin.docstrings.index import Index
from snowflake.snowpark.modin.plugin.docstrings.io import *  # noqa: F401,F403
from snowflake.snowpark.modin.plugin.docstrings.resample import Resampler
from snowflake.snowpark.modin.plugin.docstrings.series import Series
from snowflake.snowpark.modin.plugin.docstrings.series_utils import (
    CombinedDatetimelikeProperties,
    StringMethods,
)
from snowflake.snowpark.modin.plugin.docstrings.timedelta_index import TimedeltaIndex
from snowflake.snowpark.modin.plugin.docstrings.window import Expanding, Rolling

__all__ = [
    "BasePandasDataset",
    "DataFrame",
    "DataFrameGroupBy",
    "CombinedDatetimelikeProperties",
    "Resampler",
    "Rolling",
    "Expanding",
    "Series",
    "SeriesGroupBy",
    "StringMethods",
    "Index",
    "DatetimeIndex",
    "TimedeltaIndex",
]
