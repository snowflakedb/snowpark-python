#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""This module contains objects whose documentation will override Modin's documentation."""

from snowflake.snowpark.modin.plugin.docstrings.base import BasePandasDataset
from snowflake.snowpark.modin.plugin.docstrings.groupby import (
    DataFrameGroupBy,
    SeriesGroupBy,
)

__all__ = ["BasePandasDataset", "DataFrameGroupBy", "SeriesGroupBy"]
