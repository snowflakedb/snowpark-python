#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pandas._typing import Axis, Level

from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.pandas.api.extensions import (
    register_dataframe_accessor,
    register_series_accessor,
)
from snowflake.snowpark.modin.pandas.utils import raise_if_native_pandas_objects

if TYPE_CHECKING:
    from snowflake.snowpark.modin.pandas import BasePandasDataset

# Methods that, for whatever reason, need to be overridden in both Series and DF.


def _binary_op(
    self,
    op: str,
    other: BasePandasDataset,
    axis: Axis,
    level: Level | None = None,
    fill_value: float | None = None,
    **kwargs: Any,
):
    """
    Do binary operation between two datasets.

    Parameters
    ----------
    op : str
        Name of binary operation.
    other : modin.pandas.BasePandasDataset
        Second operand of binary operation.
    axis: Whether to compare by the index (0 or ‘index’) or columns. (1 or ‘columns’).
    level: Broadcast across a level, matching Index values on the passed MultiIndex level.
    fill_value: Fill existing missing (NaN) values, and any new element needed for
        successful DataFrame alignment, with this value before computation.
        If data in both corresponding DataFrame locations is missing the result will be missing.
        only arithmetic binary operation has this parameter (e.g., add() has, but eq() doesn't have).

    kwargs can contain the following parameters passed in at the frontend:
        func: Only used for `combine` method. Function that takes two series as inputs and
            return a Series or a scalar. Used to merge the two dataframes column by columns.

    Returns
    -------
    modin.pandas.BasePandasDataset
        Result of binary operation.
    """
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    raise_if_native_pandas_objects(other)
    axis = self._get_axis_number(axis)
    squeeze_self = isinstance(self, pd.Series)

    # pandas itself will ignore the axis argument when using Series.<op>.
    # Per default, it is set to axis=0. However, for the case of a Series interacting with
    # a DataFrame the behavior is axis=1. Manually check here for this case and adjust the axis.

    is_lhs_series_and_rhs_dataframe = (
        True
        if isinstance(self, pd.Series) and isinstance(other, pd.DataFrame)
        else False
    )

    new_query_compiler = self._query_compiler.binary_op(
        op=op,
        other=other,
        axis=1 if is_lhs_series_and_rhs_dataframe else axis,
        level=level,
        fill_value=fill_value,
        squeeze_self=squeeze_self,
        **kwargs,
    )

    from snowflake.snowpark.modin.pandas.dataframe import DataFrame

    # Modin Bug: https://github.com/modin-project/modin/issues/7236
    # For a Series interacting with a DataFrame, always return a DataFrame
    return (
        DataFrame(query_compiler=new_query_compiler)
        if is_lhs_series_and_rhs_dataframe
        else self._create_or_update_from_compiler(new_query_compiler)
    )


methods = ["_binary_op"]

for method in methods:
    register_dataframe_accessor(method)(locals()[method])
    register_series_accessor(method)(locals()[method])
