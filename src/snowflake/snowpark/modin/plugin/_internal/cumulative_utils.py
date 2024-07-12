#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
#
# This file contains utils functions used by cumulative aggregation functions.
#

import functools
from typing import Any, Callable

from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.functions import col, iff, sum as sum_sp
from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    drop_non_numeric_data_columns,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.groupby_utils import (
    check_is_groupby_supported_by_snowflake,
    extract_groupby_column_pandas_labels,
)
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit
from snowflake.snowpark.modin.plugin.compiler import snowflake_query_compiler
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.utils import MODIN_UNNAMED_SERIES_LABEL
from snowflake.snowpark.window import Window


def get_cumagg_col_to_expr_map_axis0(
    internal_frame: InternalFrame,
    cumagg_func: Callable,
    skipna: bool,
) -> dict[SnowparkColumn, SnowparkColumn]:
    """
    Map each input column to to a corresponding expression that computes the cumulative aggregation function on that column when axis = 0.

    Args:
        internal_frame: InternalFrame.
            The internal frame to apply the cumulative aggregation function on.
        cumagg_func: Callable
            The cumulative aggregation function to apply on the internal frame.
        skipna : bool
            Exclude NA/null values. If an entire row/column is NA, the result will be NA.

    Returns:
        Dict[SnowparkColumn, SnowparkColumn]
            Map between Snowpandas column and the corresponding expression that computes the cumulative aggregation function on that column.
    """
    window = Window.order_by(
        internal_frame._modin_frame.row_position_snowflake_quoted_identifier
    ).rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
    if skipna:
        cumagg_col_to_expr_map = {
            snowflake_quoted_id: iff(
                col(snowflake_quoted_id).is_null(),
                pandas_lit(None),
                cumagg_func(snowflake_quoted_id).over(window),
            )
            for snowflake_quoted_id in internal_frame._modin_frame.data_column_snowflake_quoted_identifiers
        }
    else:
        # When skipna is False and the aggregated values (form prior rows) contain any nulls, then the cumulative aggregate is also null.
        # For this reason, we count the number of nulls in the window and compare to zero using the two nested iff's below.
        # Note that this could have also been achieved using COUNT_IF(), but as of this writing it has not been supported by Snowpark yet.
        cumagg_col_to_expr_map = {
            snowflake_quoted_id: iff(
                sum_sp(
                    iff(
                        col(snowflake_quoted_id).is_null(), pandas_lit(1), pandas_lit(0)
                    )
                ).over(window)
                > pandas_lit(0),
                pandas_lit(None),
                cumagg_func(snowflake_quoted_id).over(window),
            )
            for snowflake_quoted_id in internal_frame._modin_frame.data_column_snowflake_quoted_identifiers
        }
    return cumagg_col_to_expr_map


def get_groupby_cumagg_frame_axis0(
    query_compiler: "snowflake_query_compiler.SnowflakeQueryCompiler",
    by: Any,
    axis: int,
    numeric_only: bool,
    groupby_kwargs: dict[str, Any],
    cumagg_func: Callable,
    cumagg_func_name: str,
    ascending: bool = True,
) -> InternalFrame:
    """
    Return the output internal frame after applying the cumulative aggregation function on the input internal frame when axis = 0.

    Args:
        by: mapping, series, callable, label, pd.Grouper, BaseQueryCompiler, list of such.
            Used to determine the groups for the groupby.
        axis : 0 (index), 1 (columns)
        numeric_only: bool
            Include only float, int, boolean columns.
        groupby_kwargs: Dict[str, Any]
            keyword arguments passed for the groupby.
        cumagg_func: Callable
            The cumulative aggregation function to apply on the internal frame.
        cumagg_func_name: str
            The name of the cumulative aggregation function to apply on the internal frame.
        ascending : bool
            If False, process the window in reverse order. Needed for cumcount.

    Returns:
        InternalFrame
            Output internal frame after applying the cumulative aggregation function.
    """
    level = groupby_kwargs.get("level", None)
    dropna = groupby_kwargs.get("dropna", True)

    if not check_is_groupby_supported_by_snowflake(by, level, axis):
        ErrorMessage.not_implemented(
            f"GroupBy {cumagg_func_name} with by = {by}, level = {level} and axis = {axis} is not supported yet in Snowpark pandas."
        )

    if level is not None and level != 0:
        ErrorMessage.not_implemented(
            f"GroupBy {cumagg_func_name} with level = {level} is not supported yet in Snowpark pandas."
        )

    by_list = extract_groupby_column_pandas_labels(query_compiler, by, level)

    qc = query_compiler
    if numeric_only:
        qc = drop_non_numeric_data_columns(query_compiler, by_list)

    by_snowflake_quoted_identifiers_list = [
        # Duplicate labels in by result in a ValueError.
        entry[0]
        for entry in qc._modin_frame.get_snowflake_quoted_identifiers_group_by_pandas_labels(
            by_list
        )
    ]

    window = (
        Window.partition_by(by_snowflake_quoted_identifiers_list)
        .order_by(
            qc._modin_frame.ordered_dataframe.ordering_column_snowflake_quoted_identifiers
        )
        .rows_between(
            Window.UNBOUNDED_PRECEDING if ascending else Window.CURRENT_ROW,
            Window.CURRENT_ROW if ascending else Window.UNBOUNDED_FOLLOWING,
        )
    )

    dropna_cond = functools.reduce(
        lambda combined_col, col: combined_col | col,
        map(
            lambda by_snowflake_quoted_identifier: col(
                by_snowflake_quoted_identifier
            ).is_null(),
            by_snowflake_quoted_identifiers_list,
        ),
    )

    pandas_labels = []
    new_columns = []
    if cumagg_func_name == "cumcount":
        new_col = cumagg_func("*").over(window) - pandas_lit(1)
        if dropna:
            new_col = iff(dropna_cond, pandas_lit(None), new_col)
        if qc._modin_frame.num_index_columns > 1:
            pandas_labels.append(
                (MODIN_UNNAMED_SERIES_LABEL,) * qc._modin_frame.num_index_columns
            )
        else:
            pandas_labels.append(MODIN_UNNAMED_SERIES_LABEL)
        new_columns.append(new_col)
    else:
        for pandas_label, snowflake_quoted_identifier in zip(
            qc._modin_frame.data_column_pandas_labels,
            qc._modin_frame.data_column_snowflake_quoted_identifiers,
        ):
            if snowflake_quoted_identifier not in by_snowflake_quoted_identifiers_list:
                new_col = iff(
                    col(snowflake_quoted_identifier).is_null(),
                    pandas_lit(None),
                    cumagg_func(snowflake_quoted_identifier).over(window),
                )
                if dropna:
                    new_col = iff(dropna_cond, pandas_lit(None), new_col)

                pandas_labels.append(pandas_label)
                new_columns.append(new_col)

    result_frame = qc._modin_frame.project_columns(pandas_labels, new_columns)
    if cumagg_func_name == "cumcount":
        return InternalFrame.create(
            ordered_dataframe=result_frame.ordered_dataframe,
            data_column_pandas_labels=[None],
            data_column_snowflake_quoted_identifiers=result_frame.data_column_snowflake_quoted_identifiers,
            index_column_pandas_labels=result_frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=result_frame.index_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=[None],
        )
    else:
        return result_frame
