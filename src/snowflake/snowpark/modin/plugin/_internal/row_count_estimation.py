#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from typing import Any, TYPE_CHECKING
from enum import Enum
from math import ceil

if TYPE_CHECKING:
    from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
        OrderedDataFrame,
    )


class DataFrameOperation(Enum):
    SELECT = "select"
    DROPNA = "dropna"
    UNION_ALL = "union_all"
    GROUP_BY = "group_by"
    SORT = "sort"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    AGG = "agg"
    JOIN = "join"
    ALIGN = "align"
    FILTER = "filter"
    LIMIT = "limit"
    SAMPLE = "sample"


class RowCountEstimator:
    @staticmethod
    def upper_bound(
        df: OrderedDataFrame, operation: DataFrameOperation, args: dict[str, Any]
    ) -> int | None:
        """
        Estimate the new upper bound for the row count after performing an operation
        on the OrderedDataFrame.

        Args:
            df (OrderedDataFrame): The original dataframe on which the operation is executed
            operation (DataFrameOperation): The transformation operation performed
            args (dict): All arguments passed to the operation method

        Returns:
            int: The estimated upper bound on the number of rows in the resulting dataframe
        """
        # Get the current upper bound. If not set, return None
        current = df.row_count_upper_bound
        if current is None:
            return None

        # These operations preserve or reduce the row count, so we can use the current upper bound
        if operation in {
            DataFrameOperation.SELECT,
            DataFrameOperation.DROPNA,
            DataFrameOperation.GROUP_BY,
            DataFrameOperation.SORT,
            DataFrameOperation.PIVOT,
            DataFrameOperation.FILTER,
        }:
            return current

        # Union all combines the row counts of the two dataframes
        elif operation == DataFrameOperation.UNION_ALL:
            other: OrderedDataFrame = args["other"]
            other_bound = other.row_count_upper_bound or other.row_count
            if other_bound is None:
                # Cannot estimate row count: other DataFrame has no row count information
                return None
            return current + other_bound

        # Unpivot creates a new row for each value in the column list
        elif operation == DataFrameOperation.UNPIVOT:
            column_list = args["column_list"]
            return current * len(column_list)

        # Agg aggregates the rows into a single row
        elif operation == DataFrameOperation.AGG:
            return 1

        # TODO: Implement a better estimate by having cases for different join types
        # Join can cause a Cartesian product with the row counts multiplying
        elif operation == DataFrameOperation.JOIN:
            right: OrderedDataFrame = args["right"]
            right_bound = right.row_count_upper_bound or right.row_count
            if right_bound is None:
                # Cannot estimate row count: other DataFrame has no row count information
                return None
            return current * right_bound

        # TODO: Implement a better estimate by having cases for different align types
        # Align can cause a Cartesian product with the row counts multiplying
        elif operation == DataFrameOperation.ALIGN:
            other_df: OrderedDataFrame = args["right"]
            other_bound = other_df.row_count_upper_bound or other_df.row_count
            if other_bound is None:
                # Cannot estimate row count: other DataFrame has no row count information
                return None
            return current * other_bound

        # Limit sets the upper bound to n rows
        elif operation == DataFrameOperation.LIMIT:
            return args["n"]

        # Sample can cause the row count to be set to n or multiplied by a fraction
        elif operation == DataFrameOperation.SAMPLE:
            n, frac = args.get("n"), args.get("frac")
            if n is not None:
                return n
            elif frac is not None:
                return ceil(current * frac)
            else:
                return None

        else:
            raise ValueError(f"Unsupported operation: {operation}")
