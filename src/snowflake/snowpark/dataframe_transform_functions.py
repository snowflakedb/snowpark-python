#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable, Dict, List, Tuple

import snowflake.snowpark
from snowflake.snowpark.functions import (
    col,
    dateadd,
    expr,
    from_unixtime,
    lit,
    unix_timestamp,
)
from snowflake.snowpark.window import Window


class DataFrameTransformFunctions:
    """Provides data transformation functions for DataFrames.
    To access an object of this class, use :attr:`DataFrame.transform`.
    """

    def __init__(self, df: "snowflake.snowpark.DataFrame") -> None:
        self._df = df

    def _default_col_formatter(input_col: str, operation: str, *args) -> str:
        args_str = "_".join(map(str, args))
        formatted_name = f"{input_col}_{operation}"
        if args_str:
            formatted_name += f"_{args_str}"
        return formatted_name

    def _validate_aggs_argument(self, aggs):
        if not isinstance(aggs, dict):
            raise TypeError("aggs must be a dictionary")
        if not aggs:
            raise ValueError("aggs must not be empty")
        if not all(
            isinstance(key, str) and isinstance(val, list) and val
            for key, val in aggs.items()
        ):
            raise ValueError(
                "aggs must have strings as keys and non-empty lists of strings as values"
            )

    def _validate_string_list_argument(self, data, argument_name):
        if not isinstance(data, list):
            raise TypeError(f"{argument_name} must be a list")
        if not data:
            raise ValueError(f"{argument_name} must not be empty")
        if not all(isinstance(item, str) for item in data):
            raise ValueError(f"{argument_name} must be a list of strings")

    def _validate_positive_integer_list_argument(self, data, argument_name):
        if not isinstance(data, list):
            raise TypeError(f"{argument_name} must be a list")
        if not data:
            raise ValueError(f"{argument_name} must not be empty")
        if not all(isinstance(item, int) and item > 0 for item in data):
            raise ValueError(f"{argument_name} must be a list of integers > 0")

    def _validate_formatter_argument(self, fromatter):
        if not callable(fromatter):
            raise TypeError("formatter must be a callable function")

    def _validate_and_extract_time_unit(
        self, time_str, argument_name, allow_negative=True
    ) -> Tuple[int, str]:
        if not time_str:
            raise ValueError(f"{argument_name} must not be empty")

        duration = int(time_str[:-1])
        unit = time_str[-1].lower()

        if not allow_negative and duration < 0:
            raise ValueError(f"{argument_name} must not be negative.")

        supported_units = ["h", "d", "w", "m", "y"]
        if unit not in supported_units:
            raise ValueError(
                f"Unsupported unit '{unit}'. Supported units are '{supported_units}"
            )

        # Converting month unit to 'mm' for Snowpark
        if unit == "m":
            unit = "mm"

        return duration, unit

    def _get_sliding_interval_start(self, time_col, unit, duration):
        unit_seconds = {"h": 3600, "d": 86400, "w": 604800}

        if unit not in unit_seconds:
            raise ValueError("Invalid unit. Supported units are 'H', 'D', 'W'.")

        interval_seconds = unit_seconds[unit] * duration

        return from_unixtime(
            (unix_timestamp(time_col) / interval_seconds).cast("long")
            * interval_seconds
        )

    def moving_agg(
        self,
        aggs: Dict[str, List[str]],
        window_sizes: List[int],
        order_by: List[str],
        group_by: List[str],
        col_formatter: Callable[[str, str, int], str] = _default_col_formatter,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Applies moving aggregations to the specified columns of the DataFrame using defined window sizes,
        and grouping and ordering criteria.

        Args:
            aggs: A dictionary where keys are column names and values are lists of the desired aggregation functions.
            window_sizes: A list of positive integers, each representing the size of the window for which to
                        calculate the moving aggregate.
            order_by: A list of column names that specify the order in which rows are processed.
            group_by: A list of column names on which the DataFrame is partitioned for separate window calculations.
            col_formatter: An optional function for formatting output column names, defaulting to the format '<input_col>_<agg>_<window>'.
                        This function takes three arguments: 'input_col' (str) for the column name, 'operation' (str) for the applied operation,
                        and 'value' (int) for the window size, and returns a formatted string for the column name.

        Returns:
            A Snowflake DataFrame with additional columns corresponding to each specified moving aggregation.

        Raises:
            ValueError: If an unsupported value is specified in arguments.
            TypeError: If an unsupported type is specified in arguments.
            SnowparkSQLException: If an unsupported aggregration is specified.

        Example:
            aggregated_df = moving_agg(
                aggs={"SALESAMOUNT": ['SUM', 'AVG']},
                window_sizes=[1, 2, 3, 7],
                order_by=['ORDERDATE'],
                group_by=['PRODUCTKEY']
            )
        """
        # Validate input arguments
        self._validate_aggs_argument(aggs)
        self._validate_string_list_argument(order_by, "order_by")
        self._validate_string_list_argument(group_by, "group_by")
        self._validate_positive_integer_list_argument(window_sizes, "window_sizes")
        self._validate_formatter_argument(col_formatter)

        # Perform window aggregation
        agg_df = self._df
        for column, agg_funcs in aggs.items():
            for window_size in window_sizes:
                for agg_func in agg_funcs:
                    window_spec = (
                        Window.partition_by(group_by)
                        .order_by(order_by)
                        .rows_between(-window_size + 1, 0)
                    )

                    # Apply the user-specified aggregation function directly. Snowflake will handle any errors for invalid functions.
                    agg_col = expr(f"{agg_func}({column})").over(window_spec)

                    formatted_col_name = col_formatter(column, agg_func, window_size)
                    agg_df = agg_df.with_column(formatted_col_name, agg_col)

        return agg_df

    def time_series_agg(
        self,
        time_col: str,
        aggs: Dict[str, List[str]],
        windows: List[str],
        group_by: List[str],
        sliding_interval: str,
        col_formatter: Callable[[str, str, int], str] = _default_col_formatter,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Applies aggregations to the specified columns of the DataFrame over specified time windows,
        and grouping criteria.

        Args:
            aggs: A dictionary where keys are column names and values are lists of the desired aggregation functions.
            windows: Time windows for aggregations using strings such as '7D' for 7 days, where the units are
                H: Hours, D: Days, W: Weeks, M: Months, Y: Years. For future-oriented analysis, use positive numbers,
                and for past-oriented analysis, use negative numbers.
            sliding_interval: Interval at which the window slides, specified in the same format as the windows.
                H: Hours, D: Days, W: Weeks.
            group_by: A list of column names on which the DataFrame is partitioned for separate window calculations.
            col_formatter: An optional function for formatting output column names, defaulting to the format '<input_col>_<agg>_<window>'.
                        This function takes three arguments: 'input_col' (str) for the column name, 'operation' (str) for the applied operation,
                        and 'value' (int) for the window size, and returns a formatted string for the column name.

        Returns:
            A Snowflake DataFrame with additional columns corresponding to each specified time window aggregation.

        Raises:
            ValueError: If an unsupported value is specified in arguments.
            TypeError: If an unsupported type is specified in arguments.
            SnowparkSQLException: If an unsupported aggregration is specified.

        Example:
            aggregated_df = df.transform.time_series_agg(
                time_col='ORDERTIME',
                group_by=['PRODUCTKEY'],
                aggs={
                    'SALESAMOUNT': ['SUM', 'MIN', 'MAX']
                },
                sliding_interval='12H',
                windows=['7D', '14D', '-7D', '-14D', '1T']
            )
        """
        self._validate_aggs_argument(aggs)
        self._validate_string_list_argument(group_by, "group_by")
        self._validate_formatter_argument(col_formatter)

        if not windows:
            raise ValueError("windows must not be empty")

        if not sliding_interval:
            raise ValueError("sliding_interval must not be empty")

        if not time_col or not isinstance(time_col, str):
            raise ValueError("time_col must be a string")

        # check for valid time_col names

        slide_duration, slide_unit = self._validate_and_extract_time_unit(
            sliding_interval, "sliding_interval", allow_negative=False
        )
        sliding_point_col = "sliding_point"

        agg_df = self._df
        agg_df = agg_df.withColumn(
            sliding_point_col,
            self._get_sliding_interval_start(time_col, slide_unit, slide_duration),
        )
        agg_df.show()
        agg_exprs = []
        for column, functions in aggs.items():
            for function in functions:
                agg_exprs.append((column, function))

        # Perform the aggregation
        sliding_windows_df = agg_df.groupBy(group_by + [sliding_point_col]).agg(
            agg_exprs
        )

        for window in windows:
            window_duration, window_unit = self._validate_and_extract_time_unit(
                window, "window"
            )

            # Perform self-join on DataFrame for aggregation within each group and time window.
            self_joined_df = sliding_windows_df.alias("A").join(
                sliding_windows_df.alias("B"), on=group_by, how="leftouter"
            )

            window_frame = dateadd(
                window_unit, lit(window_duration), f"{sliding_point_col}A"
            )

            if window_duration > 0:  # Future window
                window_start = col(f"{sliding_point_col}A")
                window_end = window_frame
            else:  # Past window
                window_start = window_frame
                window_end = col(f"{sliding_point_col}A")

            # Filter rows to include only those within the specified time window for aggregation.
            self_joined_df = self_joined_df.filter(
                col(f"{sliding_point_col}B") >= window_start
            ).filter(col(f"{sliding_point_col}B") <= window_end)

            # Perform aggregations as specified in 'aggs'.
            for agg_col, funcs in aggs.items():
                for func in funcs:
                    output_column_name = col_formatter(agg_col, func, window)
                    input_column_name = f"{agg_col}_{func}"

                    agg_column_df = self_joined_df.withColumnRenamed(
                        f"{func}({agg_col})B", input_column_name
                    )

                    agg_expr = expr(f"{func}({input_column_name})").alias(
                        output_column_name
                    )
                    agg_column_df = agg_column_df.groupBy(
                        group_by + [f"{sliding_point_col}A"]
                    ).agg(agg_expr)

                    agg_column_df = agg_column_df.withColumnRenamed(
                        f"{sliding_point_col}A", time_col
                    )

                    agg_df = agg_df.join(
                        agg_column_df, on=group_by + [time_col], how="left"
                    )
        return agg_df
