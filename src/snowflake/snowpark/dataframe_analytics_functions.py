#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable, Dict, List, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.utils import experimental
from snowflake.snowpark.column import Column, _to_col_if_str
from snowflake.snowpark.functions import (
    add_months,
    builtin,
    col,
    dateadd,
    from_unixtime,
    lag,
    lead,
    lit,
    months_between,
    to_timestamp,
    unix_timestamp,
    year,
)
from snowflake.snowpark.window import Window

# "s" (seconds), "m" (minutes), "h" (hours), "d" (days), "w" (weeks), "mm" (months), "y" (years)
SUPPORTED_TIME_UNITS = ["s", "m", "h", "d", "w", "mm", "y"]


class DataFrameAnalyticsFunctions:
    """Provides data analytics functions for DataFrames.
    To access an object of this class, use :attr:`DataFrame.analytics`.
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
        argument_requirements = (
            "The 'aggs' argument must adhere to the following rules: "
            "1) It must be a dictionary. "
            "2) It must not be empty. "
            "3) All keys must be strings. "
            "4) All values must be non-empty lists of strings."
        )

        if not isinstance(aggs, dict):
            raise TypeError(f"aggs must be a dictionary. {argument_requirements}")
        if not aggs:
            raise ValueError(f"aggs must not be empty. {argument_requirements}")
        if not all(
            isinstance(key, str) and isinstance(val, list) and val
            for key, val in aggs.items()
        ):
            raise ValueError(
                f"aggs must have strings as keys and non-empty lists of strings as values. {argument_requirements}"
            )

    def _validate_string_list_argument(self, data, argument_name):
        argument_requirements = (
            f"The '{argument_name}' argument must adhere to the following rules: "
            "1) It must be a list. "
            "2) It must not be empty. "
            "3) All items in the list must be strings."
        )
        if not isinstance(data, list):
            raise TypeError(f"{argument_name} must be a list. {argument_requirements}")
        if not data:
            raise ValueError(
                f"{argument_name} must not be empty. {argument_requirements}"
            )
        if not all(isinstance(item, str) for item in data):
            raise ValueError(
                f"{argument_name} must be a list of strings. {argument_requirements}"
            )

    def _validate_positive_integer_list_argument(self, data, argument_name):
        argument_requirements = (
            f"The '{argument_name}' argument must adhere to the following criteria: "
            "1) It must be a list. "
            "2) It must not be empty. "
            "3) All items in the list must be positive integers."
        )
        if not isinstance(data, list):
            raise TypeError(f"{argument_name} must be a list. {argument_requirements}")
        if not data:
            raise ValueError(
                f"{argument_name} must not be empty. {argument_requirements}"
            )
        if not all(isinstance(item, int) and item > 0 for item in data):
            raise ValueError(
                f"{argument_name} must be a list of integers > 0. {argument_requirements}"
            )

    def _validate_formatter_argument(self, fromatter):
        if not callable(fromatter):
            raise TypeError("formatter must be a callable function")

    def _compute_window_function(
        self,
        cols: List[Union[str, Column]],
        periods: List[int],
        order_by: List[str],
        group_by: List[str],
        col_formatter: Callable[[str, str, int], str],
        window_func: Callable[[Column, int], Column],
        func_name: str,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Generic function to create window function columns (lag or lead) for the DataFrame.
        Args:
            func_name: Should be either "LEAD" or "LAG".
        """
        self._validate_string_list_argument(order_by, "order_by")
        self._validate_string_list_argument(group_by, "group_by")
        self._validate_positive_integer_list_argument(periods, func_name.lower() + "s")
        self._validate_formatter_argument(col_formatter)

        window_spec = Window.partition_by(group_by).order_by(order_by)
        df = self._df
        col_names = []
        values = []
        for c in cols:
            for period in periods:
                column = _to_col_if_str(c, f"transform.compute_{func_name.lower()}")
                window_col = window_func(column, period).over(window_spec)
                formatted_col_name = col_formatter(
                    column.get_name().replace('"', ""), func_name, period
                )
                col_names.append(formatted_col_name)
                values.append(window_col)

        return df.with_columns(col_names, values)

    def _parse_time_string(self, time_str: str) -> Tuple[int, str]:
        index = len(time_str)
        for i, char in enumerate(time_str):
            if not char.isdigit() and char not in ["+", "-"]:
                index = i
                break

        duration = int(time_str[:index])
        unit = time_str[index:].lower()

        return duration, unit

    def _validate_and_extract_time_unit(
        self, time_str: str, argument_name: str, allow_negative: bool = True
    ) -> Tuple[int, str]:
        argument_requirements = (
            f"The '{argument_name}' argument must adhere to the following criteria: "
            "1) It must not be an empty string. "
            "2) The last character must be a supported time unit. "
            f"Supported units are '{', '.join(SUPPORTED_TIME_UNITS)}'. "
            "3) The preceding characters must represent an integer. "
            "4) The integer must not be negative if allow_negative is False."
        )
        if not time_str:
            raise ValueError(
                f"{argument_name} must not be empty. {argument_requirements}"
            )

        duration, unit = self._parse_time_string(time_str)

        if not allow_negative and duration < 0:
            raise ValueError(
                f"{argument_name} must not be negative. {argument_requirements}"
            )

        if unit not in SUPPORTED_TIME_UNITS:
            raise ValueError(
                f"Unsupported unit '{unit}'. Supported units are '{SUPPORTED_TIME_UNITS}. {argument_requirements}"
            )

        return duration, unit

    def _get_sliding_interval_start(
        self, time_col: Column, unit: str, duration: int
    ) -> Column:
        unit_seconds = {
            "s": 1,  # seconds
            "m": 60,  # minutes
            "h": 3600,  # hours
            "d": 86400,  # days
            "w": 604800,  # weeks
        }

        if unit == "mm":
            base_date = lit("1970-01-01").cast("date")
            months_since_base = months_between(time_col, base_date)
            current_window_start_month = (months_since_base / duration).cast(
                "long"
            ) * duration
            return to_timestamp(add_months(base_date, current_window_start_month))

        elif unit == "y":
            base_date = lit("1970-01-01").cast("date")
            years_since_base = year(time_col) - year(base_date)
            current_window_start_year = (years_since_base / duration).cast(
                "long"
            ) * duration
            return to_timestamp(add_months(base_date, current_window_start_year * 12))

        elif unit in unit_seconds:
            # Handle seconds, minutes, hours, days, weeks
            interval_seconds = unit_seconds[unit] * duration
            return from_unixtime(
                (unix_timestamp(time_col) / interval_seconds).cast("long")
                * interval_seconds
            )

        else:
            raise ValueError(
                "Invalid unit. Supported units are 'S', 'M', 'H', 'D', 'W', 'MM', 'Y'."
            )

    def _perform_window_aggregations(
        self,
        base_df: "snowflake.snowpark.dataframe.DataFrame",
        input_df: "snowflake.snowpark.dataframe.DataFrame",
        aggs: Dict[str, List[str]],
        group_by_cols: List[str],
        col_formatter: Callable[[str, str, str], str] = None,
        window: str = None,
        rename_suffix: str = "",
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Perform window-based aggregations on the given DataFrame.

        This function applies specified aggregation functions to columns of an input DataFrame,
        grouped by specified columns, and joins the results back to a base DataFrame.

        Parameters:
        - base_df: DataFrame to which the aggregated results will be joined.
        - input_df: DataFrame on which aggregations are to be performed.
        - aggs: A dictionary where keys are column names and values are lists of aggregation functions.
        - group_by_cols: List of column names to group by.
        - col_formatter: Optional callable to format column names of aggregated results.
        - window: Optional window specification for aggregations.
        - rename_suffix: Optional suffix to append to column names.

        Returns:
        - DataFrame with the aggregated data joined to the base DataFrame.
        """
        for column, funcs in aggs.items():
            for func in funcs:
                agg_column_name = (
                    col_formatter(column, func, window)
                    if col_formatter
                    else f"{column}_{func}{rename_suffix}"
                )
                agg_expression = builtin(func)(col(column + rename_suffix)).alias(
                    agg_column_name
                )
                agg_df = input_df.group_by(group_by_cols).agg(agg_expression)
                base_df = base_df.join(agg_df, on=group_by_cols, how="left")

        return base_df

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
                Supported aggregation are listed here https://docs.snowflake.com/en/sql-reference/functions-analytic#list-of-functions-that-support-windows.
            window_sizes: A list of positive integers, each representing the size of the window for which to
                        calculate the moving aggregate.
            order_by: A list of column names that specify the order in which rows are processed.
            group_by: A list of column names on which the DataFrame is partitioned for separate window calculations.
            col_formatter: An optional function for formatting output column names, defaulting to the format '<input_col>_<agg>_<window>'.
                        This function takes three arguments: 'input_col' (str) for the column name, 'operation' (str) for the applied operation,
                        and 'value' (int) for the window size, and returns a formatted string for the column name.

        Returns:
            A Snowpark DataFrame with additional columns corresponding to each specified moving aggregation.

        Raises:
            ValueError: If an unsupported value is specified in arguments.
            TypeError: If an unsupported type is specified in arguments.
            SnowparkSQLException: If an unsupported aggregration is specified.

        Example:
            >>> data = [
            ...     ["2023-01-01", 101, 200],
            ...     ["2023-01-02", 101, 100],
            ...     ["2023-01-03", 101, 300],
            ...     ["2023-01-04", 102, 250],
            ... ]
            >>> df = session.create_dataframe(data).to_df(
            ...     "ORDERDATE", "PRODUCTKEY", "SALESAMOUNT"
            ... )
            >>> result = df.analytics.moving_agg(
            ...     aggs={"SALESAMOUNT": ["SUM", "AVG"]},
            ...     window_sizes=[2, 3],
            ...     order_by=["ORDERDATE"],
            ...     group_by=["PRODUCTKEY"],
            ... )
            >>> result.show()
            --------------------------------------------------------------------------------------------------------------------------------------
            |"ORDERDATE"  |"PRODUCTKEY"  |"SALESAMOUNT"  |"SALESAMOUNT_SUM_2"  |"SALESAMOUNT_AVG_2"  |"SALESAMOUNT_SUM_3"  |"SALESAMOUNT_AVG_3"  |
            --------------------------------------------------------------------------------------------------------------------------------------
            |2023-01-04   |102           |250            |250                  |250.000              |250                  |250.000              |
            |2023-01-01   |101           |200            |200                  |200.000              |200                  |200.000              |
            |2023-01-02   |101           |100            |300                  |150.000              |300                  |150.000              |
            |2023-01-03   |101           |300            |400                  |200.000              |600                  |200.000              |
            --------------------------------------------------------------------------------------------------------------------------------------
            <BLANKLINE>
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
                    agg_col = builtin(agg_func)(col(column)).over(window_spec)

                    formatted_col_name = col_formatter(column, agg_func, window_size)
                    agg_df = agg_df.with_column(formatted_col_name, agg_col)

        return agg_df

    def cumulative_agg(
        self,
        aggs: Dict[str, List[str]],
        group_by: List[str],
        order_by: List[str],
        is_forward: bool,
        col_formatter: Callable[[str, str], str] = _default_col_formatter,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Applies cummulative aggregations to the specified columns of the DataFrame using defined window direction,
        and grouping and ordering criteria.

        Args:
            aggs: A dictionary where keys are column names and values are lists of the desired aggregation functions.
            order_by: A list of column names that specify the order in which rows are processed.
            group_by: A list of column names on which the DataFrame is partitioned for separate window calculations.
            is_forward: A boolean indicating the direction of accumulation. True for 'forward' and False for 'backward'.
            col_formatter: An optional function for formatting output column names, defaulting to the format '<input_col>_<agg>'.
                        This function takes two arguments: 'input_col' (str) for the column name, 'operation' (str) for the applied operation,
                        and returns a formatted string for the column name.

        Returns:
            A Snowflake DataFrame with additional columns corresponding to each specified cumulative aggregation.

        Raises:
            ValueError: If an unsupported value is specified in arguments.
            TypeError: If an unsupported type is specified in arguments.
            SnowparkSQLException: If an unsupported aggregration is specified.

        Example:
            >>> sample_data = [
            ...     ["2023-01-01", 101, 200],
            ...     ["2023-01-02", 101, 100],
            ...     ["2023-01-03", 101, 300],
            ...     ["2023-01-04", 102, 250],
            ... ]
            >>> df = session.create_dataframe(sample_data).to_df(
            ...     "ORDERDATE", "PRODUCTKEY", "SALESAMOUNT"
            ... )
            >>> res = df.analytics.cumulative_agg(
            ...     aggs={"SALESAMOUNT": ["SUM", "MIN", "MAX"]},
            ...     group_by=["PRODUCTKEY"],
            ...     order_by=["ORDERDATE"],
            ...     is_forward=True
            ... )
            >>> res.show()
            ----------------------------------------------------------------------------------------------------------
            |"ORDERDATE"  |"PRODUCTKEY"  |"SALESAMOUNT"  |"SALESAMOUNT_SUM"  |"SALESAMOUNT_MIN"  |"SALESAMOUNT_MAX"  |
            ----------------------------------------------------------------------------------------------------------
            |2023-01-03   |101           |300            |300                |300                |300                |
            |2023-01-02   |101           |100            |400                |100                |300                |
            |2023-01-01   |101           |200            |600                |100                |300                |
            |2023-01-04   |102           |250            |250                |250                |250                |
            ----------------------------------------------------------------------------------------------------------
            <BLANKLINE>
        """
        # Validate input arguments
        self._validate_aggs_argument(aggs)
        self._validate_string_list_argument(order_by, "order_by")
        self._validate_string_list_argument(group_by, "group_by")
        self._validate_formatter_argument(col_formatter)

        window_spec = Window.partition_by(group_by).order_by(order_by)
        if is_forward:
            window_spec = window_spec.rows_between(0, Window.UNBOUNDED_FOLLOWING)
        else:
            window_spec = window_spec.rows_between(Window.UNBOUNDED_PRECEDING, 0)

        # Perform cumulative aggregation
        agg_df = self._df
        for column, agg_funcs in aggs.items():
            for agg_func in agg_funcs:
                # Apply the user-specified aggregation function directly. Snowflake will handle any errors for invalid functions.
                agg_col = builtin(agg_func)(col(column)).over(window_spec)

                formatted_col_name = col_formatter(column, agg_func)
                agg_df = agg_df.with_column(formatted_col_name, agg_col)

        return agg_df

    def compute_lag(
        self,
        cols: List[Union[str, Column]],
        lags: List[int],
        order_by: List[str],
        group_by: List[str],
        col_formatter: Callable[[str, str, int], str] = _default_col_formatter,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Creates lag columns to the specified columns of the DataFrame by grouping and ordering criteria.

        Args:
            cols: List of column names or Column objects to calculate lag features.
            lags: List of positive integers specifying periods to lag by.
            order_by: A list of column names that specify the order in which rows are processed.
            group_by: A list of column names on which the DataFrame is partitioned for separate window calculations.
            col_formatter: An optional function for formatting output column names, defaulting to the format '<input_col>LAG<lag>'.
                        This function takes three arguments: 'input_col' (str) for the column name, 'operation' (str) for the applied operation,
                        and 'value' (int) for lag value, and returns a formatted string for the column name.

        Returns:
            A Snowflake DataFrame with additional columns corresponding to each specified lag period.

        Example:
            >>> sample_data = [
            ...     ["2023-01-01", 101, 200],
            ...     ["2023-01-02", 101, 100],
            ...     ["2023-01-03", 101, 300],
            ...     ["2023-01-04", 102, 250],
            ... ]
            >>> df = session.create_dataframe(sample_data).to_df(
            ...     "ORDERDATE", "PRODUCTKEY", "SALESAMOUNT"
            ... )
            >>> res = df.analytics.compute_lag(
            ...     cols=["SALESAMOUNT"],
            ...     lags=[1, 2],
            ...     order_by=["ORDERDATE"],
            ...     group_by=["PRODUCTKEY"],
            ... )
            >>> res.show()
            ------------------------------------------------------------------------------------------
            |"ORDERDATE"  |"PRODUCTKEY"  |"SALESAMOUNT"  |"SALESAMOUNT_LAG_1"  |"SALESAMOUNT_LAG_2"  |
            ------------------------------------------------------------------------------------------
            |2023-01-04   |102           |250            |NULL                 |NULL                 |
            |2023-01-01   |101           |200            |NULL                 |NULL                 |
            |2023-01-02   |101           |100            |200                  |NULL                 |
            |2023-01-03   |101           |300            |100                  |200                  |
            ------------------------------------------------------------------------------------------
            <BLANKLINE>
        """
        return self._compute_window_function(
            cols, lags, order_by, group_by, col_formatter, lag, "LAG"
        )

    def compute_lead(
        self,
        cols: List[Union[str, Column]],
        leads: List[int],
        order_by: List[str],
        group_by: List[str],
        col_formatter: Callable[[str, str, int], str] = _default_col_formatter,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Creates lead columns to the specified columns of the DataFrame by grouping and ordering criteria.

        Args:
            cols: List of column names or Column objects to calculate lead features.
            leads: List of positive integers specifying periods to lead by.
            order_by: A list of column names that specify the order in which rows are processed.
            group_by: A list of column names on which the DataFrame is partitioned for separate window calculations.
            col_formatter: An optional function for formatting output column names, defaulting to the format '<input_col>LEAD<lead>'.
                        This function takes three arguments: 'input_col' (str) for the column name, 'operation' (str) for the applied operation,
                        and 'value' (int) for the lead value, and returns a formatted string for the column name.

        Returns:
            A Snowflake DataFrame with additional columns corresponding to each specified lead period.

        Example:
            >>> sample_data = [
            ...     ["2023-01-01", 101, 200],
            ...     ["2023-01-02", 101, 100],
            ...     ["2023-01-03", 101, 300],
            ...     ["2023-01-04", 102, 250],
            ... ]
            >>> df = session.create_dataframe(sample_data).to_df(
            ...     "ORDERDATE", "PRODUCTKEY", "SALESAMOUNT"
            ... )
            >>> res = df.analytics.compute_lead(
            ...     cols=["SALESAMOUNT"],
            ...     leads=[1, 2],
            ...     order_by=["ORDERDATE"],
            ...     group_by=["PRODUCTKEY"]
            ... )
            >>> res.show()
            --------------------------------------------------------------------------------------------
            |"ORDERDATE"  |"PRODUCTKEY"  |"SALESAMOUNT"  |"SALESAMOUNT_LEAD_1"  |"SALESAMOUNT_LEAD_2"  |
            --------------------------------------------------------------------------------------------
            |2023-01-04   |102           |250            |NULL                  |NULL                  |
            |2023-01-01   |101           |200            |100                   |300                   |
            |2023-01-02   |101           |100            |300                   |NULL                  |
            |2023-01-03   |101           |300            |NULL                  |NULL                  |
            --------------------------------------------------------------------------------------------
            <BLANKLINE>
        """
        return self._compute_window_function(
            cols, leads, order_by, group_by, col_formatter, lead, "LEAD"
        )

    @experimental(version="1.12.0")
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
                S: Seconds, M: Minutes, H: Hours, D: Days, W: Weeks, MM: Months, Y: Years. For future-oriented analysis, use positive numbers,
                and for past-oriented analysis, use negative numbers.
            sliding_interval: Interval at which the window slides, specified in the same format as the windows.
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
            >>> sample_data = [
            ...     ["2023-01-01", 101, 200],
            ...     ["2023-01-02", 101, 100],
            ...     ["2023-01-03", 101, 300],
            ...     ["2023-01-04", 102, 250],
            ... ]
            >>> df = session.create_dataframe(sample_data).to_df(
            ...     "ORDERDATE", "PRODUCTKEY", "SALESAMOUNT"
            ... )
            >>> df = df.with_column("ORDERDATE", to_timestamp(df["ORDERDATE"]))
            >>> def custom_formatter(input_col, agg, window):
            ...     return f"{agg}_{input_col}_{window}"
            >>> res = df.analytics.time_series_agg(
            ...     time_col="ORDERDATE",
            ...     group_by=["PRODUCTKEY"],
            ...     aggs={"SALESAMOUNT": ["SUM", "MAX"]},
            ...     windows=["1D", "-1D"],
            ...     sliding_interval="12H",
            ...     col_formatter=custom_formatter,
            ... )
            >>> res.show()
            --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            |"PRODUCTKEY"  |"SLIDING_POINT"      |"SALESAMOUNT"  |"ORDERDATE"          |"SUM_SALESAMOUNT_1D"  |"MAX_SALESAMOUNT_1D"  |"SUM_SALESAMOUNT_-1D"  |"MAX_SALESAMOUNT_-1D"  |
            --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            |101           |2023-01-01 00:00:00  |200            |2023-01-01 00:00:00  |300                   |200                   |200                    |200                    |
            |101           |2023-01-02 00:00:00  |100            |2023-01-02 00:00:00  |400                   |300                   |300                    |200                    |
            |101           |2023-01-03 00:00:00  |300            |2023-01-03 00:00:00  |300                   |300                   |400                    |300                    |
            |102           |2023-01-04 00:00:00  |250            |2023-01-04 00:00:00  |250                   |250                   |250                    |250                    |
            --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
            <BLANKLINE>
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

        slide_duration, slide_unit = self._validate_and_extract_time_unit(
            sliding_interval, "sliding_interval", allow_negative=False
        )
        sliding_point_col = "sliding_point"

        agg_df = self._df
        agg_df = agg_df.with_column(
            sliding_point_col,
            self._get_sliding_interval_start(time_col, slide_unit, slide_duration),
        )

        # Perform aggregations at sliding interval granularity.
        group_by_cols = group_by + [sliding_point_col]
        sliding_windows_df = self._perform_window_aggregations(
            agg_df, agg_df, aggs, group_by_cols
        )

        # Perform aggregations at window intervals.
        result_df = agg_df
        for window in windows:
            window_duration, window_unit = self._validate_and_extract_time_unit(
                window, "window"
            )
            # Perform self-join on DataFrame for aggregation within each group and time window.
            left_df = sliding_windows_df.alias("A")
            right_df = sliding_windows_df.alias("B")

            for column in right_df.columns:
                if column not in group_by:
                    right_df = right_df.with_column_renamed(column, f"{column}B")

            self_joined_df = left_df.join(right_df, on=group_by, how="leftouter")

            window_frame = dateadd(
                window_unit, lit(window_duration), f"{sliding_point_col}"
            )

            if window_duration > 0:  # Future window
                window_start = col(f"{sliding_point_col}")
                window_end = window_frame
            else:  # Past window
                window_start = window_frame
                window_end = col(f"{sliding_point_col}")

            # Filter rows to include only those within the specified time window for aggregation.
            self_joined_df = self_joined_df.filter(
                col(f"{sliding_point_col}B") >= window_start
            ).filter(col(f"{sliding_point_col}B") <= window_end)

            # Peform final aggregations.
            group_by_cols = group_by + [sliding_point_col]
            result_df = self._perform_window_aggregations(
                result_df,
                self_joined_df,
                aggs,
                group_by_cols,
                col_formatter,
                window,
                rename_suffix="B",
            )

        return result_df
