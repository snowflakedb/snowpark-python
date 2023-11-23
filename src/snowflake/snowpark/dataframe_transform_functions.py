#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable, Dict, List, Union

import snowflake.snowpark
from snowflake.snowpark import Column
from snowflake.snowpark.column import _to_col_if_str
from snowflake.snowpark.functions import expr, lag, lead
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
            col_formatter: An optional function to format the output column names. Defaults to a built-in formatter
                        that outputs column names in the format "<input_col>_<agg>_<window>".

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
        self._validate_positive_integer_list(window_sizes, "window_sizes")
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
            col_formatter: An optional function for formatting output column names, defaulting to the format '<input_col>LAG<window>'.
                        This function takes three arguments: 'input_col' (str) for the column name, 'operation' (str) for the applied operation,
                        and 'value' (int) for the operation's numerical value, and returns a formatted string for the column name.

        Returns:
            A Snowflake DataFrame with additional columns corresponding to each specified lag period.
        """
        self._validate_string_list_argument(order_by, "order_by")
        self._validate_string_list_argument(group_by, "group_by")
        self._validate_positive_integer_list(lags, "lags")
        self._validate_formatter_argument(col_formatter)

        window_spec = Window.partition_by(group_by).order_by(order_by)
        lag_df = self._df
        for c in cols:
            for lag_period in lags:
                column = _to_col_if_str(c, "transform.compute_lag")
                lag_col = lag(column, lag_period).over(window_spec)
                formatted_col_name = col_formatter(
                    column.get_name().replace('"', ""), "LAG", lag_period
                )
                lag_df = lag_df.with_column(formatted_col_name, lag_col)

        return lag_df

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
            col_formatter: An optional function for formatting output column names, defaulting to the format '<input_col>LEAD<window>'.
                        This function takes three arguments: 'input_col' (str) for the column name, 'operation' (str) for the applied operation,
                        and 'value' (int) for the operation's numerical value, and returns a formatted string for the column name.

        Returns:
            A Snowflake DataFrame with additional columns corresponding to each specified lead period.
        """
        self._validate_string_list_argument(order_by, "order_by")
        self._validate_string_list_argument(group_by, "group_by")
        self._validate_positive_integer_list(leads, "leads")
        self._validate_formatter_argument(col_formatter)

        window_spec = Window.partition_by(group_by).order_by(order_by)
        lead_df = self._df
        for c in cols:
            for lead_period in leads:
                column = _to_col_if_str(c, "transform.compute_lead")
                lead_col = lead(column, lead_period).over(window_spec)
                formatted_col_name = col_formatter(
                    column.get_name().replace('"', ""), "LEAD", lead_period
                )
                lead_df = lead_df.with_column(formatted_col_name, lead_col)

        return lead_df
