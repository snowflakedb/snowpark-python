#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable, Dict, List, Optional

import snowflake.snowpark
from snowflake.snowpark.column import _to_col_if_str
from snowflake.snowpark.functions import avg, expr, rank, when
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

    def encode(
        self,
        cols: List[str],
        encoding_types: List[str],
        targets: Optional[List[str]] = None,
        col_formatter: Callable[[str, str], str] = _default_col_formatter,
    ) -> "snowflake.snowpark.dataframe.DataFrame":
        """
        Encodes columns in the DataFrame using specified encoding types.

        Args:
            cols: A list of column names to encode.
            encoding_types: A list of encoding types corresponding to each column. Supported types include 'label', 'one_hot', and 'target'.
            targets: An optional list of target columns for 'target' encoding. For non-target encodings, None should be specified.
            col_formatter: An optional function for formatting output column names. The default format is '<input_col>_<encoding_type>'.
                        This function takes two arguments: 'input_col' (str) for the column name and 'encoding_type' (str) for the type of encoding,
                        and returns a formatted string for the column name.

        Returns:
            A Snowflake DataFrame with additional columns corresponding to each encoded column.

        Raises:
            ValueError: If an unsupported encoding type is specified or if required parameters for specific encoding types are not provided.
            TypeError: If input arguments are not of expected types.

        Example:
            encoded_df = df.transform.encode(
                cols=['WEATHER_DESCRIPTION', 'ANOTHER_COLUMN'],
                encoding_types=['target', 'one_hot'],
                targets=['DAILY_HIGH_TEMP', None],
                col_formatter=col_formatter_func
            )
        """
        self._validate_string_list_argument(cols, "cols")
        self._validate_string_list_argument(encoding_types, "encoding_types")
        self._validate_formatter_argument(col_formatter)

        if targets is None:
            targets = [None] * len(cols)

        if not (len(cols) == len(encoding_types)):
            raise ValueError("Length of cols, encoding_types must be equal.")

        encoded_df = self._df
        for col_name, encoding_type, target in zip(cols, encoding_types, targets):
            formatted_col_name = col_formatter(col_name, encoding_type)
            column = _to_col_if_str(col_name, "transform.encode")

            if encoding_type == "label":
                # Create a DataFrame with distinct values and their labels
                distinct_values = (
                    self._df.select(column)
                    .distinct()
                    .with_column(
                        formatted_col_name, rank().over(Window.order_by(column))
                    )
                )

                # Add the label encoding to the df.
                distinct_values = distinct_values.with_column_renamed(
                    col_name, "temp_join_key"
                )
                encoded_df = encoded_df.join(
                    distinct_values,
                    encoded_df[col_name] == distinct_values["temp_join_key"],
                )
                encoded_df = encoded_df.drop("temp_join_key")

            elif encoding_type == "one_hot":
                distinct_col_vals = encoded_df.select(column).distinct().collect()
                for row in distinct_col_vals:
                    val = row[0]
                    column_name = formatted_col_name + f"_{val}"
                    encoded_df = encoded_df.with_column(
                        column_name, when(column == val, 1).otherwise(0)
                    )

            elif encoding_type == "target":
                if not target:
                    raise ValueError(
                        "Target column must be specified for target encoding"
                    )
                target_column = _to_col_if_str(target, "transform.encode")
                target_mean = encoded_df.group_by(col_name).agg(
                    avg(target_column).alias(formatted_col_name)
                )
                target_mean = target_mean.with_column_renamed(col_name, "temp_join_key")
                encoded_df = encoded_df.join(
                    target_mean, encoded_df[col_name] == target_mean["temp_join_key"]
                )
                encoded_df = encoded_df.drop("temp_join_key")

            else:
                raise ValueError(
                    f"Invalid encoding type for column {col_name}: {encoding_type}"
                )

        return encoded_df
