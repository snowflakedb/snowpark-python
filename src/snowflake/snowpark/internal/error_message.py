#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException


class SnowparkClientExceptionMessages:
    """Holds all of the error messages that could be used in the SnowparkClientException Class.

    IMPORTANT: keep this file in numerical order of the error-code."""

    # TODO Add the rest of the exception messages

    # Internal Error messages 001X

    @staticmethod
    def INTERNAL_TEST_MESSAGE(message: str) -> SnowparkClientException:
        return SnowparkClientException(f"internal test message: {message}.", "0010")

    # DataFrame Error Messages 01XX

    @staticmethod
    def DF_CANNOT_DROP_COLUMN_NAME(col_name: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"Unable to drop the column {col_name}. You must specify the column by name "
            f'(e.g. df.drop(col("a"))).',
            "0100",
        )

    @staticmethod
    def DF_SORT_NEED_AT_LEAST_ONE_EXPR() -> SnowparkClientException:
        return SnowparkClientException(
            "For sort(), you must specify at least one sort expression.", "0101"
        )

    @staticmethod
    def DF_CANNOT_DROP_ALL_COLUMNS() -> SnowparkClientException:
        return SnowparkClientException("Cannot drop all columns", "0102")

    @staticmethod
    def DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG(
        col_name: str, all_columns: str
    ) -> SnowparkClientException:
        return SnowparkClientException(
            f'Cannot combine the DataFrames by column names. The column "{col_name}" is '
            f"not a column in the other DataFrame ({all_columns}).",
            "0103",
        )

    @staticmethod
    def DF_SELF_JOIN_NOT_SUPPORTED() -> SnowparkClientException:
        return SnowparkClientException(
            "You cannot join a DataFrame with itself because the column references cannot "
            "be resolved correctly. Instead, call clone() to create a copy of the "
            "DataFrame, and join the DataFrame with this copy.",
            "0104",
        )

    @staticmethod
    def DF_RANDOM_SPLIT_WEIGHT_INVALID() -> SnowparkClientException:
        return SnowparkClientException(
            "The specified weights for randomSplit() must not be negative numbers.",
            "0105",
        )

    @staticmethod
    def DF_RANDOM_SPLIT_WEIGHT_ARRAY_EMPTY() -> SnowparkClientException:
        return SnowparkClientException(
            "You cannot pass an empty array of weights to randomSplit().", "0106"
        )

    @staticmethod
    def DF_FLATTEN_UNSUPPORTED_INPUT_MODE(mode: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"Unsupported input mode {mode}. For the mode parameter in flatten(), you must "
            f"specify OBJECT, ARRAY, or BOTH.",
            "0107",
        )

    @staticmethod
    def DF_CANNOT_RESOLVE_COLUMN_NAME(col_name: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"The DataFrame does not contain the column named {col_name}.", "0108"
        )

    @staticmethod
    def DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE() -> SnowparkClientException:
        return SnowparkClientException(
            "You must call DataFrameReader.schema() and specify the schema for the file.",
            "0109",
        )

    @staticmethod
    def DF_CROSS_TAB_COUNT_TOO_LARGE(
        count: int, max_count: int
    ) -> SnowparkClientException:
        return SnowparkClientException(
            f"The number of distinct values in the second input column ({count}) exceeds "
            f"the maximum number of distinct values allowed ({max_count}).",
            "0110",
        )

    @staticmethod
    def DF_DATAFRAME_IS_NOT_QUALIFIED_FOR_SCALAR_QUERY(
        count: int, columns: str
    ) -> SnowparkClientException:
        return SnowparkClientException(
            f"The DataFrame passed in to this function must have only one output column. "
            f"This DataFrame has {count} output columns: {columns}",
            "0111",
        )

    @staticmethod
    def DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR() -> SnowparkClientException:
        return SnowparkClientException(
            "You can apply only one aggregate expression to a RelationalGroupedDataFrame "
            "returned by the pivot() method.",
            "0112",
        )

    @staticmethod
    def DF_WINDOW_BOUNDARY_START_INVALID(start_value: int) -> SnowparkClientException:
        return SnowparkClientException(
            f"The starting point for the window frame is not a valid integer: {start_value}.",
            "0114",
        )

    @staticmethod
    def DF_WINDOW_BOUNDARY_END_INVALID(end_value: int) -> SnowparkClientException:
        return SnowparkClientException(
            f"The ending point for the window frame is not a valid integer: {end_value}.",
            "0115",
        )

    @staticmethod
    def DF_JOIN_INVALID_JOIN_TYPE(type1: str, types: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"Unsupported join type '{type1}'. Supported join types include: {types}.",
            "0116",
        )

    @staticmethod
    def DF_JOIN_INVALID_NATURAL_JOIN_TYPE(tpe: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"Unsupported natural join type '{tpe}'.", "0117"
        )

    @staticmethod
    def DF_JOIN_INVALID_USING_JOIN_TYPE(tpe: str) -> SnowparkClientException:
        return SnowparkClientException(f"Unsupported using join type '{tpe}'.", "0118")

    # UDF error codes 02XX

    # Plan analysis and execution error codes 03XX

    @staticmethod
    def PLAN_LAST_QUERY_RETURN_RESULTSET() -> SnowparkClientException:
        return SnowparkClientException(
            "Internal error: The execution for the last query "
            "in the Snowflake plan doesn't return a ResultSet.",
            "0300",
        )

    @staticmethod
    def PLAN_SAMPLING_NEED_ONE_PARAMETER() -> SnowparkClientException:
        return SnowparkClientException(
            "You must specify either the fraction of rows or the number of rows to sample.",
            "0303",
        )

    @staticmethod
    def PLAN_PYTHON_REPORT_UNEXPECTED_ALIAS() -> SnowparkClientException:
        return SnowparkClientException(
            "You can only define aliases for the root Columns in a DataFrame returned by "
            "select() and agg(). You cannot use aliases for Columns in expressions.",
            "0308",
        )

    @staticmethod
    def PLAN_PYTHON_REPORT_INVALID_ID(name: str) -> SnowparkClientException:
        return SnowparkClientException(
            f'The column specified in df("{name}") '
            f"is not present in the output of the DataFrame.",
            "0309",
        )

    @staticmethod
    def PLAN_PYTHON_REPORT_JOIN_AMBIGUOUS(c1: str, c2: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"The reference to the column '{c1}' is ambiguous. The column is "
            f"present in both DataFrames used in the join. To identify the "
            f"DataFrame that you want to use in the reference, use the syntax "
            f'<df>("{c2}") in join conditions and in select() calls on the '
            f"result of the join.",
            "0310",
        )

    # Miscellaneous Messages 04XX

    @staticmethod
    def MISC_QUERY_IS_CANCELLED() -> SnowparkClientException:
        return SnowparkClientException(
            "The query has been cancelled by the user.", "0402"
        )

    @staticmethod
    def MISC_SESSION_EXPIRED(err_msg: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"Your Snowpark session has expired. "
            f"You must recreate your session.\n{err_msg}",
            "0408",
        )
