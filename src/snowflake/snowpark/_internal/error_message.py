#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark.exceptions import (
    SnowparkColumnException,
    SnowparkCreateViewException,
    SnowparkDataframeException,
    SnowparkDataframeReaderException,
    SnowparkInvalidObjectNameException,
    SnowparkJoinException,
    SnowparkMissingDbOrSchemaException,
    SnowparkPlanException,
    SnowparkQueryCancelledException,
    SnowparkSessionException,
    SnowparkSQLAmbiguousJoinException,
    SnowparkSQLException,
    SnowparkSQLInvalidIdException,
    SnowparkSQLUnexpectedAliasException,
    _SnowparkInternalException,
)


class SnowparkClientExceptionMessages:
    """Holds all of the error messages that could be used in the SnowparkClientException Class.

    IMPORTANT: keep this file in numerical order of the error-code."""

    # TODO Add the rest of the exception messages

    # Internal Error messages 001X

    @staticmethod
    def INTERNAL_TEST_MESSAGE(message: str) -> _SnowparkInternalException:
        return _SnowparkInternalException(f"internal test message: {message}.", "1010")

    # DataFrame Error Messages 01XX

    @staticmethod
    def DF_CANNOT_DROP_COLUMN_NAME(col_name: str) -> SnowparkColumnException:
        return SnowparkColumnException(
            f"Unable to drop the column {col_name}. You must specify the column by name "
            f'(e.g. df.drop(col("a"))).',
            "1100",
        )

    @staticmethod
    def DF_CANNOT_DROP_ALL_COLUMNS() -> SnowparkColumnException:
        return SnowparkColumnException("Cannot drop all columns", "1101")

    @staticmethod
    def DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG(
        col_name: str, all_columns: str
    ) -> SnowparkColumnException:
        return SnowparkColumnException(
            f'Cannot combine the DataFrames by column names. The column "{col_name}" is '
            f"not a column in the other DataFrame ({all_columns}).",
            "1102",
        )

    @staticmethod
    def DF_SELF_JOIN_NOT_SUPPORTED() -> SnowparkJoinException:
        return SnowparkJoinException(
            "You cannot join a DataFrame with itself because the column references cannot "
            "be resolved correctly. Instead, call clone() to create a copy of the "
            "DataFrame, and join the DataFrame with this copy.",
            "1103",
        )

    @staticmethod
    def DF_FLATTEN_UNSUPPORTED_INPUT_MODE(mode: str) -> SnowparkDataframeException:
        return SnowparkDataframeException(
            f"Unsupported input mode {mode}. For the mode parameter in flatten(), you must "
            f"specify OBJECT, ARRAY, or BOTH.",
            "1104",
        )

    @staticmethod
    def DF_CANNOT_RESOLVE_COLUMN_NAME(col_name: str) -> SnowparkColumnException:
        return SnowparkColumnException(
            f"The DataFrame does not contain the column named {col_name}.", "1105"
        )

    @staticmethod
    def DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE() -> SnowparkDataframeReaderException:
        return SnowparkDataframeReaderException(
            "You must call DataFrameReader.schema() and specify the schema for the file.",
            "1106",
        )

    @staticmethod
    def DF_CROSS_TAB_COUNT_TOO_LARGE(
        count: int, max_count: int
    ) -> SnowparkDataframeException:
        return SnowparkDataframeException(
            f"The number of distinct values in the second input column ({count}) exceeds "
            f"the maximum number of distinct values allowed ({max_count}).",
            "1107",
        )

    @staticmethod
    def DF_DATAFRAME_IS_NOT_QUALIFIED_FOR_SCALAR_QUERY(
        count: int, columns: str
    ) -> SnowparkDataframeException:
        return SnowparkDataframeException(
            f"The DataFrame passed in to this function must have only one output column. "
            f"This DataFrame has {count} output columns: {columns}",
            "1108",
        )

    @staticmethod
    def DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR() -> SnowparkDataframeException:
        return SnowparkDataframeException(
            "You can apply only one aggregate expression to a RelationalGroupedDataFrame "
            "returned by the pivot() method.",
            "1109",
        )

    @staticmethod
    def DF_JOIN_INVALID_JOIN_TYPE(type1: str, types: str) -> SnowparkJoinException:
        return SnowparkJoinException(
            f"Unsupported join type '{type1}'. Supported join types include: {types}.",
            "1110",
        )

    @staticmethod
    def DF_JOIN_INVALID_NATURAL_JOIN_TYPE(tpe: str) -> SnowparkJoinException:
        return SnowparkJoinException(f"Unsupported natural join type '{tpe}'.", "1111")

    @staticmethod
    def DF_JOIN_INVALID_USING_JOIN_TYPE(tpe: str) -> SnowparkJoinException:
        return SnowparkJoinException(f"Unsupported using join type '{tpe}'.", "1112")

    # Plan Analysis error codes 02XX

    @staticmethod
    def PLAN_ANALYZER_INVALID_IDENTIFIER(name: str) -> SnowparkPlanException:
        return SnowparkPlanException(f"Invalid identifier {name}", "1200")

    @staticmethod
    def PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE(
        type_name: str,
    ) -> SnowparkPlanException:
        return SnowparkPlanException(
            f"Internal Error: Only PersistedView and LocalTempView are supported. "
            f"view type: {type_name}",
            "1201",
        )

    @staticmethod
    def PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES(value: str) -> SnowparkPlanException:
        return SnowparkPlanException(
            f"The COPY option 'FORCE = {value}' is not supported by the Snowpark library. "
            f"The Snowflake library loads all files, even if the files have been loaded "
            f"previously and have not changed since they were loaded.",
            "1202",
        )

    @staticmethod
    def PLAN_CREATE_VIEW_FROM_DDL_DML_OPERATIONS() -> SnowparkCreateViewException:
        return SnowparkCreateViewException(
            "Your dataframe may include DDL or DML operations. Creating a view from "
            "this DataFrame is currently not supported.",
            "1203",
        )

    @staticmethod
    def PLAN_CREATE_VIEWS_FROM_SELECT_ONLY() -> SnowparkCreateViewException:
        return SnowparkCreateViewException(
            "Creating views from SELECT queries supported only.", "1204"
        )

    @staticmethod
    def PLAN_INVALID_TYPE(type: str) -> SnowparkPlanException:
        return SnowparkPlanException(f"Invalid type, analyze. {type}", "1205")

    # SQL Execution error codes 03XX

    @staticmethod
    def SQL_LAST_QUERY_RETURN_RESULTSET() -> SnowparkSQLException:
        return SnowparkSQLException(
            "Internal error: The execution for the last query "
            "in the Snowflake plan doesn't return a ResultSet.",
            "1300",
        )

    @staticmethod
    def SQL_PYTHON_REPORT_UNEXPECTED_ALIAS() -> SnowparkSQLUnexpectedAliasException:
        return SnowparkSQLUnexpectedAliasException(
            "You can only define aliases for the root Columns in a DataFrame returned by "
            "select() and agg(). You cannot use aliases for Columns in expressions.",
            "1301",
        )

    @staticmethod
    def SQL_PYTHON_REPORT_INVALID_ID(name: str) -> SnowparkSQLInvalidIdException:
        return SnowparkSQLInvalidIdException(
            f'The column specified in df("{name}") '
            f"is not present in the output of the DataFrame.",
            "1302",
        )

    @staticmethod
    def SQL_PYTHON_REPORT_JOIN_AMBIGUOUS(
        c1: str, c2: str
    ) -> SnowparkSQLAmbiguousJoinException:
        return SnowparkSQLAmbiguousJoinException(
            f"The reference to the column '{c1}' is ambiguous. The column is "
            f"present in both DataFrames used in the join. To identify the "
            f"DataFrame that you want to use in the reference, use the syntax "
            f'<df>("{c2}") in join conditions and in select() calls on the '
            f"result of the join.",
            "1303",
        )

    # Server Error Messages 04XX

    @staticmethod
    def SERVER_CANNOT_FIND_CURRENT_DB_OR_SCHEMA(
        v1: str, v2: str, v3: str
    ) -> SnowparkMissingDbOrSchemaException:
        return SnowparkMissingDbOrSchemaException(
            f"The {v1} is not set for the current session. To set this, either run "
            f'session.sql("USE {v2}").collect() or set the {v3} connection property in '
            f"the dict or properties file that you specify when creating a session.",
            "1400",
        )

    @staticmethod
    def SERVER_QUERY_IS_CANCELLED() -> SnowparkQueryCancelledException:
        return SnowparkQueryCancelledException(
            "The query has been cancelled by the user.", "1401"
        )

    @staticmethod
    def SERVER_SESSION_EXPIRED(error_message: str) -> SnowparkSessionException:
        return SnowparkSessionException(
            f"Your Snowpark session has expired. You must recreate your "
            f"session.\n{error_message}",
            "1402",
        )

    @staticmethod
    def SERVER_NO_DEFAULT_SESSION() -> SnowparkSessionException:
        return SnowparkSessionException("No default SnowflakeSession found", "1403")

    @staticmethod
    def SERVER_SESSION_HAS_BEEN_CLOSED() -> SnowparkSessionException:
        return SnowparkSessionException(
            "Cannot perform this operation because the session has been closed.", "1404"
        )

    # General Error codes 15XX

    @staticmethod
    def GENERAL_INVALID_OBJECT_NAME(
        type_name: str,
    ) -> SnowparkInvalidObjectNameException:
        return SnowparkInvalidObjectNameException(
            f"The object name '{type_name}' is invalid.", "1500"
        )
