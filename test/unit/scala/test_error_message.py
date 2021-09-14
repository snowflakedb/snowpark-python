#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import pytest

from snowflake.snowpark.internal.error_message import SnowparkClientExceptionMessages


def test_internal_test_message():
    message = "a generic message"
    ex = SnowparkClientExceptionMessages.INTERNAL_TEST_MESSAGE(message)
    assert ex.error_code == "0010"
    assert ex.message == f"internal test message: {message}."


def test_df_cannot_drop_column_name():
    col_name = "C1"
    ex = SnowparkClientExceptionMessages.DF_CANNOT_DROP_COLUMN_NAME(col_name)
    assert ex.error_code == "0100"
    assert (
        ex.message
        == f"Unable to drop the column {col_name}. You must specify the column by name "
        f'(e.g. df.drop(col("a"))).'
    )


def test_df_sort_need_at_least_one_expr():
    ex = SnowparkClientExceptionMessages.DF_SORT_NEED_AT_LEAST_ONE_EXPR()
    assert ex.error_code == "0101"
    assert ex.message == "For sort(), you must specify at least one sort expression."


def test_df_cannot_drop_all_columns():
    ex = SnowparkClientExceptionMessages.DF_CANNOT_DROP_ALL_COLUMNS()
    assert ex.error_code == "0102"
    assert ex.message == "Cannot drop all columns"


def test_df_cannot_resolve_column_name_among():
    col_name = "C1"
    all_columns = ", ".join(["A1", "B1", "D1"])
    ex = SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG(
        col_name, all_columns
    )
    assert ex.error_code == "0103"
    assert (
        ex.message
        == f'Cannot combine the DataFrames by column names. The column "{col_name}" is '
        f"not a column in the other DataFrame ({all_columns})."
    )


def test_df_join_not_supported():
    ex = SnowparkClientExceptionMessages.DF_SELF_JOIN_NOT_SUPPORTED()
    assert ex.error_code == "0104"
    assert (
        ex.message
        == "You cannot join a DataFrame with itself because the column references cannot "
        "be resolved correctly. Instead, call clone() to create a copy of the "
        "DataFrame, and join the DataFrame with this copy."
    )


def test_df_random_split_weight_invalid():
    ex = SnowparkClientExceptionMessages.DF_RANDOM_SPLIT_WEIGHT_INVALID()
    assert ex.error_code == "0105"
    assert (
        ex.message
        == "The specified weights for randomSplit() must not be negative numbers."
    )


def test_df_random_split_weight_array_empty():
    ex = SnowparkClientExceptionMessages.DF_RANDOM_SPLIT_WEIGHT_ARRAY_EMPTY()
    assert ex.error_code == "0106"
    assert ex.message == "You cannot pass an empty array of weights to randomSplit()."


def test_df_flatten_unsupported_input_mode():
    mode = "JSON"
    ex = SnowparkClientExceptionMessages.DF_FLATTEN_UNSUPPORTED_INPUT_MODE(mode)
    assert ex.error_code == "0107"
    assert (
        ex.message
        == f"Unsupported input mode {mode}. For the mode parameter in flatten(), you must "
        f"specify OBJECT, ARRAY, or BOTH."
    )


def test_df_cannot_resolve_column_name():
    col_name = "C1"
    ex = SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME(col_name)
    assert ex.error_code == "0108"
    assert ex.message == f"The DataFrame does not contain the column named {col_name}."


def test_df_must_provide_schema_for_reading_file():
    ex = SnowparkClientExceptionMessages.DF_MUST_PROVIDE_SCHEMA_FOR_READING_FILE()
    assert ex.error_code == "0109"
    assert (
        ex.message
        == "You must call DataFrameReader.schema() and specify the schema for the file."
    )


def test_df_cross_tab_count_too_large():
    count = 100
    max_count = 99
    ex = SnowparkClientExceptionMessages.DF_CROSS_TAB_COUNT_TOO_LARGE(count, max_count)
    assert ex.error_code == "0110"
    assert (
        ex.message
        == f"The number of distinct values in the second input column ({count}) exceeds "
        f"the maximum number of distinct values allowed ({max_count})."
    )


def test_df_dataframe_is_not_qualified_for_scalar_query():
    count = 3
    columns = ", ".join(["A1", "B1", "C1"])
    ex = SnowparkClientExceptionMessages.DF_DATAFRAME_IS_NOT_QUALIFIED_FOR_SCALAR_QUERY(
        count, columns
    )
    assert ex.error_code == "0111"
    assert (
        ex.message
        == f"The DataFrame passed in to this function must have only one output column. "
        f"This DataFrame has {count} output columns: {columns}"
    )


def test_df_pivot_only_support_one_agg_expr():
    ex = SnowparkClientExceptionMessages.DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR()
    assert ex.error_code == "0112"
    assert (
        ex.message
        == "You can apply only one aggregate expression to a RelationalGroupedDataFrame "
        "returned by the pivot() method."
    )


def test_df_window_boundary_start_invalid():
    start_value = -1
    ex = SnowparkClientExceptionMessages.DF_WINDOW_BOUNDARY_START_INVALID(start_value)
    assert ex.error_code == "0114"
    assert (
        ex.message
        == f"The starting point for the window frame is not a valid integer: {start_value}."
    )


def test_df_window_boundary_end_invalid():
    end_value = 100000000000
    ex = SnowparkClientExceptionMessages.DF_WINDOW_BOUNDARY_END_INVALID(end_value)
    assert ex.error_code == "0115"
    assert (
        ex.message
        == f"The ending point for the window frame is not a valid integer: {end_value}."
    )


def test_df_join_invalid_join_type():
    type1 = "inner"
    types = "outer, left, right"
    ex = SnowparkClientExceptionMessages.DF_JOIN_INVALID_JOIN_TYPE(type1, types)
    assert ex.error_code == "0116"
    assert (
        ex.message
        == f"Unsupported join type '{type1}'. Supported join types include: {types}."
    )


def test_df_join_invalid_natural_join_type():
    tpe = "inner"
    ex = SnowparkClientExceptionMessages.DF_JOIN_INVALID_NATURAL_JOIN_TYPE(tpe)
    assert ex.error_code == "0117"
    assert ex.message == f"Unsupported natural join type '{tpe}'."


def test_df_join_invalid_using_join_type():
    tpe = "inner"
    ex = SnowparkClientExceptionMessages.DF_JOIN_INVALID_USING_JOIN_TYPE(tpe)
    assert ex.error_code == "0118"
    assert ex.message == f"Unsupported using join type '{tpe}'."


def test_plan_last_query_return_resultset():
    ex = SnowparkClientExceptionMessages.PLAN_LAST_QUERY_RETURN_RESULTSET()
    assert ex.error_code == "0300"
    assert (
        ex.message == "Internal error: The execution for the last query "
        "in the Snowflake plan doesn't return a ResultSet."
    )


def test_plan_analyzer_invalid_identifier():
    name = "c1"
    ex = SnowparkClientExceptionMessages.PLAN_ANALYZER_INVALID_IDENTIFIER(name)
    assert ex.error_code == "0301"
    assert ex.message == f"Invalid identifier {name}"


def test_plan_unsupported_view_type():
    type_name = "MaterializedView"
    ex = SnowparkClientExceptionMessages.PLAN_ANALYZER_UNSUPPORTED_VIEW_TYPE(type_name)
    assert ex.error_code == "0302"
    assert (
        ex.message
        == f"Internal Error: Only PersistedView and LocalTempView are supported. "
        f"view type: {type_name}"
    )


def test_plan_sampling_need_one_parameter():
    ex = SnowparkClientExceptionMessages.PLAN_SAMPLING_NEED_ONE_PARAMETER()
    assert ex.error_code == "0303"
    assert (
        ex.message
        == "You must specify either the fraction of rows or the number of rows to sample."
    )


def test_plan_python_report_unexpected_alias():
    ex = SnowparkClientExceptionMessages.PLAN_PYTHON_REPORT_UNEXPECTED_ALIAS()
    assert ex.error_code == "0308"
    assert (
        ex.message
        == "You can only define aliases for the root Columns in a DataFrame returned by "
        "select() and agg(). You cannot use aliases for Columns in expressions."
    )


def test_plan_python_report_invalid_id():
    name = "C1"
    ex = SnowparkClientExceptionMessages.PLAN_PYTHON_REPORT_INVALID_ID(name)
    assert ex.error_code == "0309"
    assert (
        ex.message
        == f'The column specified in df("{name}") is not present in the output of the DataFrame.'
    )


def test_plan_report_join_ambiguous():
    column = "A"
    c1 = column
    c2 = column
    ex = SnowparkClientExceptionMessages.PLAN_PYTHON_REPORT_JOIN_AMBIGUOUS(c1, c2)
    assert ex.error_code == "0310"
    assert (
        ex.message == f"The reference to the column '{c1}' is ambiguous. The column is "
        f"present in both DataFrames used in the join. To identify the "
        f"DataFrame that you want to use in the reference, use the syntax "
        f'<df>("{c2}") in join conditions and in select() calls on the '
        f"result of the join."
    )


def test_plan_copy_dont_support_skip_loaded_files():
    value = "False"
    ex = SnowparkClientExceptionMessages.PLAN_COPY_DONT_SUPPORT_SKIP_LOADED_FILES(value)
    assert ex.error_code == "0311"
    assert (
        ex.message
        == f"The COPY option 'FORCE = {value}' is not supported by the Snowpark library. "
        f"The Snowflake library loads all files, even if the files have been loaded "
        f"previously and have not changed since they were loaded."
    )


def test_plan_create_view_from_ddl_dml_operations():
    ex = SnowparkClientExceptionMessages.PLAN_CREATE_VIEW_FROM_DDL_DML_OPERATIONS()
    assert ex.error_code == "0314"
    assert (
        ex.message
        == "Your dataframe may include DDL or DML operations. Creating a view from "
        "this DataFrame is currently not supported."
    )


def test_plan_create_views_from_select_only():
    ex = SnowparkClientExceptionMessages.PLAN_CREATE_VIEWS_FROM_SELECT_ONLY()
    assert ex.error_code == "0315"
    assert ex.message == "Creating views from SELECT queries supported only."


def test_misc_cannot_cast_value():
    source_type = "List"
    value = "[0, 1, 2]"
    target_type = "int"
    ex = SnowparkClientExceptionMessages.MISC_CANNOT_CAST_VALUE(
        source_type, value, target_type
    )
    assert ex.error_code == "0400"
    assert ex.message == f"Cannot cast {source_type}({value}) to {target_type}."


def test_misc_cannot_find_current_db_or_schema():
    v1 = "SCHEMA"
    v2 = v1
    v3 = v1
    ex = SnowparkClientExceptionMessages.MISC_CANNOT_FIND_CURRENT_DB_OR_SCHEMA(
        v1, v2, v3
    )
    assert ex.error_code == "0401"
    assert (
        ex.message
        == f"The {v1} is not set for the current session. To set this, either run "
        f'session.sql("USE {v2}").collect() or set the {v3} connection property in '
        f"the Map or properties file that you specify when creating a session."
    )


def test_misc_query_is_cancelled():
    ex = SnowparkClientExceptionMessages.MISC_QUERY_IS_CANCELLED()
    assert ex.error_code == "0402"
    assert ex.message == "The query has been cancelled by the user."


def test_misc_session_expired():
    error_message = "No valid session left"
    ex = SnowparkClientExceptionMessages.MISC_SESSION_EXPIRED(error_message)
    assert ex.error_code == "0408"
    assert (
        ex.message == f"Your Snowpark session has expired. You must recreate your "
        f"session.\n{error_message}"
    )


def test_misc_invalid_object_name():
    type_name = "Iterable"
    ex = SnowparkClientExceptionMessages.MISC_INVALID_OBJECT_NAME(type_name)
    assert ex.error_code == "0412"
    assert ex.message == f"The object name '{type_name}' is invalid."


def test_misc_no_default_session():
    ex = SnowparkClientExceptionMessages.MISC_NO_DEFAULT_SESSION()
    assert ex.error_code == "0418"
    assert ex.message == "No default SnowflakeSession found"
