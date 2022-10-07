#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark._internal.analyzer import analyzer_utils
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    convert_value_to_sql_option,
)


def test_generate_scoped_temp_objects():
    temp_file_format_name = "SNOWPARK_TEMP_FILE_FORMAT_E0ZW8Z9WMY"
    assert (
        analyzer_utils.create_file_format_statement(
            temp_file_format_name,
            "csv",
            {},
            if_not_exist=True,
            temp=True,
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE SCOPED TEMPORARY FILE  FORMAT  If  NOT  EXISTS {temp_file_format_name} TYPE  = csv   "
    )

    assert (
        analyzer_utils.create_file_format_statement(
            temp_file_format_name,
            "csv",
            {},
            if_not_exist=True,
            temp=True,
            use_scoped_temp_objects=False,
            is_generated=True,
        )
        == f" CREATE TEMPORARY FILE  FORMAT  If  NOT  EXISTS {temp_file_format_name} TYPE  = csv   "
    )

    assert (
        analyzer_utils.create_file_format_statement(
            temp_file_format_name,
            "csv",
            {},
            if_not_exist=True,
            temp=True,
            use_scoped_temp_objects=True,
            is_generated=False,
        )
        == f" CREATE TEMPORARY FILE  FORMAT  If  NOT  EXISTS {temp_file_format_name} TYPE  = csv   "
    )

    assert (
        analyzer_utils.create_file_format_statement(
            temp_file_format_name,
            "csv",
            {},
            if_not_exist=True,
            temp=False,
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  FILE  FORMAT  If  NOT  EXISTS {temp_file_format_name} TYPE  = csv   "
    )

    temp_table_name = "SNOWPARK_TEMP_FILE_FORMAT_E0ZW8Z9WMY"
    temp_schema_name = "TEST_SCHEMA"
    assert (
        analyzer_utils.create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temp",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  SCOPED TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        analyzer_utils.create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  SCOPED TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        analyzer_utils.create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=False,
            is_generated=True,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        analyzer_utils.create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=True,
            is_generated=False,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        analyzer_utils.create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=False,
            is_generated=False,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        analyzer_utils.create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="transient",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  TRANSIENT  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        analyzer_utils.create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE    TABLE {temp_table_name}({temp_schema_name})"
    )


def test_convert_value_to_sql_option():
    assert convert_value_to_sql_option(True) == "True"
    assert convert_value_to_sql_option("hello world") == "'hello world'"
    assert convert_value_to_sql_option("'hello world'") == "'hello world'"
    assert convert_value_to_sql_option("hello'world") == "'hello''world'"
    assert convert_value_to_sql_option("''") == "''"
    assert convert_value_to_sql_option("") == "''"
    assert convert_value_to_sql_option(1) == "1"
    assert convert_value_to_sql_option(None) == "None"
