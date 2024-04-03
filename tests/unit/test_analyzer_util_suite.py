#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    convert_value_to_sql_option,
    create_file_format_statement,
    create_or_replace_dynamic_table_statement,
    create_table_statement,
    file_operation_statement,
    join_statement,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    Inner,
    LeftAnti,
    UsingJoin,
)


def test_generate_scoped_temp_objects():
    temp_file_format_name = "SNOWPARK_TEMP_FILE_FORMAT_E0ZW8Z9WMY"
    assert (
        create_file_format_statement(
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
        create_file_format_statement(
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
        create_file_format_statement(
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
        create_file_format_statement(
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
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temp",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  SCOPED TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  SCOPED TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=False,
            is_generated=True,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=True,
            is_generated=False,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=False,
            is_generated=False,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="transient",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  TRANSIENT  TABLE {temp_table_name}({temp_schema_name})"
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE    TABLE {temp_table_name}({temp_schema_name})"
    )


def test_create_or_replace_dynamic_table_statement():
    dt_name = "my_dt"
    warehouse = "my_warehouse"
    print(
        create_or_replace_dynamic_table_statement(
            dt_name, warehouse, "1 minute", "select * from foo"
        )
    )
    assert (
        create_or_replace_dynamic_table_statement(
            dt_name, warehouse, "1 minute", "select * from foo"
        )
        == f" CREATE  OR  REPLACE  DYNAMIC  TABLE {dt_name} LAG  = '1 minute' WAREHOUSE  = {warehouse} AS  SELECT  *  FROM (select * from foo)"
    )


def test_convert_value_to_sql_option():
    assert convert_value_to_sql_option(True) == "True"
    assert convert_value_to_sql_option("hello world") == "'hello world'"
    assert convert_value_to_sql_option("'hello world'") == "'hello world'"
    assert convert_value_to_sql_option("hello'world") == "'hello''world'"
    assert convert_value_to_sql_option("''") == "''"
    assert convert_value_to_sql_option("'") == "''''"
    assert convert_value_to_sql_option("") == "''"
    assert convert_value_to_sql_option(1) == "1"
    assert convert_value_to_sql_option(None) == "None"


def test_file_operation_negative():
    with pytest.raises(ValueError, match="Unsupported file operation type"):
        file_operation_statement("xxx", "", "", {})


def test_join_statement_negative():
    join_type = UsingJoin(LeftAnti(), [])
    with pytest.raises(
        ValueError, match=f"Unexpected using clause in {join_type.tpe} join"
    ):
        join_statement("", "", join_type, "", "", False)

    join_type = UsingJoin(Inner(), ["cond1"])
    with pytest.raises(
        ValueError, match="A join should either have using clause or a join condition"
    ):
        join_statement("", "", join_type, "cond2", "", False)
