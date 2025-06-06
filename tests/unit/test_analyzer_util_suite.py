#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import pytest
from unittest import mock

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    CHANGE_TRACKING,
    COPY_GRANTS,
    CREATE,
    DATA_RETENTION_TIME_IN_DAYS,
    ENABLE_SCHEMA_EVOLUTION,
    EQUALS,
    EXISTS,
    IF,
    MAX_DATA_EXTENSION_TIME_IN_DAYS,
    NOT,
    OR,
    REPLACE,
    convert_value_to_sql_option,
    create_file_format_statement,
    create_or_replace_dynamic_table_statement,
    create_table_as_select_statement,
    create_table_statement,
    file_operation_statement,
    join_statement,
    project_statement,
    table_function_statement,
    filter_statement,
    aggregate_statement,
    sort_statement,
    join_table_function_statement,
    lateral_statement,
    pivot_statement,
    unpivot_statement,
    sample_by_statement,
)
from snowflake.snowpark._internal.analyzer.binary_plan_node import (
    Inner,
    LeftAnti,
    UsingJoin,
)
from snowflake.snowpark._internal.utils import EMPTY_STRING


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
        == f" CREATE  SCOPED TEMPORARY  TABLE {temp_table_name}({temp_schema_name})  "
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  SCOPED TEMPORARY  TABLE {temp_table_name}({temp_schema_name})  "
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=False,
            is_generated=True,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})  "
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=True,
            is_generated=False,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})  "
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="temporary",
            use_scoped_temp_objects=False,
            is_generated=False,
        )
        == f" CREATE  TEMPORARY  TABLE {temp_table_name}({temp_schema_name})  "
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="transient",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE  TRANSIENT  TABLE {temp_table_name}({temp_schema_name})  "
    )

    assert (
        create_table_statement(
            temp_table_name,
            temp_schema_name,
            table_type="",
            use_scoped_temp_objects=True,
            is_generated=True,
        )
        == f" CREATE    TABLE {temp_table_name}({temp_schema_name})  "
    )


@pytest.mark.parametrize(
    "create_table_stmt_function",
    [
        lambda kwargs: create_table_statement("table", "schema", **kwargs),
        lambda kwargs: create_table_as_select_statement(
            "table", "select * from foo", None, **kwargs
        ),
    ],
)
@pytest.mark.parametrize(
    "replace,error", [(True, False), (False, True), (False, False)]
)
@pytest.mark.parametrize("enable_schema_evolution", [True, False, None])
@pytest.mark.parametrize("data_retention_time", [None, 1])
@pytest.mark.parametrize("max_data_extension_time", [None, 3])
@pytest.mark.parametrize("change_tracking", [None, True, False])
@pytest.mark.parametrize("copy_grants", [True, False])
def test_create_table_statement(
    create_table_stmt_function,
    replace,
    error,
    enable_schema_evolution,
    data_retention_time,
    max_data_extension_time,
    change_tracking,
    copy_grants,
):
    replace_sql = (CREATE + OR + REPLACE) if replace else EMPTY_STRING
    if_not_exists_sql = (
        (IF + NOT + EXISTS) if not replace and not error else EMPTY_STRING
    )
    enable_schema_evolution_sql = EMPTY_STRING
    data_retention_time_sql = EMPTY_STRING
    max_data_extension_time_sql = EMPTY_STRING
    change_tracking_sql = EMPTY_STRING
    copy_grants_sql = COPY_GRANTS if copy_grants else EMPTY_STRING

    if enable_schema_evolution is not None:
        enable_schema_evolution_sql = (
            f"{ENABLE_SCHEMA_EVOLUTION}{EQUALS}{enable_schema_evolution}"
        )
    if data_retention_time is not None:
        data_retention_time_sql = (
            f"{DATA_RETENTION_TIME_IN_DAYS}{EQUALS}{data_retention_time}"
        )
    if max_data_extension_time is not None:
        max_data_extension_time_sql = (
            f"{MAX_DATA_EXTENSION_TIME_IN_DAYS}{EQUALS}{max_data_extension_time}"
        )
    if change_tracking is not None:
        change_tracking_sql = f"{CHANGE_TRACKING}{EQUALS}{change_tracking}"

    kwargs = {
        "replace": replace,
        "error": error,
        "copy_grants": copy_grants,
        "enable_schema_evolution": enable_schema_evolution,
        "data_retention_time": data_retention_time,
        "max_data_extension_time": max_data_extension_time,
        "change_tracking": change_tracking,
    }
    create_table_stmt = create_table_stmt_function(kwargs)
    assert enable_schema_evolution_sql in create_table_stmt
    assert data_retention_time_sql in create_table_stmt
    assert max_data_extension_time_sql in create_table_stmt
    assert change_tracking_sql in create_table_stmt
    assert copy_grants_sql in create_table_stmt
    assert replace_sql in create_table_stmt
    assert if_not_exists_sql in create_table_stmt


def test_create_or_replace_dynamic_table_statement():
    dt_name = "my_dt"
    warehouse = "my_warehouse"
    comment = "my_comment"
    refresh_mode = "INCREMENTAL"
    initialize = "ON_SCHEDULE"
    cluster_by = ["col1"]
    data_retention_time = "2"
    max_data_extension_time = "4"
    assert create_or_replace_dynamic_table_statement(
        name=dt_name,
        warehouse=warehouse,
        lag="1 minute",
        comment=None,
        replace=True,
        if_not_exists=False,
        refresh_mode=None,
        initialize=None,
        clustering_keys=None,
        is_transient=False,
        data_retention_time=None,
        max_data_extension_time=None,
        child="select * from foo",
    ) == (
        f" CREATE  OR  REPLACE  DYNAMIC  TABLE {dt_name} LAG  = '1 minute' WAREHOUSE  = {warehouse}     "
        "AS  SELECT  * \n FROM (\nselect * from foo\n)"
    )

    assert create_or_replace_dynamic_table_statement(
        name=dt_name,
        warehouse=warehouse,
        lag="1 minute",
        comment=None,
        replace=False,
        if_not_exists=False,
        refresh_mode=None,
        initialize=None,
        clustering_keys=None,
        is_transient=False,
        data_retention_time=None,
        max_data_extension_time=None,
        child="select * from foo",
    ) == (
        f" CREATE  DYNAMIC  TABLE {dt_name} LAG  = '1 minute' WAREHOUSE  = {warehouse}     "
        "AS  SELECT  * \n FROM (\nselect * from foo\n)"
    )
    assert create_or_replace_dynamic_table_statement(
        name=dt_name,
        warehouse=warehouse,
        lag="1 minute",
        comment=None,
        replace=False,
        if_not_exists=True,
        refresh_mode=None,
        initialize=None,
        clustering_keys=None,
        is_transient=False,
        data_retention_time=None,
        max_data_extension_time=None,
        child="select * from foo",
    ) == (
        f" CREATE  DYNAMIC  TABLE  If  NOT  EXISTS {dt_name} LAG  = '1 minute' WAREHOUSE  = {warehouse}     "
        "AS  SELECT  * \n FROM (\nselect * from foo\n)"
    )
    assert create_or_replace_dynamic_table_statement(
        name=dt_name,
        warehouse=warehouse,
        lag="1 minute",
        comment=comment,
        replace=True,
        if_not_exists=False,
        refresh_mode=refresh_mode,
        initialize=initialize,
        clustering_keys=cluster_by,
        is_transient=True,
        data_retention_time=data_retention_time,
        max_data_extension_time=max_data_extension_time,
        child="select * from foo",
    ) == (
        f" CREATE  OR  REPLACE  TRANSIENT  DYNAMIC  TABLE {dt_name} LAG  = '1 minute' WAREHOUSE  = {warehouse}  "
        f"REFRESH_MODE  = '{refresh_mode}'  INITIALIZE  = '{initialize}'  CLUSTER BY ({cluster_by[0]})  "
        f"DATA_RETENTION_TIME_IN_DAYS  = '{data_retention_time}'  MAX_DATA_EXTENSION_TIME_IN_DAYS  = "
        f"'{max_data_extension_time}'  COMMENT  = '{comment}' AS  SELECT  * \n"
        " FROM (\nselect * from foo\n)"
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
    assert convert_value_to_sql_option((1,)) == "(1)"
    assert convert_value_to_sql_option((1, 2)) == "(1, 2)"


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


def test_create_iceberg_table_statement():
    with pytest.raises(
        ValueError, match="Iceberg table configuration requires base_location be set."
    ):
        create_table_statement(
            table_name="test_table",
            schema="test_col varchar",
            iceberg_config={},
        )
    assert create_table_statement(
        table_name="test_table",
        schema="test_col varchar",
        iceberg_config={
            "external_volume": "example_volume",
            "catalog": "example_catalog",
            "base_location": "/root",
            "catalog_sync": "integration_name",
            "storage_serialization_policy": "OPTIMIZED",
        },
    ) == (
        " CREATE    ICEBERG  TABLE test_table(test_col varchar)  EXTERNAL_VOLUME  = 'example_volume' "
        " CATALOG  = 'example_catalog'  BASE_LOCATION  = '/root'  CATALOG_SYNC  = 'integration_name'"
        "  STORAGE_SERIALIZATION_POLICY  = 'OPTIMIZED' "
    )


def test_create_iceberg_table_as_select_statement():
    assert create_table_as_select_statement(
        table_name="test_table",
        child="select * from foo",
        column_definition=None,
        iceberg_config={
            "external_volume": "example_volume",
            "catalog": "example_catalog",
            "base_location": "/root",
            "catalog_sync": "integration_name",
            "storage_serialization_policy": "OPTIMIZED",
        },
    ) == (
        " CREATE    ICEBERG  TABLE  test_table  EXTERNAL_VOLUME  = 'example_volume'  CATALOG  = "
        "'example_catalog'  BASE_LOCATION  = '/root'  CATALOG_SYNC  = 'integration_name'  "
        "STORAGE_SERIALIZATION_POLICY  = 'OPTIMIZED'   AS  SELECT  * \n"
        " FROM (\nselect * from foo\n)"
    )


def test_create_dynamic_iceberg_table():
    dt_name = "my_dt"
    warehouse = "my_warehouse"

    assert create_or_replace_dynamic_table_statement(
        name=dt_name,
        warehouse=warehouse,
        lag="1 minute",
        comment=None,
        replace=True,
        if_not_exists=False,
        refresh_mode=None,
        initialize=None,
        clustering_keys=None,
        is_transient=False,
        data_retention_time=None,
        max_data_extension_time=None,
        child="select * from foo",
        iceberg_config={
            "external_volume": "example_volume",
            "catalog": "example_catalog",
            "base_location": "/root",
            "catalog_sync": "integration_name",
            "storage_serialization_policy": "OPTIMIZED",
        },
    ) == (
        " CREATE  OR  REPLACE  DYNAMIC  ICEBERG  TABLE my_dt LAG  = '1 minute' WAREHOUSE  = "
        "my_warehouse    EXTERNAL_VOLUME  = 'example_volume'  CATALOG  = 'example_catalog'  "
        "BASE_LOCATION  = '/root'  CATALOG_SYNC  = 'integration_name'  STORAGE_SERIALIZATION_POLICY "
        " = 'OPTIMIZED' AS  SELECT  * \n"
        " FROM (\nselect * from foo\n)"
    )


def test_project_statement_formatting():
    assert project_statement(["col1", "col2"], "table1") == (
        " SELECT \n" "    col1, \n" "    col2\n" " FROM (\n" "table1\n" ")"
    )

    assert project_statement(["col1 as a", "col2 as b"], "table1") == (
        " SELECT \n" "    col1 as a, \n" "    col2 as b\n" " FROM (\n" "table1\n" ")"
    )

    assert project_statement(
        ["CASE WHEN col1 > 0 THEN 1 ELSE 0 END as flag", "COUNT(*) as cnt"], "table1"
    ) == (
        " SELECT \n"
        "    CASE WHEN col1 > 0 THEN 1 ELSE 0 END as flag, \n"
        "    COUNT(*) as cnt\n"
        " FROM (\n"
        "table1\n"
        ")"
    )

    child_query = "SELECT a, b\nFROM table1\nWHERE x > 0"
    assert project_statement(["col1", "col2"], child_query) == (
        " SELECT \n"
        "    col1, \n"
        "    col2\n"
        " FROM (\n"
        "SELECT a, b\n"
        "FROM table1\n"
        "WHERE x > 0\n"
        ")"
    )

    assert project_statement([], "table1") == (
        " SELECT  * \n" " FROM (\n" "table1\n" ")"
    )

    assert project_statement(["col1", "col2"], "table1", is_distinct=True) == (
        " SELECT  DISTINCT \n" "    col1, \n" "    col2\n" " FROM (\n" "table1\n" ")"
    )


def test_nested_query_formatting():
    nested_query = project_statement(
        ["t.col1", "t.col2"],
        project_statement(["inner.a as col1", "inner.b as col2"], "base_table inner"),
    )
    assert nested_query == (
        " SELECT \n"
        "    t.col1, \n"
        "    t.col2\n"
        " FROM (\n"
        " SELECT \n"
        "    inner.a as col1, \n"
        "    inner.b as col2\n"
        " FROM (\n"
        "base_table inner\n"
        ")\n"
        ")"
    )


def test_table_function_statement_formatting():
    assert table_function_statement("my_table_func()") == (
        " SELECT  * \n" " FROM (\n" " TABLE (my_table_func())\n" ")"
    )

    assert table_function_statement("my_table_func()", ["col1", "col2"]) == (
        " SELECT \n"
        "    col1, \n"
        "    col2\n"
        " FROM (\n"
        " TABLE (my_table_func())\n"
        ")"
    )


def test_filter_statement_formatting():
    assert filter_statement("x > 0 AND y < 10", "my_table") == (
        " SELECT  * \n" " FROM (\n" "my_table\n" ")\n" " WHERE x > 0 AND y < 10"
    )


@mock.patch(
    "snowflake.snowpark._internal.analyzer.analyzer_utils.random_name_for_temp_object",
    return_value="SNOWPARK_TEMP_COLUMN_T9IE0TMCWC",
)
def test_sample_by_statement_formatting(mock_random_name):
    sample = sample_by_statement(
        child="my_table", col="category", fractions={"A": 0.1, "B": 0.5, "C": 1.0}
    )
    expected = (
        " SELECT SNOWPARK_LEFT.* EXCLUDE SNOWPARK_TEMP_COLUMN_T9IE0TMCWC FROM (\n"
        " SELECT  * , PERCENT_RANK() OVER (PARTITION BY category ORDER BY RANDOM()) AS SNOWPARK_TEMP_COLUMN_T9IE0TMCWC FROM (\n"
        "my_table\n"
        ")\n"
        ") AS SNOWPARK_LEFT JOIN (\n"
        'SELECT KEY, VALUE FROM TABLE(FLATTEN(input => parse_json(\'{"A": 0.1, "B": 0.5, "C": 1.0}\')))\n'
        ") AS SNOWPARK_RIGHT ON SNOWPARK_LEFT.category = SNOWPARK_RIGHT.KEY WHERE SNOWPARK_LEFT.SNOWPARK_TEMP_COLUMN_T9IE0TMCWC <= SNOWPARK_RIGHT.VALUE"
    )
    assert sample == expected

    sample = sample_by_statement(child="my_table", col="category", fractions={})
    expected = (
        " SELECT SNOWPARK_LEFT.* EXCLUDE SNOWPARK_TEMP_COLUMN_T9IE0TMCWC FROM (\n"
        " SELECT  * , PERCENT_RANK() OVER (PARTITION BY category ORDER BY RANDOM()) AS SNOWPARK_TEMP_COLUMN_T9IE0TMCWC FROM (\n"
        "my_table\n"
        ")\n"
        ") AS SNOWPARK_LEFT JOIN (\n"
        "SELECT KEY, VALUE FROM TABLE(FLATTEN(input => parse_json('{}')))\n"
        ") AS SNOWPARK_RIGHT ON SNOWPARK_LEFT.category = SNOWPARK_RIGHT.KEY WHERE SNOWPARK_LEFT.SNOWPARK_TEMP_COLUMN_T9IE0TMCWC <= SNOWPARK_RIGHT.VALUE"
    )
    assert sample == expected


def test_aggregate_statement_formatting():
    assert aggregate_statement([], ["COUNT(*) as cnt"], "my_table") == (
        " SELECT \n" "    COUNT(*) as cnt\n" " FROM (\n" "my_table\n" ") LIMIT 1"
    )

    assert aggregate_statement(["dept", "title"], ["COUNT(*) as cnt"], "my_table") == (
        " SELECT \n"
        "    COUNT(*) as cnt\n"
        " FROM (\n"
        "my_table\n"
        ")\n"
        " GROUP BY \n"
        "    dept, \n"
        "    title"
    )


def test_sort_statement_formatting():
    assert sort_statement(["col1 ASC"], "my_table") == (
        " SELECT  * \n" " FROM (\n" "my_table\n" ")\n" " ORDER BY \n" "    col1 ASC"
    )

    assert sort_statement(["col1 ASC", "col2 DESC"], "my_table") == (
        " SELECT  * \n"
        " FROM (\n"
        "my_table\n"
        ")\n"
        " ORDER BY \n"
        "    col1 ASC, \n"
        "    col2 DESC"
    )


def test_join_table_function_statement_formatting():
    assert join_table_function_statement(
        "split_to_table(col1, ' ')",
        "my_table",
        ["id", "name"],
        ["seq", "index", "value"],
        True,
    ) == (
        " SELECT \n"
        "    T_LEFT.id, \n"
        "    T_LEFT.name, \n"
        "    T_RIGHT.seq, \n"
        "    T_RIGHT.index, \n"
        "    T_RIGHT.value\n"
        " FROM (\n"
        "my_table\n"
        ") AS T_LEFT\n"
        " JOIN \n"
        " TABLE (split_to_table(col1, ' ')) AS T_RIGHT"
    )


def test_lateral_statement_formatting():
    assert lateral_statement("TABLE(split_to_table(col1, ' '))", "my_table") == (
        " SELECT  * \n"
        " FROM (\n"
        "my_table\n"
        "), \n"
        " LATERAL TABLE(split_to_table(col1, ' '))"
    )


def test_pivot_statement_formatting():
    assert pivot_statement(
        "month", ["JAN", "FEB", "MAR"], "sum(amount)", None, "sales_data", True
    ) == (
        ' SELECT  *  EXCLUDE ("JAN", "FEB", "MAR"), "JAN" AS "JAN_sum(amount)", "FEB" AS "FEB_sum(amount)", "MAR" AS "MAR_sum(amount)" FROM (\n'
        "sales_data\n"
        ")\n"
        " PIVOT (\n"
        "    sum(amount) FOR month IN (JAN, FEB, MAR)\n"
        ")"
    )

    assert pivot_statement("month", None, "sum(amount)", "0", "sales_data", False) == (
        " SELECT  *  FROM (\n"
        "sales_data\n"
        ")\n"
        " PIVOT (\n"
        "    sum(amount) FOR month IN ( ANY ) DEFAULT ON NULL (0)\n"
        ")"
    )


def test_unpivot_statement_formatting():
    assert unpivot_statement(
        "sales_amount", "month", ["JAN", "FEB", "MAR"], False, "sales_data"
    ) == (
        " SELECT  *  FROM (\n"
        "sales_data\n"
        ")\n"
        " UNPIVOT (\n"
        "    sales_amount FOR month IN (JAN, FEB, MAR)\n)"
    )

    assert unpivot_statement(
        "sales_amount", "month", ["JAN", "FEB", "MAR"], True, "sales_data"
    ) == (
        " SELECT  *  FROM (\n"
        "sales_data\n"
        ")\n"
        " UNPIVOT  INCLUDE NULLS (\n"
        "    sales_amount FOR month IN (JAN, FEB, MAR)\n)"
    )
