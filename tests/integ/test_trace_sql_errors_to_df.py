#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
import snowflake.snowpark.context as context
import sys

from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    sum,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.window import Window
from tests.utils import Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    ),
    pytest.mark.skipif(
        sys.version_info < (3, 10),
        reason="Line numbers are flaky in Python 3.9",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    yield
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=False)
    set_ast_state(AstFlagSource.TEST, original)


def test_python_source_location_in_sql_error(session):
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    df1 = df.select(col("a").alias("x"), col("b").alias("y"))
    df2 = df1.select(col("x") + col("a"))
    with pytest.raises(SnowparkSQLException) as ex:
        df2.collect()
    assert "SQL compilation error corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 6}" in str(ex.value.debug_context)


def test_python_source_location_in_session_sql(session):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    df3 = session.sql(
        "SELECT a, b, c + '5' AS invalid_operation FROM (select 1 as a, 2 as b, array_construct(1, 2, 3) as c) WHERE a > 0"
    )
    with pytest.raises(SnowparkSQLException) as ex:
        df3.show()
    assert "SQL compilation error corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    if sys.version_info < (3, 11):
        assert f"line {line_number - 8}" in str(ex.value.debug_context)
    else:
        assert f"lines {line_number - 8}-{line_number - 6}" in str(
            ex.value.debug_context
        )


def test_join_ambiguous_column_error(session):
    df1 = session.create_dataframe([[1, "a"], [2, "b"]], schema=["id", "value"])
    df2 = session.create_dataframe([[1, "x"], [2, "y"]], schema=["id", "data"])
    joined = df1.join(df2, df1["id"] == df2["id"])
    df_error = joined.select(col("id"))
    with pytest.raises(SnowparkSQLException) as ex:
        df_error.collect()
    assert "SQL compilation error corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 6}" in str(ex.value.debug_context)


def test_window_function_error(session):
    df = session.create_dataframe(
        [[1, 10], [2, 20], [3, 30]], schema=["group_id", "value"]
    )
    window_spec = Window.partition_by("nonexistent_column").order_by("value")
    df_error = df.select(col("value"), sum(col("value")).over(window_spec))
    with pytest.raises(SnowparkSQLException) as ex:
        df_error.collect()
    assert "SQL compilation error corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 6}" in str(ex.value.debug_context)


def test_invalid_identifier_error_message(session):
    # we reuse tests from test_snowflake_plan_suite.py since AST is not enabled in that test suite
    df = session.create_dataframe([[1, 2, 3]], schema=['"abc"', '"abd"', '"def"'])
    with pytest.raises(SnowparkSQLException) as ex:
        df.select("abc").collect()
    assert ex.value.sql_error_code == 904
    assert "invalid identifier 'ABC'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abc\"'?" in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 9}" in str(ex.value)

    with pytest.raises(SnowparkSQLException) as ex:
        df.select("_ab").collect()
    assert "invalid identifier '_AB'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abd\"' or '\"abc\"'?" in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 8}" in str(ex.value)

    with pytest.raises(SnowparkSQLException) as ex:
        df.select('"abC"').collect()
    assert "invalid identifier '\"abC\"'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean" not in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 8}" in str(ex.value)

    df = session.create_dataframe([list(range(20))], schema=[str(i) for i in range(20)])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("20").collect()
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 2}" in str(ex.value)

    df = session.create_dataframe([1, 2, 3], schema=["A"])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("B").schema
    assert "There are existing quoted column identifiers: ['\"A\"']" in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 3}" in str(ex.value)


def test_missing_table_with_session_table(session):
    with pytest.raises(SnowparkSQLException) as ex:
        session.table("NON_EXISTENT_TABLE").collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 5}" in str(ex.value)


def test_missing_table_context_with_session_sql(session):
    with pytest.raises(SnowparkSQLException) as ex:
        session.sql("SELECT * FROM NON_EXISTENT_TABLE").collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    )
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 5}" in str(ex.value.debug_context)


@pytest.mark.parametrize(
    "operation_name,operation_func",
    [
        (
            "select",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .select(col("a")),
        ),
        (
            "filter",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .filter(col("a") > 0),
        ),
        (
            "sort",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .sort(col("a")),
        ),
        (
            "group_by",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .group_by(col("a")),
        ),
        (
            "join",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .join(
                session.create_dataframe([[1, 2]], schema=["x", "y"]),
                col("a") == col("x"),
            ),
        ),
        (
            "union",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .schema("a", "b")
            .union(session.table("ANOTHER_NON_EXISTENT_TABLE")),
        ),
    ],
)
def test_missing_table_with_dataframe_operations(
    session, operation_name, operation_func
):
    """Test that missing table errors are traced properly across various DataFrame operations."""

    with pytest.raises(SnowparkSQLException) as ex:
        df = operation_func(session)
        df.collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    ), f"Missing object trace not found for operation: {operation_name}"


def test_existing_table_with_save_as_table(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df = session.create_dataframe([{"a": 1, "b": 2}])
    df.write.save_as_table(table_name, mode="overwrite")
    with pytest.raises(SnowparkSQLException) as ex:
        df.write.save_as_table(table_name)

    assert f"Object '{table_name}' was first referenced" in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 5}" in str(ex.value.debug_context)

    Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "create_sql_template,conflicting_sql_template,object_type",
    [
        (
            "CREATE TABLE {name} (a INT, b INT)",
            "CREATE TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE OR REPLACE TABLE {name} (a INT, b INT)",
            "CREATE TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE TEMP TABLE {name} (a INT, b INT)",
            "CREATE TEMP TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE TEMPORARY TABLE {name} (a INT, b INT)",
            "CREATE TEMPORARY TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE TABLE IF NOT EXISTS {name} (a INT, b INT)",
            "CREATE TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
        (
            "CREATE VIEW {name} AS SELECT 1 AS a, 2 AS b",
            "CREATE VIEW {name} AS SELECT 3 AS a, 4 AS b",
            "VIEW",
        ),
        (
            "CREATE OR REPLACE VIEW {name} AS SELECT 1 AS a, 2 AS b",
            "CREATE VIEW {name} AS SELECT 3 AS a, 4 AS b",
            "VIEW",
        ),
        (
            "CREATE TEMP VIEW {name} AS SELECT 1 AS a, 2 AS b",
            "CREATE TEMP VIEW {name} AS SELECT 3 AS a, 4 AS b",
            "VIEW",
        ),
        (
            "CREATE VIEW IF NOT EXISTS {name} AS SELECT 1 AS a, 2 AS b",
            "CREATE VIEW {name} AS SELECT 3 AS a, 4 AS b",
            "VIEW",
        ),
        # Quoted table names
        (
            'CREATE TABLE "{name}" (a INT, b INT)',
            'CREATE TABLE "{name}" (a INT, b INT)',
            "TABLE",
        ),
        # Mixed quoted/unquoted (should still match)
        (
            'CREATE TABLE "{name}" (a INT, b INT)',
            "CREATE TABLE {name} (a INT, b INT)",
            "TABLE",
        ),
    ],
)
def test_existing_object_with_session_sql_create_statements(
    session, create_sql_template, conflicting_sql_template, object_type
):
    """Test that get_existing_object_context properly identifies objects created via session.sql() with various CREATE statement patterns."""
    if object_type == "TABLE":
        object_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    else:
        object_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)

    session.sql(create_sql_template.format(name=object_name)).collect()
    # Second CREATE statement should fail with "already exists" error
    with pytest.raises(SnowparkSQLException) as ex:
        session.sql(conflicting_sql_template.format(name=object_name)).collect()

    expected_message = f"Object '{object_name}' was first referenced"
    assert expected_message in str(
        ex.value.debug_context
    ), f"Expected message '{expected_message}' not found in debug context: {ex.value.debug_context}"
    if object_type == "TABLE":
        Utils.drop_table(session, object_name)
    else:
        Utils.drop_view(session, object_name)


def test_existing_object_with_schema_qualified_names(session):
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    db = session.get_current_database()
    sc = session.get_current_schema()
    df = session.create_dataframe([{"a": 1, "b": 2}])
    df.write.save_as_table([db, sc, temp_table_name], mode="overwrite")
    df2 = session.create_dataframe([{"a": 3, "b": 4}])
    with pytest.raises(SnowparkSQLException) as ex:
        df2.write.save_as_table([db, sc, temp_table_name], mode="errorifexists")

    db = db.strip('"')
    sc = sc.strip('"')
    expected_message = f"Object '{db}.{sc}.{temp_table_name}' was first referenced"
    assert expected_message in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 9}" in str(ex.value.debug_context)
    Utils.drop_table(session, temp_table_name)


def test_existing_object_with_schema_qualified_names_using_session_sql(session):
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    db = session.get_current_database()
    sc = session.get_current_schema()
    session.sql(f"CREATE TABLE {db}.{sc}.{temp_table_name} (a INT, b INT)").collect()

    with pytest.raises(SnowparkSQLException) as ex:
        session.sql(
            f"CREATE TABLE {db}.{sc}.{temp_table_name} (a INT, b INT)"
        ).collect()

    db = db.strip('"')
    sc = sc.strip('"')
    expected_message = f"Object '{db}.{sc}.{temp_table_name}' was first referenced"
    assert expected_message in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 11}" in str(ex.value.debug_context)
    Utils.drop_table(session, temp_table_name)


def test_existing_view_with_schema_qualified_names_using_session_sql(session):
    temp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    db = session.get_current_database()
    sc = session.get_current_schema()
    try:
        session.sql(
            f"CREATE VIEW {db}.{sc}.{temp_view_name} AS SELECT 1 AS a, 2 AS b"
        ).collect()

        with pytest.raises(SnowparkSQLException) as ex:
            session.sql(
                f"CREATE VIEW {db}.{sc}.{temp_view_name} AS SELECT 3 AS a, 4 AS b"
            ).collect()

        db = db.strip('"')
        sc = sc.strip('"')
        expected_message = f"Object '{db}.{sc}.{temp_view_name}' was first referenced"
        assert expected_message in str(ex.value.debug_context)
        line_number = Utils.get_current_line_number_sys()
        if sys.version_info < (3, 11):
            assert f"line {line_number - 13}" in str(ex.value.debug_context)
        else:
            assert f"lines {line_number - 13}-{line_number - 11}" in str(
                ex.value.debug_context
            )
    finally:
        Utils.drop_view(session, temp_view_name)


def test_existing_view_with_schema_qualified_names_using_dataframe_methods(session):
    temp_view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    db = session.get_current_database()
    sc = session.get_current_schema()
    df = session.create_dataframe([{"a": 1, "b": 2}])
    df.create_temp_view([db, sc, temp_view_name])
    df2 = session.create_dataframe([{"a": 3, "b": 4}])
    with pytest.raises(SnowparkSQLException) as ex:
        df2.create_temp_view([db, sc, temp_view_name])

    db = db.strip('"')
    sc = sc.strip('"')
    expected_message = f"Object '{db}.{sc}.{temp_view_name}' was first referenced"
    assert expected_message in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 9}" in str(ex.value.debug_context)
    Utils.drop_view(session, temp_view_name)


def test_existing_object_debug_context_when_ast_disabled(session):
    """Test that the debug context function gracefully handles the case when AST is disabled."""
    original_ast_state = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, False)
    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    session.sql(f"CREATE TABLE {temp_table_name} (a INT, b INT)").collect()
    with pytest.raises(SnowparkSQLException) as ex:
        session.sql(f"CREATE TABLE {temp_table_name} (a INT, b INT)").collect()

    assert f"Object '{temp_table_name}' already exists" in str(ex.value)

    Utils.drop_table(session, temp_table_name)
    set_ast_state(AstFlagSource.TEST, original_ast_state)


def test_existing_table_with_dataframe_write_operations(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    df1 = session.create_dataframe([{"a": 1, "b": 2}])
    df1.write.save_as_table(table_name, mode="overwrite")
    df2 = session.create_dataframe([{"a": 3, "b": 4}])
    with pytest.raises(SnowparkSQLException) as ex:
        df2.write.save_as_table(table_name, mode="errorifexists")

    assert f"Object '{table_name}' was first referenced" in str(ex.value.debug_context)
    line_number = Utils.get_current_line_number_sys()
    assert f"line {line_number - 6}" in str(ex.value.debug_context)
    Utils.drop_table(session, table_name)


@pytest.mark.parametrize(
    "view_method",
    [
        "create_temp_view",
        "create_or_replace_view",
        "create_or_replace_temp_view",
    ],
)
def test_existing_view_with_all_dataframe_methods(session, view_method):
    """Test that get_existing_object_context works with all DataFrame view creation methods."""
    view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
    df1 = session.create_dataframe([{"a": 1, "b": 2}])
    df2 = session.create_dataframe([{"a": 3, "b": 4}])
    getattr(df1, view_method)(view_name)

    if view_method == "create_or_replace_view":
        with pytest.raises(SnowparkSQLException) as ex:
            session.sql(f"CREATE VIEW {view_name} AS SELECT 5 AS a, 6 AS b").collect()
    elif view_method == "create_or_replace_temp_view":
        with pytest.raises(SnowparkSQLException) as ex:
            session.sql(
                f"CREATE TEMP VIEW {view_name} AS SELECT 5 AS a, 6 AS b"
            ).collect()
    else:
        with pytest.raises(SnowparkSQLException) as ex:
            getattr(df2, view_method)(view_name)

    assert f"Object '{view_name}' was first referenced" in str(
        ex.value.debug_context
    ), f"Expected object reference not found in debug context for {view_method}: {ex.value.debug_context}"

    Utils.drop_view(session, view_name)
