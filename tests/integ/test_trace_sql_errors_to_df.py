#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
import sys

import pytest
import snowflake.snowpark.context as context
from snowflake.snowpark import Session

from snowflake.snowpark._internal.utils import (
    AstFlagSource,
    is_ast_enabled,
    set_ast_state,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    lit,
    sql_expr,
    sum,
    when,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan import PlanQueryType
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.window import Window
from tests.utils import IS_IN_STORED_PROC, Utils

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    ),
    pytest.mark.skipif(
        sys.version_info < (3, 11),
        reason="Line numbers are flaky before Python 3.11",
        run=False,
    ),
    pytest.mark.skipif(
        "FIPS_TEST" in os.environ,
        reason="SNOW-2204213: Reading source file location is not correct in FIPS mode",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    # The SQL error tracing does not use the AST collection; keep it off to make sure.
    set_ast_state(AstFlagSource.TEST, False)
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    yield
    assert not is_ast_enabled(), "the debug features must not turn the AST on"
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


def _mapped_lines(ex) -> list:
    """The lines of this file that the debug context maps the SQL error to."""
    return [
        int(line)
        for line in re.findall(
            re.escape(os.path.basename(__file__)) + r": line (\d+)",
            str(ex.value.debug_context),
        )
    ]


@pytest.fixture(params=[True, False], ids=["multiline", "singleline"])
def multiline(request, session):
    original = session._generate_multiline_queries
    if request.param:
        session._enable_multiline_queries()
    else:
        session._disable_multiline_queries()
    yield request.param
    if original:
        session._enable_multiline_queries()
    else:
        session._disable_multiline_queries()


def test_sql_error_maps_to_operation_of_its_subquery(session, multiline):
    """An operation that wraps the previous ones in a subquery is reported alone, not
    with the operations of the subquery (SNOW-4412764)."""
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    bad = sql_expr("TO_DATE(a, b, a, b)")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.with_column("c", col("a") + 1)
    df = df.with_column("d", bad)
    line = Utils.get_current_line_number_sys() - 1
    df = df.with_column("e", col("a") * 2)
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


def test_sql_error_in_case_expression(session, multiline):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    bad = sql_expr("TO_DATE(a, b, a, b)")
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.with_column("c", col("a") + 1)
    df = df.with_column("k", when(col("a") > 0, bad).otherwise(lit(None)))
    line = Utils.get_current_line_number_sys() - 1
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == [line]


def test_sql_error_in_flattened_operations(session, multiline):
    """Operations the SQL simplifier flattens into one SELECT share the SQL lines, so they
    are all reported; the offending one is among them."""
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    bad = sql_expr("TO_DATE(a, b, a, b)")
    first = Utils.get_current_line_number_sys() + 1
    df = session.create_dataframe([(1, "a")], schema=["a", "b"])
    df = df.select("a", "b")
    df = df.filter(col("a") > 0)
    df = df.filter(bad.is_not_null())
    df = df.sort(col("a"))
    with pytest.raises(SnowparkSQLException) as ex:
        df.collect()
    assert _mapped_lines(ex) == list(range(first, first + 5))


def test_sql_error_of_another_query_reports_all_operations(session, multiline):
    """The schema query run when the join is created has other lines than the sql of the
    plan, so all the operations of the matched plan are reported."""
    bad = sql_expr("TO_DATE(a, b, a, b)")
    first = Utils.get_current_line_number_sys() + 1
    left = session.create_dataframe([(1, "a")], schema=["a", "b"])
    left = left.with_column("x", bad)
    right = session.create_dataframe([(1, "z")], schema=["a", "y"])
    with pytest.raises(SnowparkSQLException) as ex:
        left.join(right, left["a"] == right["a"])
    if session.sql_simplifier_enabled:
        assert _mapped_lines(ex) == [first, first + 1, first + 4]
    else:
        # Without the simplifier the join resolves the schema of ``left`` with the sql
        # of its own plan, so the line maps precisely to the failing operation.
        assert _mapped_lines(ex) == [first + 1]


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
    assert f"lines {line_number - 8}-{line_number - 6}" in str(ex.value.debug_context)


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


@pytest.mark.parametrize(
    "operation_name,operation_func",
    [
        # from_ is a SelectableEntity (table)
        ("table_select", lambda t, session: t.select("A")),
        ("table_select_expr", lambda t, session: t.select((col("A") + 1).alias("B"))),
        ("table_select_chain", lambda t, session: t.select("A").select("A")),
        ("table_filter", lambda t, session: t.filter(col("A") > 0)),
        ("table_sort", lambda t, session: t.sort("A")),
        (
            "table_filter_then_select",
            lambda t, session: t.filter(col("A") > 0).select("A"),
        ),
        ("table_with_column", lambda t, session: t.with_column("B", col("A") + 1)),
        ("table_drop", lambda t, session: t.drop("A")),
        # from_ is a SetStatement
        ("union_then_select", lambda t, session: t.union(t).select("A")),
        # from_ is a SelectSnowflakePlan
        (
            "join_then_select",
            lambda t, session: t.join(session.table("NON_EXISTENT_TABLE"), "A").select(
                "A"
            ),
        ),
        ("limit_then_select", lambda t, session: t.limit(1).select("A")),
        # from_ is a SelectSQL
        (
            "sql_select",
            lambda t, session: session.sql("select * from NON_EXISTENT_TABLE").select(
                "A"
            ),
        ),
        (
            "sql_filter",
            lambda t, session: session.sql("select * from NON_EXISTENT_TABLE").filter(
                col("A") > 0
            ),
        ),
    ],
)
def test_missing_table_context_when_error_is_raised_during_construction(
    session, operation_name, operation_func
):
    """The describe that fails while building the dataframe must carry the same debug
    context as an error raised at collect(), whatever the kind of the inner `from_`."""
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    with pytest.raises(SnowparkSQLException) as ex:
        operation_func(session.table("NON_EXISTENT_TABLE"), session)

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    ), f"Missing object context not found for operation: {operation_name}"


def test_missing_table_context_points_to_table_line_when_error_is_raised_during_construction(
    session,
):
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    with pytest.raises(SnowparkSQLException) as ex:
        line_number = Utils.get_current_line_number_sys()
        session.table("NON_EXISTENT_TABLE").select("A")

    assert f"line {line_number + 1}" in str(ex.value.debug_context)


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
    assert f"lines {line_number - 13}-{line_number - 11}" in str(ex.value.debug_context)
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


def _create_temp_table_sql(column_lines):
    """A multi-line non-select statement, so session.sql() runs it as a pre-action."""
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    return f"create or replace temp table {table_name} as\nselect\n" + ",\n".join(
        column_lines
    )


# Fails at SQL line 10, after the last line of the main query that RESULT_SCANs it.
_LONG_BAD_SQL_COLUMNS = [f"{i} as c{i}" for i in range(7)] + ["no_such_col as bad"]


def _pre_action_beyond_main_query_lines(session):
    sql = _create_temp_table_sql(_LONG_BAD_SQL_COLUMNS)
    sql_line = Utils.get_current_line_number_sys() + 1
    df = session.sql(sql)
    return df.filter(col("c0") > 1), [sql_line], []


def _pre_action_within_main_query_lines(session):
    # Fails at SQL line 5, which also exists in the main query.
    sql = _create_temp_table_sql(["1 as a", "2 as b", "no_such_col as bad"])
    sql_line = Utils.get_current_line_number_sys() + 1
    df = session.sql(sql)
    return df.filter(col("a") > 1), [sql_line], []


def _second_of_two_pre_actions(session):
    sql_ok = _create_temp_table_sql(["1 as a"])
    sql_bad = _create_temp_table_sql(["1 as b", "no_such_col as bad"])
    ok_line = Utils.get_current_line_number_sys() + 1
    df_ok = session.sql(sql_ok)
    bad_line = Utils.get_current_line_number_sys() + 1
    df_bad = session.sql(sql_bad)
    df = df_ok.cross_join(df_bad)
    assert len(df._plan.queries) == 3
    return df, [bad_line], [ok_line]


def _pre_action_repeated_by_cte(session):
    sql = _create_temp_table_sql(_LONG_BAD_SQL_COLUMNS)
    sql_line = Utils.get_current_line_number_sys() + 1
    df = session.sql(sql).filter(col("c0") > 1)
    return df.union_all(df), [sql_line], []


def _main_query_after_session_sql_pre_action(session):
    df = session.sql(_create_temp_table_sql(["1 as a"]))
    filter_line = Utils.get_current_line_number_sys() + 1
    df = df.filter(col("no_such_col") > 1)
    return df, [filter_line], []


def _main_query_after_large_local_data_pre_actions(session):
    df = session.create_dataframe([[1, 2], [3, 4], [5, 6]], schema=["a", "b"])
    select_line = Utils.get_current_line_number_sys() + 1
    df = df.select(col("a"), col("no_such_col"))
    assert len(df._plan.queries) > 1
    return df, [select_line], []


def _large_query_breakdown_partition(session):
    # The invalid column is deep in the plan, so it fails in a partition query that
    # runs before the last query. The partition is not owned by a node of the original
    # plan, so no source location is reported rather than a wrong one.
    if not session.sql_simplifier_enabled:
        # without the simplifier, with_column resolves the schema eagerly and the
        # error is raised while the DataFrame is built, before any partition runs
        pytest.skip("SQL simplifier must be enabled for this test")
    df = session.create_dataframe([[1, 2]], schema=["a", "b"])
    df = df.filter(col("no_such_col") > 0)
    for i in range(12):
        df = df.with_column("a", col("a") + lit(i)).filter(col("a") > i)
        df = df.union_all(df.select("a", "b"))
    assert len(df._plan.execution_queries[PlanQueryType.QUERIES]) > 1
    return df, None, []


@pytest.mark.parametrize(
    "build_df, session_settings",
    [
        (_pre_action_beyond_main_query_lines, {}),
        (_pre_action_beyond_main_query_lines, {"sql_simplifier_enabled": False}),
        (_pre_action_within_main_query_lines, {}),
        (_second_of_two_pre_actions, {}),
        (_pre_action_repeated_by_cte, {"cte_optimization_enabled": True}),
        (_main_query_after_session_sql_pre_action, {}),
        (_main_query_after_large_local_data_pre_actions, {}),
        (
            _large_query_breakdown_partition,
            {
                # with CTE optimization the plan is compiled into one query instead
                "cte_optimization_enabled": False,
                "large_query_breakdown_enabled": True,
                "_large_query_breakdown_complexity_bounds": (300, 600),
            },
        ),
    ],
)
def test_sql_error_in_query_before_or_after_pre_actions(
    session, build_df, session_settings
):
    # SNOW-4412768: an error raised by a query that runs before the last query of the
    # plan maps to the DataFrame operation that produced it.
    from snowflake.snowpark._internal.analyzer import analyzer

    original_settings = {name: getattr(session, name) for name in session_settings}
    original_threshold = analyzer.ARRAY_BIND_THRESHOLD
    try:
        for name, value in session_settings.items():
            setattr(session, name, value)
        analyzer.ARRAY_BIND_THRESHOLD = 2
        df, expected_lines, unexpected_lines = build_df(session)
        with pytest.raises(SnowparkSQLException) as ex:
            df.collect()
    finally:
        analyzer.ARRAY_BIND_THRESHOLD = original_threshold
        for name, value in original_settings.items():
            setattr(session, name, value)
    debug_context = str(ex.value.debug_context)
    if expected_lines is None:
        assert "SQL compilation error corresponds to Python source" not in debug_context
        return
    assert "SQL compilation error corresponds to Python source" in debug_context
    for line in expected_lines:
        assert f"line {line}" in debug_context
    for line in unexpected_lines:
        assert f"line {line}" not in debug_context


def test_pre_action_owner_carries_session_sql_trace_node(session):
    # The source location of a pre-action error comes from the trace nodes of the plan
    # that owns the pre-action, which must include the session.sql() call.
    if not session.sql_simplifier_enabled:
        pytest.skip("SQL simplifier must be enabled for this test")
    from snowflake.snowpark._internal.utils import get_plan_from_pre_query_line_number

    df = session.sql(_create_temp_table_sql(["1 as a"]))
    filtered = df.filter(col("a") > 1)
    owner = get_plan_from_pre_query_line_number(
        filtered._plan, filtered._plan.queries[0], 1
    )
    assert any(node is df._trace_node for node in owner.df_trace_nodes)


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_sql_error_tracing_is_per_session(session, db_parameters):
    def build(s):
        df = s.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
        return df.select(col("a").alias("x")).select(col("x") + col("b"))

    with Session.builder.configs(db_parameters).create() as other:
        other.sql_simplifier_enabled = session.sql_simplifier_enabled
        other.debug.disable_trace_sql_errors_to_dataframe()
        assert session.debug.trace_sql_errors_to_dataframe is True
        on, off = build(session), build(other)
        # the session with the feature off sends the same SQL and records no line intervals
        assert off.queries == on.queries
        assert not any(q.query_line_intervals for q in off._plan.queries)
        with pytest.raises(SnowparkSQLException) as ex_on:
            on.collect()
        with pytest.raises(SnowparkSQLException) as ex_off:
            off.collect()
    assert "SQL compilation error corresponds to Python source" in str(
        ex_on.value.debug_context
    )
    assert "SQL compilation error corresponds to Python source" not in str(
        ex_off.value.debug_context or ""
    )
