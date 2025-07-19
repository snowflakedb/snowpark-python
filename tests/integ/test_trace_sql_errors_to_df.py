#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import (
    col,
    sum,
)
from snowflake.snowpark.window import Window

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    yield
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

    with pytest.raises(SnowparkSQLException) as ex:
        df.select("_ab").collect()
    assert "invalid identifier '_AB'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean '\"abd\"' or '\"abc\"'?" in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)

    with pytest.raises(SnowparkSQLException) as ex:
        df.select('"abC"').collect()
    assert "invalid identifier '\"abC\"'" in str(ex.value)
    assert (
        "There are existing quoted column identifiers: ['\"abc\"', '\"abd\"', '\"def\"']"
        in str(ex.value)
    )
    assert "Do you mean" not in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)

    df = session.create_dataframe([list(range(20))], schema=[str(i) for i in range(20)])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("20").collect()
    assert "SQL compilation error corresponds to Python source" in str(ex.value)

    df = session.create_dataframe([1, 2, 3], schema=["A"])
    with pytest.raises(
        SnowparkSQLException, match="There are existing quoted column identifiers:*..."
    ) as ex:
        df.select("B").schema
    assert "There are existing quoted column identifiers: ['\"A\"']" in str(ex.value)
    assert "SQL compilation error corresponds to Python source" in str(ex.value)


def test_missing_table_with_session_table(session):
    with pytest.raises(SnowparkSQLException) as ex:
        session.table("NON_EXISTENT_TABLE").collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    )


def test_missing_table_context_with_session_sql(session):
    with pytest.raises(SnowparkSQLException) as ex:
        session.sql("SELECT * FROM NON_EXISTENT_TABLE").collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    )


@pytest.mark.parametrize(
    "operation_name,operation_func",
    [
        (
            "select",
            lambda session: session.table("NON_EXISTENT_TABLE").select(col("a")),
        ),
        (
            "filter",
            lambda session: session.table("NON_EXISTENT_TABLE").filter(col("a") > 0),
        ),
        ("sort", lambda session: session.table("NON_EXISTENT_TABLE").sort(col("a"))),
        (
            "group_by",
            lambda session: session.table("NON_EXISTENT_TABLE")
            .group_by(col("a"))
            .count(),
        ),
        (
            "join",
            lambda session: session.table("NON_EXISTENT_TABLE").join(
                session.create_dataframe([[1, 2]], schema=["x", "y"]),
                col("a") == col("x"),
            ),
        ),
        (
            "union",
            lambda session: session.table("NON_EXISTENT_TABLE").union(
                session.table("ANOTHER_NON_EXISTENT_TABLE")
            ),
        ),
        ("collect", lambda session: session.table("NON_EXISTENT_TABLE").collect()),
        ("show", lambda session: session.table("NON_EXISTENT_TABLE").show()),
        ("count", lambda session: session.table("NON_EXISTENT_TABLE").count()),
    ],
)
def test_missing_table_with_dataframe_operations(
    session, operation_name, operation_func
):
    """Test that missing table errors are traced properly across various DataFrame operations."""
    # I have no idea why, but this test only works when i configure context here even when
    # i configure it in conftest.py
    import snowflake.snowpark.context as context

    context.configure_development_features(
        enable_trace_sql_errors_to_dataframe=True,
    )

    with pytest.raises(SnowparkSQLException) as ex:
        operation_func(session)

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    ), f"Missing object trace not found for operation: {operation_name}"
