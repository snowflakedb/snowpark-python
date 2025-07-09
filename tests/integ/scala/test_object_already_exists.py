#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.exceptions import SnowparkSQLException

pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="This is a SQL test suite",
        run=False,
    ),
]


@pytest.fixture(autouse=True)
def setup(request, session):
    """Setup fixture to enable AST for all tests in this module."""
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, True)
    yield
    set_ast_state(AstFlagSource.TEST, original)


def test_existing_table_with_save_as_table(session):
    table_name = "DUPLICATE_TABLE"
    df = session.create_dataframe([{"a": 1, "b": 2}])
    df.write.save_as_table(table_name, mode="overwrite")

    try:
        with pytest.raises(SnowparkSQLException) as ex:
            df.write.save_as_table(table_name)

        assert f"Table '{table_name}' was first referenced" in str(
            ex.value.debug_context
        )
    finally:
        session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()


def test_existing_table_with_session_sql_create_table(session):
    table_name = "DUPLICATE_TABLE"

    session.sql(f"CREATE TABLE {table_name} (a INT, b INT)").collect()

    try:
        with pytest.raises(SnowparkSQLException) as ex:
            session.sql(f"CREATE TABLE {table_name} (a INT, b INT)").collect()

        assert f"Table '{table_name}' was first referenced" in str(
            ex.value.debug_context
        )
    finally:
        session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()


def test_existing_table_with_dataframe_write_operations(session):
    table_name = "DUPLICATE_TABLE"

    df1 = session.create_dataframe([{"a": 1, "b": 2}])
    df1.write.save_as_table(table_name, mode="overwrite")

    try:
        df2 = session.create_dataframe([{"a": 3, "b": 4}])
        with pytest.raises(SnowparkSQLException) as ex:
            df2.write.save_as_table(table_name, mode="errorifexists")

        assert f"Table '{table_name}' was first referenced" in str(
            ex.value.debug_context
        )
    finally:
        session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
