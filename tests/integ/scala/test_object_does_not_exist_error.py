#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col
import snowflake.snowpark.context as context

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
    context.configure_development_features(
        enable_trace_sql_errors_to_dataframe=True,
    )
    yield
    set_ast_state(AstFlagSource.TEST, original)


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


def test_missing_table_with_dataframe_operations(session):
    with pytest.raises(SnowparkSQLException) as ex:
        df = session.table("NON_EXISTENT_TABLE")
        df.select(col("a")).collect()

    assert "Missing object 'NON_EXISTENT_TABLE' corresponds to Python source" in str(
        ex.value.debug_context
    )
