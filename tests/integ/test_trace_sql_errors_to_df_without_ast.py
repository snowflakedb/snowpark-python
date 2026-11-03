#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Runs every test of test_trace_sql_errors_to_df.py with the AST collection disabled.

The tests and their assertions are the unchanged ones; only the autouse fixture is replaced so
the AST stays off. They pass only if the SQL error mapping does not depend on the AST.
"""

import pytest

from snowflake.snowpark import context
from snowflake.snowpark._internal.utils import (
    AstFlagSource,
    is_ast_enabled,
    set_ast_state,
)
from tests.integ.test_trace_sql_errors_to_df import *  # noqa: F401,F403


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    set_ast_state(AstFlagSource.TEST, False)
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=True)
    yield
    assert not is_ast_enabled(), "the debug features must not turn the AST on"
    context.configure_development_features(enable_trace_sql_errors_to_dataframe=False)
    set_ast_state(AstFlagSource.TEST, original)
