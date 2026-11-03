#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Runs every test of test_df_debug_trace.py with the AST collection disabled.

The tests and their assertions are the unchanged ones; only the autouse fixture is replaced so
the AST stays off. They pass only if the dataframe trace does not depend on the AST.
"""

import os

import pytest

from snowflake.snowpark import context
from snowflake.snowpark._internal.utils import (
    AstFlagSource,
    is_ast_enabled,
    set_ast_state,
)
from tests.integ.test_df_debug_trace import *  # noqa: F401,F403
from tests.integ.test_df_debug_trace import (
    SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH,
)


@pytest.fixture(autouse=True)
def setup(request, session):
    original = session.ast_enabled
    context.configure_development_features(enable_dataframe_trace_on_error=True)
    set_ast_state(AstFlagSource.TEST, False)
    os.environ.pop(SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH, None)
    yield
    assert not is_ast_enabled(), "the debug features must not turn the AST on"
    context.configure_development_features(enable_dataframe_trace_on_error=False)
    set_ast_state(AstFlagSource.TEST, original)
    os.environ.pop(SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH, None)
