#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.session import Session
from tests.parameters import CONNECTION_PARAMETERS


@pytest.mark.parametrize("use_local_testing", [False, True])
@pytest.mark.parametrize("ast_enabled,n_batches", [(True, 1), (False, 0)])
def test_parameter(use_local_testing, ast_enabled, n_batches):
    set_ast_state(AstFlagSource.TEST, ast_enabled)
    # Check for each session whether AST collection works or not.
    # TODO: on public CI, need to replace schema.
    session = (
        Session.builder.configs(CONNECTION_PARAMETERS)
        .config("local_testing", use_local_testing)
        .create()
    )
    session.ast_enabled = ast_enabled

    with session.ast_listener() as al:
        session.create_dataframe([1, 2, 3, 4]).collect()

    assert (
        len(al.base64_batches) == n_batches
    ), f"With ast_enabled={ast_enabled} expected {n_batches} batches."
