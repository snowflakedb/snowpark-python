#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.stored_procedure import StoredProcedureRegistration
from snowflake.snowpark.types import IntegerType


@pytest.mark.parametrize(
    "execute_as",
    [
        "owner",
        "caller",
    ],
)
def test_stored_procedure_execute_as(execute_as):
    """Make sure that EXECUTE AS option is rendered into SQL correctly."""
    fake_session = mock.create_autospec(Session)
    fake_session._conn = mock.create_autospec(ServerConnection)
    fake_session.sproc = StoredProcedureRegistration(fake_session)
    fake_session._plan_builder = SnowflakePlanBuilder(fake_session)
    fake_session._analyzer = Analyzer(fake_session)

    def return1(_):
        return 1

    sproc(
        return1,
        name="UNIT_TEST",
        packages=[],
        return_type=IntegerType(),
        session=fake_session,
        execute_as=execute_as,
    )
    assert any(
        f"EXECUTE AS {execute_as.upper()}" in c.args[0]
        for c in fake_session._run_query.call_args_list
    )
