#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark._internal.telemetry import TelemetryClient
from snowflake.snowpark.exceptions import SnowparkSQLException
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
    fake_session._conn._telemetry_client = mock.create_autospec(TelemetryClient)
    fake_session.sproc = StoredProcedureRegistration(fake_session)
    fake_session._plan_builder = SnowflakePlanBuilder(fake_session)
    fake_session._analyzer = Analyzer(fake_session)
    fake_session._runtime_version_from_requirement = None
    fake_session._packages = {}

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


def test_negative_execute_as():
    fake_session = mock.create_autospec(Session)
    fake_session.sproc = StoredProcedureRegistration(fake_session)
    fake_session._runtime_version_from_requirement = None
    with pytest.raises(
        TypeError,
        match="'execute_as' value 'invalid EXECUTE AS' " "is invalid, choose from",
    ):
        sproc(
            lambda: 1,
            session=fake_session,
            execute_as="invalid EXECUTE AS",
        )


@mock.patch("snowflake.snowpark.stored_procedure.cleanup_failed_permanent_registration")
def test_do_register_sp_negative(cleanup_registration_patch):
    fake_session = mock.create_autospec(Session)
    fake_session._runtime_version_from_requirement = None
    fake_session.get_fully_qualified_name_if_possible = mock.Mock(
        return_value="database.schema"
    )
    fake_session._run_query = mock.Mock(side_effect=ProgrammingError())
    fake_session.sproc = StoredProcedureRegistration(fake_session)
    fake_session._packages = {}
    with pytest.raises(SnowparkSQLException) as ex_info:
        sproc(lambda: 1, session=fake_session, return_type=IntegerType(), packages=[])
    assert ex_info.value.error_code == "1304"
    cleanup_registration_patch.assert_called()

    fake_session._run_query = mock.Mock(side_effect=BaseException())
    fake_session.sproc = StoredProcedureRegistration(fake_session)
    with pytest.raises(BaseException):
        sproc(lambda: 1, session=fake_session, return_type=IntegerType(), packages=[])
    cleanup_registration_patch.assert_called()
