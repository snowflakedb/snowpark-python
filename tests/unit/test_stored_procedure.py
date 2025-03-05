#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
from unittest import mock

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark._internal.analyzer.analyzer import Analyzer
from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlanBuilder
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark._internal.telemetry import TelemetryClient
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    set_ast_state,
    AstFlagSource,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.stored_procedure import StoredProcedureRegistration
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.version import VERSION


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
    fake_session._join_alias_fix = False
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
        _emit_ast=False,
    )
    assert any(
        f"EXECUTE AS {execute_as.upper()}" in c.args[0]
        for c in fake_session._run_query.call_args_list
    )


def test_execute_as_negative():
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
            _emit_ast=False,
        )


@mock.patch("snowflake.snowpark.stored_procedure.cleanup_failed_permanent_registration")
def test_do_register_sp_negative(cleanup_registration_patch):
    AST_ENABLED = False
    set_ast_state(AstFlagSource.TEST, AST_ENABLED)
    fake_session = mock.create_autospec(Session)
    fake_session.ast_enabled = AST_ENABLED
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


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
@mock.patch(
    "snowflake.snowpark.session._is_execution_environment_sandboxed_for_client",
    return_value=True,
)
def test_do_register_sproc_sandbox(session_sandbox, cleanup_registration_patch):

    callback_side_effect_list = []

    def mock_callback(extension_function_properties):
        callback_side_effect_list.append(extension_function_properties)
        return False

    with mock.patch(
        "snowflake.snowpark.context._should_continue_registration",
        new=mock_callback,
    ):
        sproc(
            lambda: 1,
            return_type=IntegerType(),
            packages=[],
            native_app_params={
                "schema": "some_schema",
                "application_roles": ["app_viewer"],
            },
            _emit_ast=False,
        )
        cleanup_registration_patch.assert_not_called()

        assert len(callback_side_effect_list) == 1
        callableProperties = callback_side_effect_list[0]
        assert not callableProperties.replace
        assert callableProperties.object_type == TempObjectType.PROCEDURE
        assert not callableProperties.if_not_exists
        assert callableProperties.object_name != ""
        assert len(callableProperties.input_args) == 0
        assert len(callableProperties.input_sql_types) == 0
        assert callableProperties.return_sql == "RETURNS INT"
        assert (
            callableProperties.runtime_version
            == f"{sys.version_info[0]}.{sys.version_info[1]}"
        )
        assert callableProperties.all_imports == ""
        assert (
            callableProperties.all_packages
            == f"'snowflake-snowpark-python=={'.'.join(map(str, VERSION))}'"
        )
        assert callableProperties.external_access_integrations is None
        assert callableProperties.secrets is None
        assert callableProperties.handler is None
        assert callableProperties.execute_as == "owner"
        assert callableProperties.inline_python_code is None
        assert callableProperties.native_app_params == {
            "schema": "some_schema",
            "application_roles": ["app_viewer"],
        }
        assert callableProperties.raw_imports is None
