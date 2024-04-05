#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udf
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.udf import UDFRegistration


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
def test_do_register_udf_negative(cleanup_registration_patch):
    fake_session = mock.create_autospec(Session)
    fake_session._runtime_version_from_requirement = None
    fake_session.get_fully_qualified_name_if_possible = mock.Mock(
        return_value="database.schema"
    )
    fake_session._run_query = mock.Mock(side_effect=ProgrammingError())
    fake_session._import_paths = {}
    fake_session.udf = UDFRegistration(fake_session)
    with pytest.raises(SnowparkSQLException) as ex_info:
        udf(lambda: 1, session=fake_session, return_type=IntegerType(), packages=[])
    assert ex_info.value.error_code == "1304"
    cleanup_registration_patch.assert_called()

    fake_session._run_query = mock.Mock(
        side_effect=BaseException("Test BaseException code path")
    )
    fake_session.udf = UDFRegistration(fake_session)
    with pytest.raises(BaseException, match="Test BaseException code path"):
        udf(lambda: 1, session=fake_session, return_type=IntegerType(), packages=[])
    cleanup_registration_patch.assert_called()


# Sandbox is [True, False] with no explicit Connection object.
@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
@mock.patch("snowflake.snowpark.session._is_execution_environment_sandboxed")
@mock.patch(
    "snowflake.snowpark._internal.udf_utils._is_execution_environment_sandboxed"
)
@pytest.mark.parametrize("is_sandbox", [True, False])
def test_do_register_udf_sandbox(
    utils_sandbox, session_sandbox, cleanup_registration_patch, is_sandbox
):

    callback_side_effect_list = []

    def mock_callback(callableProperties):
        callback_side_effect_list.append(callableProperties)
        return False  # i.e. don't register with Snowflake.

    session_sandbox.return_value = is_sandbox
    utils_sandbox.return_value = is_sandbox

    fake_session = mock.create_autospec(Session)
    fake_session._runtime_version_from_requirement = None
    fake_session.get_fully_qualified_name_if_possible = mock.Mock(
        return_value="database.schema"
    )
    fake_session._run_query = mock.Mock(side_effect=ProgrammingError())
    fake_session._import_paths = {}
    fake_session.udf = UDFRegistration(fake_session)

    with mock.patch(
        "snowflake.snowpark._internal.udf_utils._should_continue_registration",
        new=mock_callback,
    ):
        udf(lambda: 1, session=fake_session, return_type=IntegerType(), packages=[])

    cleanup_registration_patch.assert_not_called()

    assert len(callback_side_effect_list) == 1
    callableProperties = callback_side_effect_list[0]
    assert not callableProperties.replace
    assert not callableProperties.is_permanent
    assert not callableProperties.secure
    assert callableProperties.object_type == TempObjectType.FUNCTION
    assert not callableProperties.if_not_exists
    assert callableProperties.object_name == "database.schema"
    assert len(callableProperties.input_args) == 0
    assert len(callableProperties.input_sql_types) == 0
    assert callableProperties.return_sql == "RETURNS INT"
    assert not callableProperties.strict
    assert not callableProperties.immutable
    assert callableProperties.runtime_version == "3.8"
    assert callableProperties.all_imports == ""
    assert callableProperties.all_packages == ""
    assert callableProperties.external_access_integrations is None
    assert callableProperties.secrets is None
    assert callableProperties.handler is None
    assert callableProperties.execute_as is None
    assert callableProperties.inline_python_code is None
    assert callableProperties.native_app_params is None
    assert callableProperties.import_paths == {}
    assert (
        callableProperties.is_execution_environment_sandboxed.return_value == is_sandbox
    )


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
@mock.patch(
    "snowflake.snowpark.session._is_execution_environment_sandboxed", return_value=True
)
@mock.patch(
    "snowflake.snowpark._internal.udf_utils._is_execution_environment_sandboxed",
    return_value=True,
)
@pytest.mark.parametrize(
    "local_testing, continue_registration",
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_do_register_udf_sandbox_mock_connection(
    utils_sandbox,
    session_sandbox,
    cleanup_registration_patch,
    local_testing,
    continue_registration,
):

    callback_side_effect_list = []

    def mock_callback(callableProperties):
        callback_side_effect_list.append(callableProperties)
        return continue_registration

    fake_connection = mock.create_autospec(MockServerConnection)
    fake_connection._conn = mock.Mock()
    fake_connection._telemetry_client = mock.Mock()
    fake_connection._local_testing = local_testing
    session = Session(fake_connection)
    session._runtime_version_from_requirement = None
    session.get_fully_qualified_name_if_possible = mock.Mock(
        return_value="database.schema"
    )
    session._import_paths = {}

    with mock.patch(
        "snowflake.snowpark._internal.udf_utils._should_continue_registration",
        new=mock_callback,
    ):
        if local_testing and continue_registration:
            # MockUDFRegistration object
            udf(
                lambda: 1,
                session=session,
                return_type=IntegerType(),
                packages=[],
                native_app_params={
                    "schema": "some_schema",
                    "application_roles": ["app_viewer"],
                },
            )
            cleanup_registration_patch.assert_not_called()
        else:
            udf(
                lambda: 1,
                session=session,
                return_type=IntegerType(),
                packages=[],
                native_app_params={
                    "schema": "some_schema",
                    "application_roles": ["app_viewer"],
                },
            )
            cleanup_registration_patch.assert_not_called()

    if local_testing:
        assert (
            len(callback_side_effect_list) == 0
        )  # Because of MockUDFRegistration object
    else:
        assert len(callback_side_effect_list) == 1
        callableProperties = callback_side_effect_list[0]
        assert not callableProperties.replace
        assert not callableProperties.is_permanent
        assert not callableProperties.secure
        assert callableProperties.object_type == TempObjectType.FUNCTION
        assert not callableProperties.if_not_exists
        assert callableProperties.object_name == "database.schema"
        assert len(callableProperties.input_args) == 0
        assert len(callableProperties.input_sql_types) == 0
        assert callableProperties.return_sql == "RETURNS INT"
        assert not callableProperties.strict
        assert not callableProperties.immutable
        assert callableProperties.runtime_version == "3.8"
        assert callableProperties.all_imports == ""
        assert callableProperties.all_packages == ""
        assert callableProperties.external_access_integrations is None
        assert callableProperties.secrets is None
        assert callableProperties.handler is None
        assert callableProperties.execute_as is None
        assert callableProperties.inline_python_code is None
        assert callableProperties.native_app_params == {
            "schema": "some_schema",
            "application_roles": ["app_viewer"],
        }
        assert callableProperties.import_paths == {}
        assert callableProperties.is_execution_environment_sandboxed.return_value
