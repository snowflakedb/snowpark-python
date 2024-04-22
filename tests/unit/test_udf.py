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
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.udf import UDFRegistration


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
def test_do_register_sp_negative(cleanup_registration_patch):
    fake_session = mock.create_autospec(Session)
    fake_session._runtime_version_from_requirement = None
    fake_session.get_fully_qualified_name_if_possible = mock.Mock(
        return_value="database.schema"
    )
    fake_session._run_query = mock.Mock(side_effect=ProgrammingError())
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


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
@mock.patch(
    "snowflake.snowpark.session._is_execution_environment_sandboxed", return_value=True
)
def test_do_register_udf_sandbox(session_sandbox, cleanup_registration_patch):

    callback_side_effect_list = []

    def mock_callback(extension_function_properties):
        callback_side_effect_list.append(extension_function_properties)
        return False  # i.e. don't register with Snowflake.

    with mock.patch(
        "snowflake.snowpark._internal.udf_utils._should_continue_registration",
        new=mock_callback,
    ):
        udf(
            lambda: 1,
            return_type=IntegerType(),
            packages=[],
            native_app_params={
                "schema": "some_schema",
                "application_roles": ["app_viewer"],
            },
        )

    cleanup_registration_patch.assert_not_called()

    assert len(callback_side_effect_list) == 1
    extension_function_properties = callback_side_effect_list[0]
    assert not extension_function_properties.replace
    assert extension_function_properties.object_type == TempObjectType.FUNCTION
    assert not extension_function_properties.if_not_exists
    assert extension_function_properties.object_name != ""
    assert len(extension_function_properties.input_args) == 0
    assert len(extension_function_properties.input_sql_types) == 0
    assert extension_function_properties.return_sql == "RETURNS INT"
    assert extension_function_properties.runtime_version == "3.8"
    assert extension_function_properties.all_imports == ""
    assert extension_function_properties.all_packages == ""
    assert extension_function_properties.external_access_integrations is None
    assert extension_function_properties.secrets is None
    assert extension_function_properties.handler is None
    assert extension_function_properties.execute_as is None
    assert extension_function_properties.inline_python_code is None
    assert extension_function_properties.import_paths == {}
    assert extension_function_properties.native_app_params == {
        "schema": "some_schema",
        "application_roles": ["app_viewer"],
    }
