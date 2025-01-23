#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
from typing import Tuple
from unittest import mock

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    set_ast_state,
    AstFlagSource,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udtf
from snowflake.snowpark.udtf import UDTFRegistration

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


@mock.patch("snowflake.snowpark.udtf.cleanup_failed_permanent_registration")
def test_do_register_sp_negative(cleanup_registration_patch):
    AST_ENABLED = False
    set_ast_state(AstFlagSource.TEST, AST_ENABLED)
    fake_session = mock.create_autospec(Session)
    fake_session.ast_enabled = AST_ENABLED
    fake_session.get_fully_qualified_name_if_possible = mock.Mock(
        return_value="database.schema"
    )
    fake_session._run_query = mock.Mock(side_effect=ProgrammingError())
    fake_session._runtime_version_from_requirement = None
    fake_session._packages = []
    fake_session.udtf = UDTFRegistration(fake_session)
    with pytest.raises(SnowparkSQLException) as ex_info:

        @udtf(output_schema=["num"], session=fake_session)
        class UDTFProgrammingErrorTester:
            def process(self, n: int) -> Iterable[Tuple[int]]:
                yield (n,)

    assert ex_info.value.error_code == "1304"
    cleanup_registration_patch.assert_called()

    fake_session._run_query = mock.Mock(
        side_effect=BaseException("Test BaseException code path")
    )
    fake_session.udtf = UDTFRegistration(fake_session)
    with pytest.raises(BaseException, match="Test BaseException code path"):

        @udtf(output_schema=["num"], session=fake_session)
        class UDTFBaseExceptionTester:
            def process(self, n: int) -> Iterable[Tuple[int]]:
                yield (n,)

    cleanup_registration_patch.assert_called()


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
@mock.patch(
    "snowflake.snowpark.session._is_execution_environment_sandboxed_for_client",
    return_value=True,
)
def test_do_register_udtf_sandbox(session_sandbox, cleanup_registration_patch):

    callback_side_effect_list = []

    def mock_callback(extension_function_properties):
        callback_side_effect_list.append(extension_function_properties)
        return False  # i.e. don't register with Snowflake.

    with mock.patch(
        "snowflake.snowpark.context._should_continue_registration",
        new=mock_callback,
    ):

        @udtf(
            output_schema=["num"],
            native_app_params={
                "schema": "some_schema",
                "application_roles": ["app_viewer"],
            },
            _emit_ast=False,
        )
        class UDTFTester:
            def process(self, n: int) -> Iterable[Tuple[int]]:
                yield (n,)

    cleanup_registration_patch.assert_not_called()

    assert len(callback_side_effect_list) == 1
    extension_function_properties = callback_side_effect_list[0]
    assert not extension_function_properties.replace
    assert extension_function_properties.object_type == TempObjectType.FUNCTION
    assert not extension_function_properties.if_not_exists
    assert extension_function_properties.object_name != ""
    assert len(extension_function_properties.input_args) == 1
    assert len(extension_function_properties.input_sql_types) == 1
    assert extension_function_properties.return_sql == "RETURNS TABLE (NUM BIGINT)"
    assert (
        extension_function_properties.runtime_version
        == f"{sys.version_info[0]}.{sys.version_info[1]}"
    )
    assert extension_function_properties.all_imports == ""
    assert extension_function_properties.all_packages == ""
    assert extension_function_properties.external_access_integrations is None
    assert extension_function_properties.secrets is None
    assert extension_function_properties.handler is None
    assert extension_function_properties.execute_as is None
    assert extension_function_properties.inline_python_code is None
    assert extension_function_properties.raw_imports is None
    assert extension_function_properties.native_app_params == {
        "schema": "some_schema",
        "application_roles": ["app_viewer"],
    }
