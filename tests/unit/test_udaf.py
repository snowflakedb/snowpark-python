#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
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
from snowflake.snowpark.functions import udaf
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.udaf import UDAFRegistration, UserDefinedAggregateFunction


def test_register_udaf_negative():
    AST_ENABLED = False
    set_ast_state(AstFlagSource.TEST, AST_ENABLED)
    fake_session = mock.create_autospec(Session)
    fake_session.ast_enabled = AST_ENABLED
    fake_session.udaf = UDAFRegistration(fake_session)
    with pytest.raises(TypeError, match="Invalid handler: expecting a class type"):
        fake_session.udaf.register(1)

    class FakeClass:
        pass

    sum_udaf = UserDefinedAggregateFunction(
        FakeClass, "fake_name", IntegerType(), [IntegerType()]
    )

    with pytest.raises(TypeError, match="must be Column or column name"):
        sum_udaf(1)

    with pytest.raises(
        ValueError, match="Incorrect number of arguments passed to the UDAF"
    ):
        sum_udaf("a", "b")


@mock.patch("snowflake.snowpark.udaf.cleanup_failed_permanent_registration")
def test_do_register_udaf_negative(cleanup_registration_patch):
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
    fake_session.udaf = UDAFRegistration(fake_session)
    with pytest.raises(SnowparkSQLException) as ex_info:

        @udaf(session=fake_session)
        class SnowparkSQLExceptionTestHandler:
            def __init__(self) -> None:
                self._sum = 0

            @property
            def aggregate_state(self) -> int:
                return self._sum

            def accumulate(self, input_value: int) -> None:
                self._sum += input_value

            def merge(self, other_sum: int) -> None:
                self._sum += other_sum

            def finish(self) -> int:
                return self._sum

    assert ex_info.value.error_code == "1304"
    cleanup_registration_patch.assert_called()

    fake_session._run_query = mock.Mock(
        side_effect=BaseException("Test BaseException code path")
    )
    fake_session.udaf = UDAFRegistration(fake_session)
    with pytest.raises(BaseException, match="Test BaseException code path"):

        @udaf(session=fake_session)
        class BaseExceptionTestHandler:
            def __init__(self) -> None:
                self._sum = 0

            @property
            def aggregate_state(self) -> int:
                return self._sum

            def accumulate(self, input_value: int) -> None:
                self._sum += input_value

            def merge(self, other_sum: int) -> None:
                self._sum += other_sum

            def finish(self) -> int:
                return self._sum

    cleanup_registration_patch.assert_called()


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
@mock.patch(
    "snowflake.snowpark.session._is_execution_environment_sandboxed_for_client",
    return_value=True,
)
def test_do_register_udaf_sandbox(session_sandbox, cleanup_registration_patch):

    callback_side_effect_list = []

    def mock_callback(extension_function_properties):
        callback_side_effect_list.append(extension_function_properties)
        return False  # i.e. don't register with Snowflake.

    with mock.patch(
        "snowflake.snowpark.context._should_continue_registration",
        new=mock_callback,
    ):

        class PythonSumUDAF:
            def __init__(self) -> None:
                self._sum = 0

            @property
            def aggregate_state(self):
                return self._sum

            def accumulate(self, input_value):
                self._sum += input_value

            def merge(self, other_sum):
                self._sum += other_sum

            def finish(self):
                return self._sum

        udaf(
            PythonSumUDAF,
            name="sum_int",
            replace=True,
            return_type=IntegerType(),
            input_types=[IntegerType()],
            _emit_ast=False,
        )

    cleanup_registration_patch.assert_not_called()

    assert len(callback_side_effect_list) == 1
    extension_function_properties = callback_side_effect_list[0]
    assert extension_function_properties.replace
    assert (
        extension_function_properties.object_type == TempObjectType.AGGREGATE_FUNCTION
    )
    assert not extension_function_properties.if_not_exists
    assert extension_function_properties.object_name != ""
    assert len(extension_function_properties.input_args) == 1
    assert len(extension_function_properties.input_sql_types) == 1
    assert extension_function_properties.return_sql == "RETURNS INT"
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
    assert extension_function_properties.native_app_params is None
