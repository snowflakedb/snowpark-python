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
from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.udf import UDFRegistration


@mock.patch("snowflake.snowpark.udf.cleanup_failed_permanent_registration")
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
    "snowflake.snowpark.session._is_execution_environment_sandboxed_for_client",
    return_value=True,
)
def test_do_register_udf_sandbox(session_sandbox, cleanup_registration_patch):

    callback_side_effect_list = []

    def mock_callback(extension_function_properties):
        callback_side_effect_list.append(extension_function_properties)
        return False  # i.e. don't register with Snowflake.

    with mock.patch(
        "snowflake.snowpark.context._should_continue_registration",
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
            _emit_ast=False,
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


def test_artifact_repository_adds_cloudpickle():
    """Test that cloudpickle is automatically added when using artifact_repository with packages."""
    from snowflake.snowpark._internal.udf_utils import resolve_imports_and_packages

    # Test case 1: packages provided without cloudpickle
    result = resolve_imports_and_packages(
        session=None,
        object_type=TempObjectType.FUNCTION,
        func=lambda: 1,
        arg_names=[],
        udf_name="test_udf",
        stage_location=None,
        imports=None,
        packages=["urllib3", "requests"],
        artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
    )
    _, _, _, all_packages, _, _ = result

    # Verify cloudpickle was added
    assert all_packages is not None
    package_list = all_packages.split(",") if all_packages else []
    assert any(
        pkg.strip().strip("'").startswith("cloudpickle==") for pkg in package_list
    ), f"cloudpickle not found in packages: {all_packages}"

    # Test case 2: packages already contains cloudpickle
    result2 = resolve_imports_and_packages(
        session=None,
        object_type=TempObjectType.FUNCTION,
        func=lambda: 1,
        arg_names=[],
        udf_name="test_udf2",
        stage_location=None,
        imports=None,
        packages=["urllib3", "cloudpickle>=2.0", "requests"],
        artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
    )
    _, _, _, all_packages2, _, _ = result2

    # Verify cloudpickle was not duplicated
    package_list2 = all_packages2.split(",") if all_packages2 else []
    cloudpickle_count = sum(
        1 for pkg in package_list2 if "cloudpickle" in pkg.strip().strip("'").lower()
    )
    assert (
        cloudpickle_count == 1
    ), f"cloudpickle should appear exactly once, found {cloudpickle_count} times in: {all_packages2}"

    # Test case 3: packages with various version specifiers
    test_cases = [
        ["urllib3", "cloudpickle==2.2.1"],
        ["urllib3", "cloudpickle>=2.0"],
        ["urllib3", "cloudpickle~=2.2"],
        ["urllib3", "cloudpickle<=3.0"],
    ]

    for packages in test_cases:
        result = resolve_imports_and_packages(
            session=None,
            object_type=TempObjectType.FUNCTION,
            func=lambda: 1,
            arg_names=[],
            udf_name="test_udf_versioned",
            stage_location=None,
            imports=None,
            packages=packages,
            artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
        )
        _, _, _, all_packages, _, _ = result
        package_list = all_packages.split(",") if all_packages else []
        cloudpickle_count = sum(
            1 for pkg in package_list if "cloudpickle" in pkg.strip().strip("'").lower()
        )
        assert (
            cloudpickle_count == 1
        ), f"For {packages}, cloudpickle should appear exactly once, found {cloudpickle_count} times"
