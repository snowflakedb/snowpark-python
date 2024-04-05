#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
import os
import pickle
from unittest import mock

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.server_connection import ServerConnection
from snowflake.snowpark._internal.udf_utils import (
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    generate_python_code,
    get_error_message_abbr,
    pickle_function,
    resolve_imports_and_packages,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.types import StringType


def test_get_error_message_abbr_exception():
    with pytest.raises(ValueError, match="Expect FUNCTION of PROCEDURE"):
        get_error_message_abbr(TempObjectType.VIEW)


def test_cleanup_failed_permanent_registration_exception(
    mock_server_connection, caplog
):
    fake_session = Session(mock_server_connection)
    with mock.patch.object(
        fake_session, "_run_query", side_effect=Exception("fake exception")
    ):
        with caplog.at_level(logging.WARNING):
            cleanup_failed_permanent_registration(
                fake_session, "fake_upload_file_stage_location", "fake_stage_location"
            )
    assert "Failed to clean uploaded file" in caplog.text


def test_pickle_function_exception():
    with mock.patch("cloudpickle.dumps", side_effect=TypeError("fake error")):
        with pytest.raises(TypeError, match="fake error"):
            pickle_function(lambda: None)

    with mock.patch(
        "cloudpickle.dumps", side_effect=pickle.PicklingError("fake error")
    ):
        with pytest.raises(
            pickle.PicklingError, match="fake error.*save the unpicklable object"
        ):
            pickle_function(lambda: None)


def test_generate_python_code_exception():
    with mock.patch(
        "snowflake.snowpark._internal.code_generation.generate_source_code",
        side_effect=Exception("fake error"),
    ):
        generated_code = generate_python_code(
            func=lambda: None,
            arg_names=[],
            object_type=TempObjectType.FUNCTION,
            is_pandas_udf=False,
            is_dataframe_input=False,
            source_code_display=True,
        )
        assert "Source code comment could not be generated" in generated_code


@pytest.mark.parametrize(
    "local_testing, packages",
    [
        (
            True,
            ["random_package_one", "random_package_two"],
        ),  # Mock connection with local testing
        (True, []),  # Mock connection with local testing
        (
            False,
            ["random_package_one", "random_package_two"],
        ),  # Mock connection in sandbox
        (False, []),  # Mock connection in sandbox
    ],
)
def test_resolve_imports_and_packages_empty_imports(local_testing, packages):
    fake_connection = mock.create_autospec(MockServerConnection)
    fake_connection._conn = mock.Mock()
    fake_connection._local_testing = local_testing
    session = Session(fake_connection)

    with mock.patch(
        "snowflake.snowpark._internal.udf_utils._is_execution_environment_sandboxed",
        return_value=True,
    ), mock.patch(
        "snowflake.snowpark.session._is_execution_environment_sandboxed",
        return_value=True,
    ):
        (
            handler,
            inline_code,
            all_imports,
            all_packages,
            udf_level_imports,
            upload_file_stage_location,
            custom_python_runtime_version_allowed,
        ) = resolve_imports_and_packages(
            session=session,
            object_type=TempObjectType.FUNCTION,
            func=lambda: None,
            arg_names=[],
            udf_name="random_udf",
            stage_location=None,  # will call session.get_session_stage
            imports=None,
            packages=packages,
        )

        assert handler is None
        assert inline_code is None
        assert all_imports == ""
        assert all_packages == ",".join([f"'{package}'" for package in packages])
        assert udf_level_imports == {}
        assert upload_file_stage_location is None
        assert not custom_python_runtime_version_allowed


@pytest.mark.parametrize("local_testing", [True, False])
def test_resolve_imports_and_packages_imports_as_str(local_testing, tmp_path_factory):
    fake_connection = mock.create_autospec(MockServerConnection)
    fake_connection._conn = mock.Mock()
    fake_connection._local_testing = local_testing
    session = Session(fake_connection)

    tmp_path = tmp_path_factory.mktemp("session_test")
    a_temp_file = tmp_path / "file.py"
    a_temp_file.write_text("any text is good")

    try:
        with mock.patch(
            "snowflake.snowpark._internal.udf_utils._is_execution_environment_sandboxed",
            return_value=True,
        ), mock.patch(
            "snowflake.snowpark.session._is_execution_environment_sandboxed",
            return_value=True,
        ):
            (
                handler,
                inline_code,
                all_imports,
                all_packages,
                udf_level_imports,
                upload_file_stage_location,
                custom_python_runtime_version_allowed,
            ) = resolve_imports_and_packages(
                session=session,
                object_type=TempObjectType.FUNCTION,
                func=lambda: None,
                arg_names=[],
                udf_name="random_udf",
                stage_location=None,  # will call session.get_session_stage
                imports=[str(a_temp_file)],
                packages=None,
            )

            assert handler is None
            assert inline_code is None
            assert all_imports == ""
            assert all_packages == ""
            assert str(a_temp_file) in udf_level_imports
            assert upload_file_stage_location is None
            assert not custom_python_runtime_version_allowed

            (
                handler,
                inline_code,
                all_imports,
                all_packages,
                udf_level_imports,
                upload_file_stage_location,
                custom_python_runtime_version_allowed,
            ) = resolve_imports_and_packages(
                session=session,
                object_type=TempObjectType.FUNCTION,
                func=lambda: None,
                arg_names=[],
                udf_name="random_udf",
                stage_location=None,  # will call session.get_session_stage
                imports=[(str(a_temp_file), "file")],
                packages=None,
            )

            assert handler is None
            assert inline_code is None
            assert all_imports == ""
            assert all_packages == ""
            assert str(a_temp_file) in udf_level_imports
            assert upload_file_stage_location is None
            assert not custom_python_runtime_version_allowed

            with pytest.raises(TypeError):
                (
                    handler,
                    inline_code,
                    all_imports,
                    all_packages,
                    udf_level_imports,
                    upload_file_stage_location,
                    custom_python_runtime_version_allowed,
                ) = resolve_imports_and_packages(
                    session=session,
                    object_type=TempObjectType.FUNCTION,
                    func=lambda: None,
                    arg_names=[],
                    udf_name="random_udf",
                    stage_location=None,  # will call session.get_session_stage
                    imports=[{}],
                    packages=None,
                )
    finally:
        os.remove(a_temp_file)


@pytest.mark.parametrize(
    "connection_server_type, local_testing",
    [
        (ServerConnection, None),
        (MockServerConnection, True),  # Mock connection with local testing
        (MockServerConnection, False),
    ],  # Mock connection in sandbox
)
def test_create_python_udf_or_sp(connection_server_type, local_testing):

    mock_callback = mock.MagicMock(return_value=False)

    fake_connection = mock.create_autospec(connection_server_type)
    fake_connection._conn = mock.Mock()
    if local_testing is not None:
        fake_connection._local_testing = local_testing
    session = Session(fake_connection)
    session._runtime_version_from_requirement = "3.8"

    with mock.patch(
        "snowflake.snowpark._internal.udf_utils._should_continue_registration",
        new=mock_callback,
    ):
        create_python_udf_or_sp(
            session=session,
            return_type=StringType(),
            input_args=[],
            all_imports="",
            all_packages="",
            handler="",
            object_type=TempObjectType.FUNCTION,
            object_name="",
            is_permanent=True,
            replace=False,
            if_not_exists=False,
        )

    mock_callback.assert_called_once()
