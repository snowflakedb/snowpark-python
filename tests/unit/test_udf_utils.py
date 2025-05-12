#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import os
import pickle
import threading
from unittest import mock

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark._internal.udf_utils import (
    RegistrationType,
    add_snowpark_package_to_sproc_packages,
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    generate_anonymous_python_sp_sql,
    generate_python_code,
    get_error_message_abbr,
    pickle_function,
    resolve_imports_and_packages,
    resolve_packages_in_client_side_sandbox,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.types import StringType
from snowflake.snowpark.version import VERSION


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
    "packages",
    [
        ["random_package_one", "random_package_two"],
        ["random_package_one", "random_package_two", "random_package_two"],
        [
            "random_package_one",
            "random_package_one",
            "random_package_two",
            "random_package_two",
        ],
    ],
)
def test_resolve_packages_in_sandbox(packages):
    result = resolve_packages_in_client_side_sandbox(packages)
    assert len(result) == 2
    assert result[0] == "random_package_one"
    assert result[1] == "random_package_two"


@pytest.mark.parametrize(
    "packages, error_cls",
    [
        [
            [
                "random_package_one",
                "random_package_two==1.1",
                "random_package_two==1.4",
            ],
            ValueError,
        ],
        [
            [
                "random_package_one",
                "random_package_two==1.1",
                "random_package_two==1.2",
                "random_package_two==1.3",
            ],
            RuntimeError,
        ],
    ],
)
def test_resolve_packages_in_sandbox_with_value_error(packages, error_cls):
    with pytest.raises(error_cls):
        resolve_packages_in_client_side_sandbox(packages)


def test_resolve_imports_and_packages_in_sandbox():
    packages = ["random_package_one", "random_package_two"]
    (
        handler,
        inline_code,
        all_imports,
        all_packages,
        upload_file_stage_location,
        custom_python_runtime_version_allowed,
    ) = resolve_imports_and_packages(
        session=None,
        object_type=TempObjectType.FUNCTION,
        func=lambda: None,
        arg_names=["dummy_args"],
        udf_name="dummy_name",
        stage_location=None,
        imports=None,
        packages=packages,
    )

    assert handler is None
    assert inline_code is None
    assert all_imports == ""
    assert all_packages == ",".join([f"'{package}'" for package in packages])
    assert upload_file_stage_location is None
    assert not custom_python_runtime_version_allowed


def test_resolve_imports_and_packages_imports_as_str(tmp_path_factory):

    tmp_path = tmp_path_factory.mktemp("session_test")
    a_temp_file = tmp_path / "file.py"
    a_temp_file.write_text("any text is good")

    try:
        (
            handler,
            inline_code,
            all_imports,
            all_packages,
            upload_file_stage_location,
            custom_python_runtime_version_allowed,
        ) = resolve_imports_and_packages(
            session=None,
            object_type=TempObjectType.FUNCTION,
            func=lambda: None,
            arg_names=["dummy_args"],
            udf_name="dummy_name",
            stage_location=None,
            imports=[str(a_temp_file)],
            packages=None,
        )

        assert handler is None
        assert inline_code is None
        assert all_imports == ""
        assert all_packages == ""
        assert upload_file_stage_location is None
        assert not custom_python_runtime_version_allowed

        packages = ["random_package_one", "random_package_two"]
        (
            handler,
            inline_code,
            all_imports,
            all_packages,
            upload_file_stage_location,
            custom_python_runtime_version_allowed,
        ) = resolve_imports_and_packages(
            session=None,
            object_type=TempObjectType.FUNCTION,
            func=lambda: None,
            arg_names=["dummy_args"],
            udf_name="dummy_name",
            stage_location=None,
            imports=[(str(a_temp_file), "file")],
            packages=packages,
        )

        assert handler is None
        assert inline_code is None
        assert all_imports == ""
        assert all_packages == ",".join([f"'{package}'" for package in packages])
        assert upload_file_stage_location is None
        assert not custom_python_runtime_version_allowed

    finally:
        os.remove(a_temp_file)


@pytest.mark.parametrize(
    "packages",
    [
        None,
        ["random_package_one", "random_package_two"],
    ],
)
def test_add_snowpark_package_to_sproc_packages_add_package(packages):
    old_packages_length = len(packages) if packages else 0
    result = add_snowpark_package_to_sproc_packages(session=None, packages=packages)

    major, minor, patch = VERSION
    package_name = "snowflake-snowpark-python"
    final_name = f"{package_name}=={major}.{minor}.{patch}"

    assert len(result) == old_packages_length + 1
    assert final_name in result


def test_add_snowpark_package_to_sproc_packages_does_not_replace_package():
    packages = [
        "random_package_one",
        "random_package_two",
        "snowflake-snowpark-python==1.12.0",
    ]
    result = add_snowpark_package_to_sproc_packages(session=None, packages=packages)

    assert len(result) == len(packages)
    assert "snowflake-snowpark-python==1.12.0" in result


def test_add_snowpark_package_to_sproc_packages_to_session():
    fake_session = mock.create_autospec(Session)
    fake_session._packages = {
        "random_package_one": "random_package_one",
        "random_package_two": "random_package_two",
    }
    fake_session._package_lock = threading.RLock()
    result = add_snowpark_package_to_sproc_packages(session=fake_session, packages=None)

    major, minor, patch = VERSION
    package_name = "snowflake-snowpark-python"
    final_name = f"{package_name}=={major}.{minor}.{patch}"
    assert len(result) == 3
    assert final_name in result

    fake_session._packages[
        "snowflake-snowpark-python"
    ] = "snowflake-snowpark-python==1.12.0"
    result = add_snowpark_package_to_sproc_packages(session=fake_session, packages=None)
    assert result is None


@pytest.mark.parametrize("copy_grants", [True, False])
@pytest.mark.parametrize(
    "object_type",
    [
        TempObjectType.FUNCTION,
        TempObjectType.PROCEDURE,
        TempObjectType.TABLE_FUNCTION,
        TempObjectType.AGGREGATE_FUNCTION,
    ],
)
def test_copy_grant_for_udf_or_sp_registration(
    mock_session, mock_server_connection, copy_grants, object_type
):
    mock_session._conn = mock_server_connection
    mock_session._runtime_version_from_requirement = None
    with mock.patch.object(mock_session, "_run_query") as mock_run_query:
        create_python_udf_or_sp(
            session=mock_session,
            func=lambda: None,
            return_type=StringType(),
            input_args=[],
            opt_arg_defaults=[],
            handler="",
            object_type=object_type,
            object_name="",
            all_imports="",
            all_packages="",
            raw_imports=None,
            registration_type=RegistrationType.UDF,
            is_permanent=True,
            replace=False,
            if_not_exists=False,
            copy_grants=copy_grants,
        )
        if copy_grants:
            mock_run_query.assert_called_once()
            assert "COPY GRANTS" in mock_run_query.call_args[0][0]


def test_create_python_udf_or_sp_with_none_session():
    mock_callback = mock.MagicMock(return_value=False)

    with mock.patch(
        "snowflake.snowpark.context._should_continue_registration",
        new=mock_callback,
    ):
        create_python_udf_or_sp(
            session=None,
            func=lambda: None,
            return_type=StringType(),
            input_args=[],
            opt_arg_defaults=[],
            handler="",
            object_type=TempObjectType.FUNCTION,
            object_name="",
            all_imports="",
            all_packages="",
            raw_imports=None,
            registration_type=RegistrationType.UDF,
            is_permanent=True,
            replace=False,
            if_not_exists=False,
        )

    mock_callback.assert_called_once()


def test_generate_anonymous_python_sp_sql_with_none_session():
    mock_callback = mock.MagicMock(return_value=False)

    with mock.patch(
        "snowflake.snowpark.context._should_continue_registration",
        new=mock_callback,
    ):
        generate_anonymous_python_sp_sql(
            func=lambda: None,
            return_type=StringType(),
            input_args=[],
            handler="",
            object_name="",
            all_imports="",
            all_packages="",
            raw_imports=None,
        )

    mock_callback.assert_called_once()
