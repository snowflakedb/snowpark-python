#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os as _os
import sys
import sys as _sys
import threading
from collections import defaultdict
from typing import Tuple
from unittest import mock

import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Session
import snowflake.snowpark.session as session_module
from snowflake.snowpark._internal.udf_utils import resolve_imports_and_packages
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    set_ast_state,
    AstFlagSource,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udtf
from snowflake.snowpark.udtf import UDTFRegistration

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
    fake_session.udtf = UDTFRegistration(fake_session)
    fake_session._artifact_repository_packages = defaultdict(dict)
    fake_session._packages = {}
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


# SNOW-4174500: session-imported stage paths must be re-staged, not forwarded verbatim.
_APP_STAGE_PATH = '@SAMOOHA_APP_PKG."APP_ARTIFACTS_V1_0_82".APP_FILES/pandas_helper.py'


def test_bug_reproduced_imports_none_inherits_all_session_paths():
    """Bug baseline: imports=None inherits all session stage paths (002003/093023 trigger)."""
    session = mock.MagicMock()
    session._import_paths = {_APP_STAGE_PATH: (None, None)}
    session._lock = threading.RLock()
    session._resolve_imports.return_value = [_APP_STAGE_PATH]
    session.get_session_stage.return_value = "@TEMP_SESSION_STAGE"
    session._get_default_artifact_repository.return_value = "conda_channel"
    session._get_packages_by_artifact_repository.return_value = {}
    session._resolve_packages.return_value = ["'cloudpickle>=3.1.1'"]
    session._runtime_version_from_requirement = None

    _, _, all_imports, _, _, _ = resolve_imports_and_packages(
        session=session,
        object_type=TempObjectType.TABLE_FUNCTION,
        func=lambda pdf: pdf,
        arg_names=["pdf"],
        udf_name="test_udtf",
        stage_location=None,
        imports=None,
        packages=None,
    )
    assert _APP_STAGE_PATH in all_imports


def test_fix_redirect_is_called_for_session_level_imports(tmp_path):
    """Fix: session-level imports are re-staged; the original stage path must not appear."""
    (tmp_path / "pandas_helper.py").write_text("SCALE = 1\n")
    import_dir = f"{tmp_path}{_os.sep}"

    with Session.builder.config("local_testing", True).create() as real_session:
        real_session._import_paths[_APP_STAGE_PATH] = (None, None)
        with mock.patch.object(
            session_module, "is_in_stored_procedure", return_value=True
        ), mock.patch.dict(
            _sys._xoptions, {"snowflake_import_directory": import_dir}
        ), mock.patch.object(
            real_session, "_list_files_in_stage", return_value=set()
        ), mock.patch.object(
            real_session._conn, "upload_stream"
        ):
            resolved = real_session._resolve_imports("@TEMP_STAGE", "@TEMP_STAGE")
        real_session._import_paths.pop(_APP_STAGE_PATH, None)

    assert all(_APP_STAGE_PATH not in r for r in resolved)
    assert any("pandas_helper" in r for r in resolved)


def test_fix_explicit_udf_imports_bypass_redirect(tmp_path):
    """Explicit imports= are the caller's choice and must not be redirected."""
    (tmp_path / "pandas_helper.py").write_text("SCALE = 1\n")
    import_dir = f"{tmp_path}{_os.sep}"

    with Session.builder.config("local_testing", True).create() as real_session:
        with mock.patch.object(
            session_module, "is_in_stored_procedure", return_value=True
        ), mock.patch.dict(
            _sys._xoptions, {"snowflake_import_directory": import_dir}
        ), mock.patch.object(
            real_session,
            "_redirect_inherited_stage_imports",
            wraps=real_session._redirect_inherited_stage_imports,
        ) as mock_redirect, mock.patch.object(
            real_session, "_list_files_in_stage", return_value=set()
        ), mock.patch.object(
            real_session._conn, "upload_stream"
        ):
            real_session._resolve_imports(
                "@TEMP_STAGE", "@TEMP_STAGE", {_APP_STAGE_PATH: (None, None)}
            )

    mock_redirect.assert_not_called()
