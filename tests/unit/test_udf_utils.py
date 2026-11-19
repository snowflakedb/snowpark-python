#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import os
import pickle
import sys
import sysconfig
import threading
from unittest import mock

import cloudpickle
import pytest

from snowflake.connector import ProgrammingError
from snowflake.snowpark import Session
from snowflake.snowpark._internal.udf_utils import (
    RegistrationType,
    add_snowpark_package_to_sproc_packages,
    check_python_runtime_version,
    cleanup_failed_permanent_registration,
    create_python_udf_or_sp,
    generate_anonymous_python_sp_sql,
    generate_python_code,
    get_gil_runtime_version,
    get_error_message_abbr,
    get_func_arg_names,
    pickle_function,
    resolve_imports_and_packages,
    resolve_packages_in_client_side_sandbox,
)
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    get_python_runtime_version,
)
from snowflake.snowpark.context import (
    _ANACONDA_SHARED_REPOSITORY,
    _DEFAULT_ARTIFACT_REPOSITORY,
    _PYPI_SHARED_REPOSITORY,
)
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


def test_udf_init_once_decorator_simple():
    """Test @udf_init_once as simple decorator (same API as _snowflake.udf_init_once)."""
    from snowflake.snowpark.functions import udf_init_once

    @udf_init_once
    def my_init():
        pass

    # The client-side decorator is a no-op: it returns the function unchanged.
    assert my_init.__name__ == "my_init"
    assert callable(my_init)


def test_udf_init_once_rejects_non_zero_arg_function():
    from snowflake.snowpark.functions import udf_init_once

    with pytest.raises(TypeError, match="must take 0 arguments"):

        @udf_init_once
        def bad_init(x):
            return x


def test_udf_init_once_rejects_non_callable():
    from snowflake.snowpark.functions import udf_init_once

    with pytest.raises(TypeError, match="must be callable"):
        udf_init_once("not_a_function")


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
    assert all(f"'{package}'" in all_packages for package in packages)
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
        assert all(f"'{package}'" in all_packages for package in packages)
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
    result = add_snowpark_package_to_sproc_packages(
        session=None, packages=packages, artifact_repository=_ANACONDA_SHARED_REPOSITORY
    )

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
    result = add_snowpark_package_to_sproc_packages(
        session=None, packages=packages, artifact_repository=_ANACONDA_SHARED_REPOSITORY
    )

    assert len(result) == len(packages)
    assert "snowflake-snowpark-python==1.12.0" in result


def test_add_snowpark_package_to_sproc_packages_to_session():
    fake_session = mock.create_autospec(Session)
    fake_session._packages = {
        "random_package_one": "random_package_one",
        "random_package_two": "random_package_two",
    }
    fake_session._package_lock = threading.RLock()
    fake_session._get_packages_by_artifact_repository.side_effect = (
        lambda a: Session._get_packages_by_artifact_repository(fake_session, a)
    )
    result = add_snowpark_package_to_sproc_packages(
        session=fake_session,
        packages=None,
        artifact_repository=_DEFAULT_ARTIFACT_REPOSITORY,
    )

    major, minor, patch = VERSION
    package_name = "snowflake-snowpark-python"
    final_name = f"{package_name}=={major}.{minor}.{patch}"
    assert len(result) == 3
    assert final_name in result

    fake_session._packages[
        "snowflake-snowpark-python"
    ] = "snowflake-snowpark-python==1.12.0"
    result = add_snowpark_package_to_sproc_packages(
        session=fake_session,
        packages=None,
        artifact_repository=_DEFAULT_ARTIFACT_REPOSITORY,
    )
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


def test_get_func_arg_names():
    def my_no_arg_sproc(session):
        pass

    arg_names = get_func_arg_names(my_no_arg_sproc, TempObjectType.PROCEDURE, 0, False)
    assert arg_names == []
    arg_names = get_func_arg_names(my_no_arg_sproc, TempObjectType.PROCEDURE, 0, True)
    assert arg_names == []

    def my_sproc(session, first_arg, second_arg):
        pass

    arg_names = get_func_arg_names(my_sproc, TempObjectType.PROCEDURE, 2, True)
    assert arg_names == ["first_arg", "second_arg"]
    arg_names = get_func_arg_names(my_sproc, TempObjectType.PROCEDURE, 2, False)
    assert arg_names == ["arg1", "arg2"]

    def my_udf(x, y, z):
        pass

    arg_names = get_func_arg_names(my_udf, TempObjectType.FUNCTION, 3, True)
    assert arg_names == ["x", "y", "z"]

    # wrong number of arguments found should fallback to default arg names
    arg_names = get_func_arg_names(my_udf, TempObjectType.FUNCTION, 2, True)
    assert arg_names == ["arg1", "arg2"]

    # failures should fallback to default arg names
    # we can reproduce failure by passing in a python builtin function, which cannot be inspected
    arg_names = get_func_arg_names(min, TempObjectType.FUNCTION, 2, True)
    assert arg_names == ["arg1", "arg2"]

    class MyUDTF:
        def process(self, x, y, z):
            return [(x, y, z)]

    arg_names = get_func_arg_names(MyUDTF, TempObjectType.TABLE_FUNCTION, 3, True)
    assert arg_names == ["x", "y", "z"]

    class SumUDAF:
        def __init__(self) -> None:
            self._partial_sum = 0

        @property
        def aggregate_state(self):
            return self._partial_sum

        def accumulate(self, input_value, input_value2):
            self._partial_sum += input_value + input_value2

        def merge(self, other_partial_sum):
            self._partial_sum += other_partial_sum

        def finish(self):
            return self._partial_sum

    arg_names = get_func_arg_names(SumUDAF, TempObjectType.AGGREGATE_FUNCTION, 2, True)
    assert arg_names == ["input_value", "input_value2"]

    # wrong class type should fallback to default arg names
    arg_names = get_func_arg_names(SumUDAF, TempObjectType.TABLE_FUNCTION, 2, True)
    assert arg_names == ["arg1", "arg2"]


def test_resolve_imports_and_packages_non_conda_injects_cloudpickle_ge():
    """Auto-injected cloudpickle uses >= not == so the server can resolve a
    compatible version for the target runtime (SNOW-3081273)."""
    import cloudpickle

    _, _, _, all_packages, _, _ = resolve_imports_and_packages(
        session=None,
        object_type=TempObjectType.PROCEDURE,
        func=lambda: None,
        arg_names=[],
        udf_name="test_sp",
        stage_location=None,
        imports=None,
        packages=["snowflake-snowpark-python"],
        artifact_repository="SNOWPARK_PYTHON_TEST_REPOSITORY",
    )
    assert all_packages is not None
    assert f"cloudpickle>={cloudpickle.__version__}" in all_packages
    assert f"cloudpickle=={cloudpickle.__version__}" not in all_packages


def _package_names(all_packages: str):
    from packaging.requirements import Requirement

    names = []
    for part in all_packages.split(","):
        spec = part.strip().strip("'")
        if spec:
            try:
                names.append(Requirement(spec).name.lower())
            except Exception:
                names.append(spec.lower())
    return names


def _resolve_ar_packages(session, packages, is_pandas_udf):
    def _call():
        _, _, _, all_packages, _, _ = resolve_imports_and_packages(
            session=session,
            object_type=TempObjectType.FUNCTION,
            func=lambda x: x,
            arg_names=["x"],
            udf_name="test_udf",
            stage_location=None,
            imports=None,
            packages=packages,
            is_pandas_udf=is_pandas_udf,
            is_dataframe_input=False,
            artifact_repository=_PYPI_SHARED_REPOSITORY,
        )
        return all_packages

    if session is None:
        return _call()
    with mock.patch.object(session, "get_session_stage", return_value="@test_stage"):
        with mock.patch.object(session, "_resolve_imports", return_value=[]):
            return _call()


# SNOW-4130609: PyPI AR + explicit packages now injects pandas for pandas_udf.


def test_ar_pandas_udf_explicit_packages_injects_pandas(session):
    names = _package_names(_resolve_ar_packages(session, ["numpy"], True))
    assert {"numpy", "cloudpickle", "pandas"} <= set(names)
    assert names.count("pandas") == 1


def test_ar_regular_udf_explicit_packages_does_not_inject_pandas(session):
    names = _package_names(_resolve_ar_packages(session, ["numpy"], False))
    assert {"numpy", "cloudpickle"} <= set(names)
    assert "pandas" not in names


def test_ar_pandas_udf_does_not_duplicate_explicit_pandas(session):
    all_packages = _resolve_ar_packages(session, ["numpy", "pandas>=2.0"], True)
    assert _package_names(all_packages).count("pandas") == 1
    assert "pandas>=2.0" in all_packages


def test_ar_pandas_udf_packages_none_still_injects_pandas(session):
    names = _package_names(_resolve_ar_packages(session, None, True))
    assert "pandas" in names


def test_ar_explicit_packages_ignore_session_packages(session):
    pkg_dict = session._get_packages_by_artifact_repository(_PYPI_SHARED_REPOSITORY)
    pkg_dict["numpy"] = "numpy==1.26.0"
    names = _package_names(_resolve_ar_packages(session, ["cloudpickle"], False))
    assert "numpy" not in names
    assert pkg_dict == {"numpy": "numpy==1.26.0"}


def test_ar_non_str_packages_raise_typeerror(session):
    with pytest.raises(
        TypeError,
        match="Non-conda artifact repository requires that all packages be passed as str or module",
    ):
        _resolve_ar_packages(session, [1], False)


@pytest.mark.parametrize("use_session", [True, False])
def test_ar_module_packages_pinned_to_local_version(session, use_session):
    import importlib.metadata

    all_packages = _resolve_ar_packages(
        session if use_session else None, [cloudpickle], False
    )
    assert f"cloudpickle=={importlib.metadata.version('cloudpickle')}" in all_packages


def test_sandbox_ar_pandas_udf_unparseable_spec_still_injects_pandas():
    # Requirement() raises on this spec, so the startswith fallback must run.
    names = _package_names(
        _resolve_ar_packages(None, ["numpy", "invalid package!!!"], True)
    )
    assert "pandas" in names
    assert names.count("pandas") == 1


def _patch_gil_disabled(value):
    get_config_var = sysconfig.get_config_var
    return mock.patch(
        "snowflake.snowpark._internal.utils.sysconfig.get_config_var",
        side_effect=lambda name: value
        if name == "Py_GIL_DISABLED"
        else get_config_var(name),
    )


@pytest.fixture
def free_threaded_runtime_version():
    """Makes the running interpreter look like a free-threaded build; yields its expected RUNTIME_VERSION."""
    get_python_runtime_version.cache_clear()
    with _patch_gil_disabled(1):
        yield f"{sys.version_info[0]}.{sys.version_info[1]}t"
    get_python_runtime_version.cache_clear()


@pytest.mark.parametrize("gil_disabled, suffix", [(None, ""), (0, ""), (1, "t")])
def test_get_python_runtime_version(gil_disabled, suffix):
    get_python_runtime_version.cache_clear()
    try:
        with _patch_gil_disabled(gil_disabled):
            assert (
                get_python_runtime_version()
                == f"{sys.version_info[0]}.{sys.version_info[1]}{suffix}"
            )
    finally:
        get_python_runtime_version.cache_clear()


def test_check_python_runtime_version_free_threaded(free_threaded_runtime_version):
    check_python_runtime_version(None)
    check_python_runtime_version(free_threaded_runtime_version)
    with pytest.raises(ValueError, match="Cloudpickle can only be used"):
        check_python_runtime_version(free_threaded_runtime_version[:-1])


def _create_udf_query(mock_session, mock_server_connection, **kwargs):
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
            object_type=TempObjectType.FUNCTION,
            object_name="",
            all_imports="",
            all_packages="",
            raw_imports=None,
            registration_type=RegistrationType.UDF,
            is_permanent=True,
            replace=False,
            if_not_exists=False,
            **kwargs,
        )
    return mock_run_query.call_args[0][0]


def test_create_python_udf_or_sp_free_threaded_runtime_version(
    mock_session, mock_server_connection, free_threaded_runtime_version
):
    query = _create_udf_query(mock_session, mock_server_connection)
    assert f"RUNTIME_VERSION='{free_threaded_runtime_version}'" in query


def test_create_python_udf_or_sp_explicit_runtime_version_is_quoted(
    mock_session, mock_server_connection
):
    query = _create_udf_query(
        mock_session, mock_server_connection, runtime_version="3.10"
    )
    assert "RUNTIME_VERSION='3.10'" in query


def test_generate_anonymous_python_sp_sql_free_threaded_runtime_version(
    free_threaded_runtime_version,
):
    sql = generate_anonymous_python_sp_sql(
        func=lambda: None,
        return_type=StringType(),
        input_args=[],
        handler="",
        object_name="",
        all_imports="",
        all_packages="",
        raw_imports=None,
    )
    assert f"RUNTIME_VERSION='{free_threaded_runtime_version}'" in sql


def test_get_gil_runtime_version(mock_session, free_threaded_runtime_version):
    assert get_gil_runtime_version(None) == free_threaded_runtime_version[:-1]
    mock_session._runtime_version_from_requirement = None
    assert get_gil_runtime_version(mock_session) == free_threaded_runtime_version[:-1]
    mock_session._runtime_version_from_requirement = "3.13t"
    assert get_gil_runtime_version(mock_session) == "3.13"
    mock_session._runtime_version_from_requirement = "3.12"
    assert get_gil_runtime_version(mock_session) == "3.12"


def _register_udf(mock_session, **kwargs):
    return create_python_udf_or_sp(
        session=mock_session,
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
        **kwargs,
    )


def _unsupported_runtime_error(runtime_version):
    return ProgrammingError(
        f"001422 (22023): SQL compilation error:\n"
        f"invalid value '{runtime_version}' for property 'RUNTIME_VERSION'"
    )


def test_create_python_udf_or_sp_falls_back_when_free_threaded_runtime_unsupported(
    mock_session, mock_server_connection, free_threaded_runtime_version
):
    mock_session._conn = mock_server_connection
    mock_session._runtime_version_from_requirement = None
    mock_session._free_threaded_runtime_unsupported = False
    gil_runtime_version = free_threaded_runtime_version[:-1]
    with mock.patch.object(
        mock_session,
        "_run_query",
        side_effect=[_unsupported_runtime_error(free_threaded_runtime_version), None],
    ) as mock_run_query:
        _register_udf(mock_session)
    queries = [c[0][0] for c in mock_run_query.call_args_list]
    assert f"RUNTIME_VERSION='{free_threaded_runtime_version}'" in queries[0]
    assert f"RUNTIME_VERSION='{gil_runtime_version}'" in queries[1]
    assert mock_session._free_threaded_runtime_unsupported is True

    # The rejection is remembered, so later registrations go straight to the GIL runtime.
    with mock.patch.object(mock_session, "_run_query") as mock_run_query:
        _register_udf(mock_session)
    mock_run_query.assert_called_once()
    assert f"RUNTIME_VERSION='{gil_runtime_version}'" in mock_run_query.call_args[0][0]


def test_create_python_udf_or_sp_explicit_free_threaded_runtime_does_not_fall_back(
    mock_session, mock_server_connection, free_threaded_runtime_version
):
    mock_session._conn = mock_server_connection
    mock_session._runtime_version_from_requirement = None
    mock_session._free_threaded_runtime_unsupported = False
    with mock.patch.object(
        mock_session,
        "_run_query",
        side_effect=_unsupported_runtime_error(free_threaded_runtime_version),
    ) as mock_run_query:
        with pytest.raises(ProgrammingError, match="RUNTIME_VERSION"):
            _register_udf(mock_session, runtime_version=free_threaded_runtime_version)
    mock_run_query.assert_called_once()
    assert mock_session._free_threaded_runtime_unsupported is False


def test_create_python_udf_or_sp_unrelated_error_does_not_fall_back(
    mock_session, mock_server_connection, free_threaded_runtime_version
):
    mock_session._conn = mock_server_connection
    mock_session._runtime_version_from_requirement = None
    mock_session._free_threaded_runtime_unsupported = False
    with mock.patch.object(
        mock_session, "_run_query", side_effect=ProgrammingError("002003: no schema")
    ) as mock_run_query:
        with pytest.raises(ProgrammingError, match="no schema"):
            _register_udf(mock_session)
    mock_run_query.assert_called_once()
    assert mock_session._free_threaded_runtime_unsupported is False


def test_generate_anonymous_python_sp_sql_uses_gil_runtime_once_rejected(
    mock_session, free_threaded_runtime_version
):
    mock_session._free_threaded_runtime_unsupported = True
    sql = generate_anonymous_python_sp_sql(
        func=lambda: None,
        return_type=StringType(),
        input_args=[],
        handler="",
        object_name="",
        all_imports="",
        all_packages="",
        raw_imports=None,
        session=mock_session,
    )
    assert f"RUNTIME_VERSION='{free_threaded_runtime_version[:-1]}'" in sql
