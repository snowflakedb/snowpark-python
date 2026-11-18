#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib
import os
import sys
from unittest.mock import patch

import pytest

from snowflake.snowpark._internal.utils import IS_V5_DRIVER
from snowflake.snowpark.mock._util import ImportContext


def test_import_context():
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    test_file = os.path.join(cur_dir, "files", "udf_file.py")

    old_path = list(sys.path)
    old_modules = set(sys.modules.keys())

    # Positive Case
    with ImportContext({test_file}):
        import files.udf_file  # noqa: F401

        assert old_path != list(sys.path)
        assert old_modules != set(sys.modules.keys())

    assert old_path == list(sys.path)
    assert old_modules == set(sys.modules.keys())

    # Negative Case
    with pytest.raises(FileNotFoundError):
        with ImportContext({test_file, "foobar"}):
            pass

    assert old_path == list(sys.path)
    assert old_modules == set(sys.modules.keys())


@pytest.mark.parametrize("numpy_available", [True, False])
def test_numpy_installed(numpy_available):
    import snowflake.snowpark.mock._options as options

    # Connector 5 resolves numpy in its extras module. Reloading that module
    # replaces MissingOptionalDependency, so patch the exported object instead.
    if IS_V5_DRIVER:
        import snowflake.connector._common.extras as extras
        from snowflake.snowpark._internal.options import MissingOptionalDependency

        numpy_obj = (
            importlib.import_module("numpy")
            if numpy_available
            else extras.MissingOptionalDependency("numpy")
        )
        try:
            with patch.object(extras, "numpy", numpy_obj):
                importlib.reload(options)
                assert options.installed_numpy == numpy_available
                if numpy_available:
                    assert not isinstance(options.numpy, MissingOptionalDependency)
                else:
                    assert isinstance(options.numpy, MissingOptionalDependency)
        finally:
            importlib.reload(options)
        return

    real_import = importlib.import_module

    def maybe_import(name, package=None):
        if name == "numpy" and not numpy_available:
            raise ModuleNotFoundError()
        return real_import(name, package)

    try:
        with patch("importlib.import_module", maybe_import):
            importlib.reload(options)
            assert options.installed_numpy == numpy_available
            if numpy_available:
                assert not isinstance(options.numpy, options.MissingNumpy)
            else:
                assert isinstance(options.numpy, options.MissingNumpy)
    finally:
        importlib.reload(options)


def test_mocked_connection_reports_closed_after_close():
    from snowflake.snowpark.mock._connection import MockedSnowflakeConnection

    conn = MockedSnowflakeConnection()
    try:
        assert conn.is_closed() is False
        conn.close()
        assert conn.is_closed() is True
    finally:
        conn.close()
