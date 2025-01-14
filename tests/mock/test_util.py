#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib
import os
import sys
from unittest.mock import patch

import pytest

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
    def bad_import(_):
        raise ModuleNotFoundError()

    with patch(
        "importlib.import_module",
        importlib.import_module if numpy_available else bad_import,
    ):
        import snowflake.snowpark.mock._options

        importlib.reload(snowflake.snowpark.mock._options)
        assert snowflake.snowpark.mock._options.installed_numpy == numpy_available
        if numpy_available:
            assert not isinstance(
                snowflake.snowpark.mock._options.numpy,
                snowflake.snowpark.mock._options.MissingNumpy,
            )
        else:
            assert isinstance(
                snowflake.snowpark.mock._options.numpy,
                snowflake.snowpark.mock._options.MissingNumpy,
            )
