#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
import sys

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
