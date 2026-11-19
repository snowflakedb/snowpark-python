#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib.util
import subprocess
import sys
import sysconfig

import pytest

pytestmark = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="requires a free-threaded Python build",
)


@pytest.mark.parametrize(
    "modules",
    [
        ["snowflake.snowpark"],
        ["snowflake.snowpark", "pandas", "snowflake.connector.pandas_tools"],
    ],
)
def test_import_keeps_gil_disabled(modules):
    # Runs in a fresh interpreter because the GIL stays enabled once any extension module that is not
    # marked free-threading safe is imported, and this process has already imported the test dependencies.
    if any(importlib.util.find_spec(m.split(".")[0]) is None for m in modules):
        pytest.skip(f"{modules} not installed")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            f"import sys, {', '.join(modules)}; print(sys._is_gil_enabled())",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "False", result.stderr
