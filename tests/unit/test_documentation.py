#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools
import inspect
import runpy
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]


def documented_function():
    return None


@functools.wraps(documented_function)
def decorated_function():
    return documented_function()


class DocumentedClass:
    @property
    def value(self):
        return None


@pytest.fixture
def documentation_config():
    original_path = sys.path[:]
    try:
        return runpy.run_path(str(REPOSITORY / "docs/source/conf.py"))
    finally:
        sys.path[:] = original_path


@pytest.mark.parametrize("working_directory", ["repository", "docs", "elsewhere"])
@pytest.mark.parametrize(
    "name,target",
    [
        ("documented_function", documented_function),
        ("decorated_function", documented_function),
        ("DocumentedClass.value", DocumentedClass.value.fget),
    ],
)
def test_source_links_are_independent_of_working_directory(
    monkeypatch, tmp_path, documentation_config, working_directory, name, target
):
    directory = {
        "repository": REPOSITORY,
        "docs": REPOSITORY / "docs/source",
        "elsewhere": tmp_path,
    }[working_directory]
    monkeypatch.chdir(directory)
    source, line = inspect.getsourcelines(target)
    expected = (
        "https://github.com/snowflakedb/snowpark-python/blob/"
        f"v{documentation_config['release']}/tests/unit/test_documentation.py"
        f"#L{line}-L{line + len(source) - 1}"
    )
    assert (
        documentation_config["linkcode_resolve"](
            "py", {"module": __name__, "fullname": name}
        )
        == expected
    )


@pytest.mark.parametrize(
    "domain,module,name",
    [
        ("js", __name__, "documented_function"),
        ("py", "missing_module", "missing"),
        ("py", __name__, "missing"),
        ("py", "builtins", "len"),
        ("py", "inspect", "getsourcefile"),
    ],
)
def test_source_links_skip_unavailable_or_external_objects(
    documentation_config, domain, module, name
):
    assert (
        documentation_config["linkcode_resolve"](
            domain, {"module": module, "fullname": name}
        )
        is None
    )


def test_source_links_allow_unavailable_line_numbers(documentation_config):
    with patch("inspect.getsourcelines", side_effect=OSError):
        result = documentation_config["linkcode_resolve"](
            "py", {"module": __name__, "fullname": "documented_function"}
        )
    assert result.endswith("/tests/unit/test_documentation.py")


def test_configuration_loads_outside_documentation_directory(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    original_path = sys.path[:]
    try:
        config = runpy.run_path(str(REPOSITORY / "docs/source/conf.py"))
        assert config["REPO_ROOT"] == REPOSITORY
        assert Path(config["SRC_DIR"]) == REPOSITORY / "src"
    finally:
        sys.path[:] = original_path
