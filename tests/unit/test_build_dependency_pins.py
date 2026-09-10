#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
"""Guards against unpinned build/release tooling (SNOW-4081342, CWE-829).

The release pipeline (`.github/workflows/python-publish.yml`) runs
`.github/scripts/install_protoc.sh`, then `tox -e protoc`, then `python -m build`
(whose `setup.py` `build_py` invokes `protoc-gen-mypy`), and finally signs and
publishes the artifacts. Every tool installed along that path executes code inside
a job holding `contents: write`, `id-token: write` and `PYPI_API_TOKEN`, so each
one must be resolved at an exact version rather than "whatever is newest on PyPI".
"""

import configparser
import re
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

PYPROJECT = REPO_ROOT / "pyproject.toml"
TOX_INI = REPO_ROOT / "tox.ini"
INSTALL_PROTOC = REPO_ROOT / ".github" / "scripts" / "install_protoc.sh"

# The version an unpinned resolve installs today on the release Python (3.11).
# Pinning below this would silently downgrade the code generator that produces the
# shipped ast_pb2.pyi stubs.
EXPECTED_MYPY_PROTOBUF = "5.1.0"
# py>=3.14 stays on 3.6.0; see the comment in pyproject.toml for why.
EXPECTED_MYPY_PROTOBUF_PY314 = "3.6.0"


def _requirement_names_and_specs(requirements):
    """Split PEP 508 requirement strings into (name, specifier, marker) triples."""
    parsed = []
    for raw in requirements:
        requirement, _, marker = raw.partition(";")
        match = re.match(r"^\s*([A-Za-z0-9._-]+)\s*(.*)$", requirement)
        assert match, f"could not parse requirement {raw!r}"
        parsed.append((match.group(1).lower(), match.group(2).strip(), marker.strip()))
    return parsed


def _build_system_requires():
    with open(PYPROJECT, "rb") as f:
        return tomllib.load(f)["build-system"]["requires"]


def _tox_protoc_deps():
    parser = configparser.ConfigParser()
    parser.read(TOX_INI)
    raw = parser["testenv:protoc"]["deps"]
    return [
        line.strip()
        for line in raw.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


# --------------------------------------------------------------------------- #
# mypy-protobuf: the tool named in the finding. Pinned in all three files.
# --------------------------------------------------------------------------- #


def test_pyproject_pins_mypy_protobuf_exactly():
    """Both marker branches of the build-system requirement use `==`."""
    pins = {
        marker: spec
        for name, spec, marker in _requirement_names_and_specs(_build_system_requires())
        if name == "mypy-protobuf"
    }
    assert pins, "mypy-protobuf missing from [build-system].requires"

    for marker, spec in pins.items():
        assert spec.startswith("=="), (
            f"mypy-protobuf is not exact-version pinned in pyproject.toml "
            f"(spec={spec!r}, marker={marker!r}). An unpinned or range-pinned build "
            f"requirement lets a newly published release run arbitrary code during "
            f"the signed release build (CWE-829)."
        )

    by_branch = {
        ("py314" if "3.14" in marker and ">=" in marker else "default"): spec
        for marker, spec in pins.items()
    }
    assert by_branch.get("default") == f"=={EXPECTED_MYPY_PROTOBUF}", (
        f"the python_version < '3.14' branch must stay at {EXPECTED_MYPY_PROTOBUF}, "
        f"the version an unpinned resolve installs today; got {by_branch.get('default')!r}. "
        f"Downgrading it changes the generated ast_pb2.pyi stubs."
    )
    assert by_branch.get("py314") == f"=={EXPECTED_MYPY_PROTOBUF_PY314}"


def test_tox_protoc_env_pins_mypy_protobuf_exactly():
    deps = {
        name: spec for name, spec, _ in _requirement_names_and_specs(_tox_protoc_deps())
    }
    assert "mypy-protobuf" in deps, "mypy-protobuf missing from [testenv:protoc] deps"
    assert deps["mypy-protobuf"] == f"=={EXPECTED_MYPY_PROTOBUF}", (
        f"[testenv:protoc] must exact-pin mypy-protobuf (got "
        f"{deps['mypy-protobuf']!r}); this env is invoked by python-publish.yml."
    )


def test_install_protoc_script_pins_mypy_protobuf_exactly():
    content = INSTALL_PROTOC.read_text()

    # Every install invocation must carry an exact pin, in both the uv and pip branches.
    install_lines = [
        line.strip()
        for line in content.splitlines()
        if re.search(r"^\s*(uv\s+)?pip install", line)
    ]
    assert len(install_lines) >= 2, (
        f"expected both the uv and pip install branches in {INSTALL_PROTOC.name}, "
        f"found {install_lines!r}"
    )
    for line in install_lines:
        assert (
            "mypy-protobuf==" in line or "MYPY_PROTOBUF_VERSION" in line
        ), f"unpinned mypy-protobuf install in {INSTALL_PROTOC.name}: {line!r}"

    assert re.search(
        rf'MYPY_PROTOBUF_VERSION="{re.escape(EXPECTED_MYPY_PROTOBUF)}"', content
    ), (
        f"{INSTALL_PROTOC.name} must pin mypy-protobuf to {EXPECTED_MYPY_PROTOBUF}, the "
        f"version an unpinned resolve installs today on the release Python."
    )

    assert not re.search(r"(uv )?pip install\s+mypy-protobuf(\s|$)", content), (
        f"{INSTALL_PROTOC.name} still contains a bare, unpinned "
        f"`pip install mypy-protobuf`"
    )


# --------------------------------------------------------------------------- #
# Sibling deps installed into the same signed release job. The Mythos draft left
# these open, so the checks are deliberately broader than the finding's title.
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("dep", ["protoc-wheel-0", "mypy-protobuf", "protobuf"])
def test_all_tox_protoc_deps_are_exact_pinned(dep):
    """No dependency in the release codegen env may float."""
    deps = {
        name: spec for name, spec, _ in _requirement_names_and_specs(_tox_protoc_deps())
    }
    assert dep in deps, f"{dep} missing from [testenv:protoc] deps"
    assert deps[dep].startswith("=="), (
        f"{dep} is not exact-version pinned in [testenv:protoc] (spec={deps[dep]!r}). "
        f"This env runs in the signed release job, so a floating dependency is the "
        f"same CWE-829 sink as an unpinned mypy-protobuf."
    )


def test_pyproject_protoc_wheel_is_exact_pinned():
    specs = [
        spec
        for name, spec, _ in _requirement_names_and_specs(_build_system_requires())
        if name == "protoc-wheel-0"
    ]
    assert specs, "protoc-wheel-0 missing from [build-system].requires"
    for spec in specs:
        assert spec.startswith(
            "=="
        ), f"protoc-wheel-0 must be exact-version pinned (got {spec!r})"


def test_pyproject_setuptools_is_bounded():
    """setuptools is bounded rather than exact-pinned, but must not float freely.

    pyproject.toml ships inside the sdist, so an exact setuptools pin would break
    source builds (conda-forge, distros) on newer Pythons that need a newer
    setuptools. A bounded range still removes the "any future upload is executed"
    exposure; full hash pinning is tracked as the stronger follow-up.
    """
    specs = [
        spec
        for name, spec, _ in _requirement_names_and_specs(_build_system_requires())
        if name == "setuptools"
    ]
    assert specs, "setuptools missing from [build-system].requires"
    for spec in specs:
        assert spec, "setuptools must not be completely unpinned in pyproject.toml"
        assert "<" in spec, (
            f"setuptools must carry an upper bound so an arbitrary future release is "
            f"not executed during the signed build (got {spec!r})"
        )
        assert (
            ">=" in spec
        ), f"setuptools must keep its lower bound for PEP 517 support (got {spec!r})"
