#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import configparser
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import yaml

from scripts.check_jdbc_test_results import (
    MINIMUM_TESTS,
    VENDOR_SMOKE_TESTS,
    check_results,
)


@pytest.fixture
def jdbc_report(tmp_path):
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite")
    names = sorted(VENDOR_SMOKE_TESTS) + [
        f"test_jdbc_{index}" for index in range(MINIMUM_TESTS - len(VENDOR_SMOKE_TESTS))
    ]
    for name in names:
        ET.SubElement(
            suite, "testcase", classname="tests.integ.datasource.test_jdbc", name=name
        )
    return root, tmp_path / "jdbc.xml"


def test_jdbc_report_success(jdbc_report):
    root, path = jdbc_report
    ET.ElementTree(root).write(path)
    check_results(path)


@pytest.mark.parametrize("tag", ["skipped", "failure", "error"])
def test_jdbc_report_rejects_nonpassing_test(jdbc_report, tag):
    root, path = jdbc_report
    ET.SubElement(root.find(".//testcase"), tag)
    ET.ElementTree(root).write(path)
    with pytest.raises(ValueError, match="skipped, failed or errored"):
        check_results(path)


@pytest.mark.parametrize("empty", [True, False])
def test_jdbc_report_rejects_incomplete_run(jdbc_report, empty):
    root, path = jdbc_report
    suite = root.find("testsuite")
    if empty:
        suite.clear()
    else:
        suite.remove(suite[-1])
    ET.ElementTree(root).write(path)
    with pytest.raises(ValueError, match="Expected at least"):
        check_results(path)


@pytest.mark.parametrize("vendor", sorted(VENDOR_SMOKE_TESTS))
def test_jdbc_report_requires_each_vendor(jdbc_report, vendor):
    root, path = jdbc_report
    for case in root.findall(".//testcase"):
        if case.get("name") == vendor:
            case.set("name", "test_something_else")
    ET.ElementTree(root).write(path)
    with pytest.raises(ValueError, match="Missing JDBC vendor"):
        check_results(path)


def test_jdbc_report_rejects_other_suite(jdbc_report):
    root, path = jdbc_report
    for case in root.findall(".//testcase"):
        case.set("classname", "tests.integ.datasource.test_oracledb")
    ET.ElementTree(root).write(path)
    with pytest.raises(ValueError, match="Expected at least"):
        check_results(path)


def test_jdbc_report_missing_or_malformed(tmp_path):
    path = tmp_path / "jdbc.xml"
    with pytest.raises(FileNotFoundError):
        check_results(path)
    path.write_text("not XML")
    with pytest.raises(ET.ParseError):
        check_results(path)


def test_jdbc_workflow_contract():
    root = Path(__file__).resolve().parents[2]
    # BaseLoader preserves the YAML 1.2 workflow key 'on' as a string.
    workflow = yaml.load(
        (root / ".github/workflows/jdbc.yml").read_text(), Loader=yaml.BaseLoader
    )
    assert set(workflow["on"]) == {"workflow_dispatch", "schedule"}
    assert workflow["on"]["schedule"] == [{"cron": "17 9 * * *"}]
    job = workflow["jobs"]["jdbc"]
    assert int(job["timeout-minutes"]) <= 30
    assert job["if"] == "github.repository == 'snowflakedb/snowpark-python'"
    assert job["env"]["cloud_provider"] == "aws"
    assert workflow["concurrency"]["cancel-in-progress"] == "false"
    assert any(
        step.get("run") == "python -m tox -e jdbc --skip-missing-interpreters false"
        for step in job["steps"]
    )
    checkout = next(
        step for step in job["steps"] if "actions/checkout@" in step.get("uses", "")
    )
    assert checkout["with"]["persist-credentials"] == "false"
    decrypt = next(
        step for step in job["steps"] if "PARAMETER_PASSWORD" in step.get("env", {})
    )
    assert decrypt["env"]["PARAMETER_PASSWORD"] == "${{ secrets.PARAMETER_PASSWORD }}"
    assert "set -euo pipefail" in decrypt["run"]
    assert "umask 077" in decrypt["run"]
    for encrypted in (
        "parameters/parameters_aws.py.gpg",
        "parameters/parameters_dbapi.py.gpg",
        "parameters/rsa_keys/rsa_key_aws.p8.gpg",
    ):
        assert f".github/workflows/{encrypted}" in decrypt["run"]
        assert (root / ".github/workflows" / encrypted).is_file()
    upload = next(
        step
        for step in job["steps"]
        if "actions/upload-artifact@" in step.get("uses", "")
    )
    assert upload["if"] == "always()"
    assert upload["with"]["path"] == ".tox/junit.jdbc.xml"
    cleanup = job["steps"][-1]
    assert cleanup["if"] == "always()"
    assert cleanup["run"] == "rm -f tests/parameters.py tests/rsa_key_aws.p8"
    assert workflow["permissions"] == {"contents": "read"}
    config = configparser.ConfigParser(interpolation=None)
    config.read(root / "tox.ini")
    jdbc = config["testenv:jdbc"]
    assert "tests/integ/datasource/test_jdbc.py" in jdbc["commands"]
    assert "-n 0" in jdbc["commands"]
    assert "--maxfail=1" in jdbc["commands"]
    assert "scripts/check_jdbc_test_results.py" in jdbc["commands"]
    assert "unlink(missing_ok=True)" in jdbc["commands_pre"]
    assert "{posargs" not in jdbc["commands"]
    assert "PYTEST_ADDOPTS =" in jdbc["setenv"]
    assert "--timeout=180" in jdbc["commands"]
    assert "-o junit_logging=no" in jdbc["commands"]


@pytest.mark.parametrize("name", [None, "test_basic_jdbc"])
def test_jdbc_report_rejects_duplicate_or_unnamed_case(jdbc_report, name):
    root, path = jdbc_report
    case = root.find("testsuite")[-1]
    if name is None:
        del case.attrib["name"]
    else:
        case.set("name", name)
    ET.ElementTree(root).write(path)
    with pytest.raises(ValueError, match="duplicate or unnamed"):
        check_results(path)


def test_jdbc_report_accepts_additional_tests(jdbc_report):
    root, path = jdbc_report
    ET.SubElement(
        root.find("testsuite"),
        "testcase",
        classname="tests.integ.datasource.test_jdbc",
        name="test_new_jdbc_regression",
    )
    ET.ElementTree(root).write(path)
    check_results(path)


@pytest.mark.parametrize("skipped", [True, False])
def test_jdbc_report_cli_exit_status(jdbc_report, skipped):
    root, path = jdbc_report
    if skipped:
        ET.SubElement(root.find(".//testcase"), "skipped")
    ET.ElementTree(root).write(path)
    script = Path(__file__).resolve().parents[2] / "scripts/check_jdbc_test_results.py"
    result = subprocess.run(
        [sys.executable, str(script), str(path)], capture_output=True, text=True
    )
    assert result.returncode == (1 if skipped else 0)
    if skipped:
        assert "skipped, failed or errored" in result.stderr
