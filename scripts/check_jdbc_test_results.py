"""Require the JDBC baseline and all four vendors with no skipped or failed tests."""

import argparse
import xml.etree.ElementTree as ET


# The GA stack collects 48 cases. New cases may increase this baseline.
MINIMUM_TESTS = 48
VENDOR_SMOKE_TESTS = {
    "test_basic_jdbc",
    "test_connect_postgres",
    "test_connect_mysql",
    "test_connect_sql_server",
}


def check_results(path):
    root = ET.parse(path).getroot()
    cases = root.findall(".//testcase")
    jdbc_cases = [
        case
        for case in cases
        if case.get("classname") == "tests.integ.datasource.test_jdbc"
    ]
    names = {case.get("name") for case in jdbc_cases}
    if len(names) != len(jdbc_cases) or None in names:
        raise ValueError("JDBC results contain duplicate or unnamed tests")
    if len(jdbc_cases) < MINIMUM_TESTS:
        raise ValueError(f"Expected at least {MINIMUM_TESTS} JDBC test results")
    if not VENDOR_SMOKE_TESTS.issubset(names):
        raise ValueError("Missing JDBC vendor smoke test results")
    if any(
        case.find(tag) is not None
        for case in cases
        for tag in ("skipped", "failure", "error")
    ):
        raise ValueError("JDBC run contains skipped, failed or errored tests")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report")
    args = parser.parse_args()
    check_results(args.report)
