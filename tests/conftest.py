#!/usr/bin/env python
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import logging
import os
from pathlib import Path

import pytest

from snowflake.snowpark._internal.utils import warning_dict

logging.getLogger("snowflake.connector").setLevel(logging.ERROR)

excluded_frontend_files = [
    "accessor.py",
]


def is_excluded_frontend_file(path):
    for excluded in excluded_frontend_files:
        if str(path).endswith(excluded):
            return True
    return False


def pytest_addoption(parser):
    parser.addoption("--disable_sql_simplifier", action="store_true", default=False)
    parser.addoption("--local_testing_mode", action="store_true", default=False)
    parser.addoption("--enable_cte_optimization", action="store_true", default=False)


def pytest_collection_modifyitems(items) -> None:
    """Applies tags to tests based on folders that they are in."""
    top_test_dir = Path(__file__).parent
    top_doctest_dir = top_test_dir.parent.joinpath("src/snowflake/snowpark")
    for item in items:
        item_path = Path(str(item.fspath)).parent
        try:
            relative_path = item_path.relative_to(top_test_dir)
            for part in relative_path.parts:
                item.add_marker(part)
        except ValueError as e:
            # item_path.relative_to(top_test_dir) will throw an error if
            # the path isn't in the tests dir. We also accept doctest files
            # in src/snowflake/snowpark (and set a marker for them) but
            # we raise an exception for all other dirs that are passed in
            if item_path == top_doctest_dir:
                item.add_marker("doctest")
            elif "modin" in str(item_path):
                if not is_excluded_frontend_file(item.fspath):
                    item.add_marker("doctest")
                    item.add_marker(pytest.mark.usefixtures("add_doctest_imports"))
            else:
                raise e


@pytest.fixture(scope="session")
def sql_simplifier_enabled(pytestconfig):
    disable_sql_simplifier = pytestconfig.getoption("disable_sql_simplifier")
    return not disable_sql_simplifier


@pytest.fixture(scope="session")
def local_testing_mode(pytestconfig):
    return pytestconfig.getoption("local_testing_mode")


@pytest.fixture(scope="function")
def local_testing_telemetry_setup():
    # the import here is because we want LocalTestOOBTelemetryService to be initialized
    # after pytest_sessionstart is setup so that it can detect os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"]
    # and set internal usage to be true
    from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService

    LocalTestOOBTelemetryService.get_instance().enable()
    LocalTestOOBTelemetryService.get_instance()._is_internal_usage = True
    yield
    LocalTestOOBTelemetryService.get_instance().disable()


@pytest.fixture(scope="session")
def cte_optimization_enabled(pytestconfig):
    return pytestconfig.getoption("enable_cte_optimization")


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"


@pytest.fixture(autouse=True)
def clear_warning_dict():
    yield
    # clear the warning dict so that warnings from one test don't affect
    # warnings from other tests.
    warning_dict.clear()


def attr_to_dict(span):
    dict_attr = {}
    for name in span.attributes:
        dict_attr[name] = span.attributes[name]
    dict_attr["code.filepath"] = os.path.basename(dict_attr["code.filepath"])
    dict_attr["status_code"] = span.status.status_code
    dict_attr["status_description"] = span.status.description
    return dict_attr


def span_extractor(dict_exporter):
    spans = []
    raw_spans = dict_exporter.get_finished_spans()
    for span in raw_spans:
        spans.append((span.name, attr_to_dict(span), span))
    return spans


def check_single_answer(result, expected_answer):
    for answer_name in expected_answer:
        if answer_name == "status_description":
            if expected_answer[answer_name] not in result[answer_name]:
                return False
            else:
                continue
        if expected_answer[answer_name] != result[answer_name]:
            return False
    return True


def check_answers(results, expected_answer):
    results = span_extractor(results)
    for result in results:
        if expected_answer[0] == result[0]:
            if check_single_answer(result[1], expected_answer[1]):
                return True
    return False
