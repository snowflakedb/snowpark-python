#!/usr/bin/env python
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from snowflake.snowpark._internal.utils import warning_dict
from .ast.conftest import default_unparser_path

logging.getLogger("snowflake.connector").setLevel(logging.ERROR)

excluded_frontend_files = [
    "accessor.py",
]

# Modin breaks Python 3.8 compatibility, do not test when running under 3.8.
COMPATIBLE_WITH_MODIN = sys.version_info.minor > 8


# the fixture only works when opentelemetry is installed
try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import SERVICE_NAME, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    @pytest.fixture(scope="session")
    def dict_exporter():
        resource = Resource(attributes={SERVICE_NAME: "snowpark-python-open-telemetry"})
        trace_provider = TracerProvider(resource=resource)
        dict_exporter = InMemorySpanExporter()
        processor = SimpleSpanProcessor(dict_exporter)
        trace_provider.add_span_processor(processor)
        trace.set_tracer_provider(trace_provider)
        yield dict_exporter

    opentelemetry_installed = True

except ModuleNotFoundError:
    opentelemetry_installed = False


def is_excluded_frontend_file(path):
    for excluded in excluded_frontend_files:
        if str(path).endswith(excluded):
            return True
    return False


def pytest_addoption(parser, pluginmanager):
    parser.addoption("--disable_sql_simplifier", action="store_true", default=False)
    parser.addoption("--disable_cte_optimization", action="store_true", default=False)
    parser.addoption("--skip_sql_count_check", action="store_true", default=False)
    if not any(
        "--local_testing_mode" in opt.names() for opt in parser._anonymous.options
    ):
        parser.addoption("--local_testing_mode", action="store_true", default=False)
    parser.addoption("--enable_ast", action="store_true", default=False)
    parser.addoption("--dataframe_processor_pkg_version", action="store", default=None)
    parser.addoption("--dataframe_processor_location", action="store", default=None)
    parser.addoption("--validate_ast", action="store_true", default=False)
    parser.addoption(
        "--unparser_jar",
        action="store",
        default=default_unparser_path(),
        type=str,
        help="Path to the Unparser JAR built in the monorepo.",
    )
    parser.addoption("--join_alias_fix", action="store_true", default=True)


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
                if not COMPATIBLE_WITH_MODIN:
                    pass
                elif not is_excluded_frontend_file(item.fspath):
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
    # SNOW-1845413 we have a default value here unlike other options to bypass test in stored procedure
    return pytestconfig.getoption("local_testing_mode", False)


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
def ast_enabled(pytestconfig):
    return pytestconfig.getoption("enable_ast")


@pytest.fixture(scope="session")
def dataframe_processor_pkg_version(pytestconfig):
    return pytestconfig.getoption("dataframe_processor_pkg_version")


@pytest.fixture(scope="session")
def dataframe_processor_location(pytestconfig):
    return pytestconfig.getoption("dataframe_processor_location")


@pytest.fixture(scope="session")
def validate_ast(pytestconfig):
    return pytestconfig.getoption("validate_ast")


@pytest.fixture(scope="session")
def cte_optimization_enabled(pytestconfig):
    return not pytestconfig.getoption("disable_cte_optimization")


@pytest.fixture(scope="session")
def join_alias_fix(pytestconfig):
    return pytestconfig.getoption("join_alias_fix")


@pytest.fixture(scope="module", autouse=True)
def proto_generated():
    """Generate Protobuf Python files automatically"""
    try:
        from snowflake.snowpark._internal.proto.generated import ast_pb2  # noqa: F401
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "tox", "-e", "protoc"])


@pytest.fixture(scope="session")
def unparser_jar(pytestconfig):
    unparser_jar = pytestconfig.getoption("--unparser_jar")
    if unparser_jar is not None and not os.path.exists(unparser_jar):
        unparser_jar = None

    if unparser_jar is None and pytestconfig.getoption("--validate_ast"):
        raise RuntimeError(
            f"Unparser JAR not found at {unparser_jar}. "
            f"Please set the correct path with --unparser_jar or SNOWPARK_UNPARSER_JAR."
        )
    return unparser_jar


def pytest_sessionstart(session):
    os.environ["SNOWPARK_LOCAL_TESTING_INTERNAL_TELEMETRY"] = "1"


SKIP_SQL_COUNT_CHECK = False


@pytest.fixture(scope="session", autouse=True)
def setup_skip_sql_count_check(pytestconfig):
    skip = pytestconfig.getoption("skip_sql_count_check")
    if skip:
        global SKIP_SQL_COUNT_CHECK
        SKIP_SQL_COUNT_CHECK = True


@pytest.fixture(autouse=True)
def clear_warning_dict():
    yield
    # clear the warning dict so that warnings from one test don't affect
    # warnings from other tests.
    warning_dict.clear()
