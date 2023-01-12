#!/usr/bin/env python
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import logging
from pathlib import Path

import pytest

logging.getLogger("snowflake.connector").setLevel(logging.ERROR)


def pytest_addoption(parser):
    parser.addoption("--disable_sql_simplifier", action="store_true", default=False)


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
            else:
                raise e


@pytest.fixture(scope="session")
def sql_simplifier_enabled(pytestconfig):
    disable_sql_simplifier = pytestconfig.getoption("disable_sql_simplifier")
    return not disable_sql_simplifier
