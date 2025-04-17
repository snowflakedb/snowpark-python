#!/usr/bin/env python3

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.functions import to_timestamp  # pragma: no cover
import pytest  # pragma: no cover


@pytest.fixture(autouse=True, scope="module")  # pragma: no cover
def add_doctest_imports(doctest_namespace) -> None:  # pragma: no cover
    """
    Make `to_timestamp` name available for doctests.
    """
    doctest_namespace["to_timestamp"] = to_timestamp  # pragma: no cover
