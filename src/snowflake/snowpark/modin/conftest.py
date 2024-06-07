#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd  # pragma: no cover
import numpy as np  # pragma: no cover
import pytest  # pragma: no cover

import snowflake.snowpark.modin.plugin as plugin


@pytest.fixture(autouse=True, scope="module")  # pragma: no cover
def add_doctest_imports(doctest_namespace) -> None:  # pragma: no cover
    """
    Make `np` and `pd` names available for doctests.
    """
    doctest_namespace["np"] = np  # pragma: no cover
    doctest_namespace["pd"] = pd  # pragma: no cover
    doctest_namespace["plugin"] = plugin  # pragma: no cover
