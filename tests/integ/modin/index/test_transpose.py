#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker

NATIVE_INDEX_TEST_DATA = [
    native_pd.Index([], dtype="object"),
    native_pd.Index([1, 2, 3]),
    native_pd.Index([["a", "b"], [1, 2]]),
]


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("native_index", NATIVE_INDEX_TEST_DATA)
def test_index_transpose(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index.equals(snow_index.T)
