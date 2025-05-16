#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "sample, expected_len",
    [
        ([], 0),
        ([1, 2], 2),
        ([1, 2, None], 3),
    ],
)
@sql_count_checker(query_count=0)
def test_len(sample, expected_len):
    snow = pd.Series(sample)
    native = native_pd.Series(sample)
    assert len(snow) == expected_len
    assert len(snow) == len(native)
