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
        ({"a": []}, 0),
        ({"a": [1, 2]}, 2),
        ({"a": [1, 2], "b": [1, 2], "c": [1, 2]}, 2),
        ({"td": native_pd.timedelta_range(1, periods=20)}, 20),
    ],
)
# Frames constructed from literal objects cache their sizes, so no queries are necessary.
@sql_count_checker(query_count=0)
def test_len(sample, expected_len):
    snow = pd.DataFrame(sample)
    native = native_pd.DataFrame(sample)
    assert len(snow) == expected_len
    assert len(snow) == len(native)
