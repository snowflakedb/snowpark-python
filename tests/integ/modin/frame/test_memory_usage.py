#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_memory_usage():
    assert pd.DataFrame([1]).memory_usage()[0] == 0
    assert pd.Series([1]).memory_usage() == 0
    df = pd.DataFrame({"A": [1, 2, 3], "B": ["x", "y", "z"]})
    df.index.name = "My Index"
    assert df.memory_usage().size == 3
    assert df.memory_usage().index[0] == "Index"
    assert df.memory_usage(index=False).size == 2
