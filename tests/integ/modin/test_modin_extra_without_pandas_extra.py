#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
def test_simple_to_pandas():
    pd.DataFrame([1]).to_pandas()
