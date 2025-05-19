#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2)
def test_to_string():
    native_df = native_pd.DataFrame(data={"col1": [1, 2], "col2": [4, 3]})
    snow_df = pd.DataFrame(native_df)
    assert snow_df.to_string() == native_df.to_string()
