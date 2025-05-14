#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2)
def test_to_string():
    native_ser = native_pd.Series([-1, 5, 6, 2, 4])
    snow_ser = pd.Series(native_ser)
    assert snow_ser.to_string() == native_ser.to_string()
