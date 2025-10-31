#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
def test_series_to_json():
    native_series = native_pd.Series([1, 2, None, 4], index=["a", "b", "c", "d"])
    snow_series = pd.Series(native_series)
    native_result = native_series.to_json()
    snow_result = snow_series.to_json()
    assert isinstance(snow_result, str)
    assert native_result == snow_result


@sql_count_checker(query_count=1)
def test_dataframe_to_json():
    native_df = native_pd.DataFrame(
        {
            "col1": [1, 2],
            "col2": [0.5, None],
            "col3": native_pd.timedelta_range("1 hour", periods=2),
        },
        index=["a", "b'"],
    )
    snow_df = pd.DataFrame(native_df)
    native_result = native_df.to_json()
    snow_result = snow_df.to_json()
    assert isinstance(snow_result, str)
    assert native_result == snow_result
