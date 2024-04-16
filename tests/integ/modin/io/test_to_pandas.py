#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_frame_equal, assert_series_equal


@sql_count_checker(query_count=2)
def test_pd_to_pandas():
    data = {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    assert_frame_equal(
        pd.to_pandas(pd.DataFrame(data)),
        native_pd.DataFrame(data),
        check_index_type=False,
        check_dtype=False,
    )
    assert_series_equal(
        pd.to_pandas(pd.Series(data["a"])),
        native_pd.Series(data["a"]),
        check_index_type=False,
        check_dtype=False,
    )
