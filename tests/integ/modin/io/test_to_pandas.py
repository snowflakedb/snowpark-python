#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=3)
def test_pd_to_pandas():
    data = {
        "a": [1, 2, 3],
        "b": ["x", "y", "z"],
        "c": native_pd.timedelta_range("2 days", periods=3),
    }
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
    assert_series_equal(
        pd.to_pandas(pd.Series(data["c"])),
        native_pd.Series(data["c"]),
    )


@sql_count_checker(query_count=2)
def test_to_pandas_with_attrs():
    df = pd.DataFrame([[1, 2]])
    df.attrs = {"k": "v"}
    assert df.to_pandas().attrs == df.attrs
    s = pd.Series([1])
    s.attrs = {"k": "v"}
    assert s.to_pandas().attrs == s.attrs
