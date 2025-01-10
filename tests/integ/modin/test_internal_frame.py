#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
)
from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import multithreaded_run


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            native_pd.DataFrame({"A": [0, 1, 0, 1, 2], "B": [1, 2, 3, 4, 5]}),
            native_pd.DataFrame({"A": [0, 1, 2], "B": [3, 4, 5]}, index=[2, 3, 4]),
        ),
        (
            native_pd.DataFrame({"A": [0, 0, 0, 0], "B": [1, 2, 3, 4]}),
            native_pd.DataFrame({"A": [0], "B": [4]}, index=[3]),
        ),
    ],
)
@sql_count_checker(query_count=2, join_count=1)
def test_strip_duplicates(input, expected):
    snow_df = pd.DataFrame(input)
    internal_frame: InternalFrame = snow_df._query_compiler._modin_frame
    internal_frame = internal_frame.strip_duplicates(
        internal_frame.data_column_snowflake_quoted_identifiers[:1]
    )
    result = pd.DataFrame(query_compiler=SnowflakeQueryCompiler(internal_frame))
    assert_frame_equal(result, pd.DataFrame(expected))


@multithreaded_run()
@sql_count_checker(query_count=2, join_count=1)
def test_strip_duplicates_after_sort():
    df = pd.DataFrame({"A": [0, 1, 0, 1, 2], "B": [1, 2, 3, 4, 5]})
    df = df.sort_values(by="B", ascending=False)
    internal_frame: InternalFrame = df._query_compiler._modin_frame
    internal_frame = internal_frame.strip_duplicates(
        internal_frame.data_column_snowflake_quoted_identifiers[:1]
    )
    result = pd.DataFrame(query_compiler=SnowflakeQueryCompiler(internal_frame))
    expected = pd.DataFrame({"A": [2, 1, 0], "B": [5, 2, 1]}, index=[4, 1, 0])
    assert_frame_equal(result, expected)
