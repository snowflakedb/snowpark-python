#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "lhs, rhs, expected",
    [
        ([1, 2, 3], [1, 2, 3], True),
        ([1, 2, None], [1, 2, None], True),  # nulls are considered equal
        ([1, 2, 3], [1.0, 2.0, 3.0], False),  # float and integer types are not equal
        ([1, 2, 3], ["1", "2", "3"], False),  # integer and string types are not equal
    ],
)
@sql_count_checker(query_count=7, join_count=3)
def test_equals_dataframe(lhs, rhs, expected):
    df1 = pd.DataFrame({"A": lhs})
    df2 = pd.DataFrame({"A": rhs})
    assert df1.equals(df2) == df1.to_pandas().equals(df2.to_pandas())


@pytest.mark.parametrize(
    "lhs, rhs, expected",
    [
        ([1, 2, 3], [1, 2, 3], True),
        ([1, 2, None], [1, 2, None], True),  # nulls are considered equal
        ([1, 2, 3], [1.0, 2.0, 3.0], False),  # float and integer types are not equal
        ([1, 2, 3], ["1", "2", "3"], False),  # integer and string types are not equal
    ],
)
@sql_count_checker(query_count=6, join_count=2)
def test_equals_series(lhs, rhs, expected):
    s1 = pd.Series(lhs)
    s2 = pd.Series(rhs)
    assert s1.equals(s2) == s1.to_pandas().equals(s2.to_pandas())
