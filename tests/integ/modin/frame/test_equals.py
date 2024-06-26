#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "lhs, rhs, expected",
    [
        ([1, 2, 3], [1, 2, 3], True),
        ([1, 2, 3], [1, 2, 4], False),  # different values
        ([1, 2, None], [1, 2, None], True),  # nulls are considered equal
        ([1, 2, 3], [1.0, 2.0, 3.0], False),  # float and integer types are not equal
        ([1, 2, 3], ["1", "2", "3"], False),  # integer and string types are not equal
    ],
)
@sql_count_checker(query_count=7, join_count=3)
def test_equals(lhs, rhs, expected):
    df1 = pd.DataFrame({"A": lhs})
    df2 = pd.DataFrame({"A": rhs})
    assert df1.equals(df2) == expected
    assert df1.to_pandas().equals(df2.to_pandas()) == expected


@pytest.mark.parametrize(
    "lhs, rhs, expected",
    [
        ([1, 2], [1, 2], True),
        ([1, 2], [3, 4], False),  # different label values
        (["1", None], ["1", None], True),  # nulls are considered equal
        ([1, 2], [1.0, 2.0], True),  # float and integer types are equal
        ([1, 2], ["1", "2"], False),  # integer and string types are not equal
    ],
)
def test_equals_column_labels(lhs, rhs, expected):
    with SqlCounter(query_count=7 if expected else 4, join_count=3 if expected else 0):
        df1 = pd.DataFrame([[1, 2]], columns=lhs)
        df2 = pd.DataFrame([[1, 2]], columns=rhs)
        assert df1.equals(df2) == expected
        assert df1.to_pandas().equals(df2.to_pandas()) == expected
