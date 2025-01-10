#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "lhs, rhs, expected",
    [
        ([1, 2, 3], [1, 2, 3], True),
        pytest.param(
            [pd.Timedelta(1), pd.Timedelta(2), pd.Timedelta(3)],
            [pd.Timedelta(1), pd.Timedelta(2), pd.Timedelta(3)],
            True,
            id="timedelta",
        ),
        ([1, 2, 3], [1, 2, 4], False),  # different values
        ([1, 2, None], [1, 2, None], True),  # nulls are considered equal
        ([1, 2, 3], [1.0, 2.0, 3.0], False),  # float and integer types are not equal
        ([1, 2, 3], ["1", "2", "3"], False),  # integer and string types are not equal
        (
            [1, 2, 3],
            pandas.timedelta_range(1, periods=3),
            False,  # timedelta and integer types are not equal
        ),
    ],
)
@sql_count_checker(query_count=2, join_count=2)
# High query count coming from to_pandas calls in Index.equals(other) and count queries
# in SnowflakeQueryCompiler.all.
def test_equals(lhs, rhs, expected):
    df1 = pandas.DataFrame({"A": lhs})
    df2 = pandas.DataFrame({"A": rhs})
    assert df1.equals(df2) == expected
    assert pd.DataFrame(df1).equals(pd.DataFrame(df2)) == expected


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
    with SqlCounter(query_count=2 if expected else 1, join_count=2 if expected else 1):
        df1 = pandas.DataFrame([[1, 2]], columns=lhs)
        df2 = pandas.DataFrame([[1, 2]], columns=rhs)
        assert df1.equals(df2) == expected
        assert pd.DataFrame(df1).equals(pd.DataFrame(df2)) == expected


@pytest.mark.parametrize(
    "ltype, rtype, expected",
    [
        (np.int8, np.int16, True),
        (np.int32, np.int16, True),
        (np.float16, np.float32, True),
        (np.float64, np.float32, True),
        (np.int16, "object", False),
        (np.int16, np.float16, False),
        ("timedelta64[ns]", int, False),
        ("timedelta64[ns]", float, False),
    ],
)
@sql_count_checker(query_count=2, join_count=2)
def test_equals_numeric_variants(ltype, rtype, expected):
    df1 = pandas.DataFrame([1, 3], columns=["col1"]).astype(ltype)
    df2 = pandas.DataFrame([1, 3], columns=["col1"]).astype(rtype)
    # Native pandas should return False
    assert df1.equals(df2) is False

    df1 = pd.DataFrame([1, 3], columns=["col1"]).astype(ltype)
    df2 = pd.DataFrame([1, 3], columns=["col1"]).astype(rtype)
    # Snowpark pandas should return True for variants of same type.
    assert pd.DataFrame(df1).equals(pd.DataFrame(df2)) == expected
