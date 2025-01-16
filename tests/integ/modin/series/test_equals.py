#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


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
        ([1, 2, None], [1, 2, None], True),  # nulls are considered equal
        ([1, 2, 3], [1.0, 2.0, 3.0], False),  # float and integer types are not equal
        ([1, 2, 3], ["1", "2", "3"], False),  # integer and string types are not equal
        ([1, 2, 3], [1, 3, 2], False),  # different order
    ],
)
@sql_count_checker(query_count=2, join_count=2)
def test_equals_series(lhs, rhs, expected):
    s1 = pandas.Series(lhs)
    s2 = pandas.Series(rhs)
    assert s1.equals(s2) == pd.Series(s1).equals(pd.Series(s2))


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
    s1 = pandas.Series([1, 3]).astype(ltype)
    s2 = pandas.Series([1, 3]).astype(rtype)
    # Native pandas should return False
    assert s1.equals(s2) is False

    s1 = pd.Series([1, 3]).astype(ltype)
    s2 = pd.Series([1, 3]).astype(rtype)
    # Snowpark pandas should return True for variants of same type.
    assert s1.equals(s2) == expected


@sql_count_checker(query_count=2, join_count=2)
def test_equals_with_native_object():
    s1 = pd.Series([1, 3])
    s2 = pandas.Series([1, 3])
    # Native pandas should return False
    assert s1.equals(s2)


@pytest.mark.parametrize(
    "lhs, rhs",
    [
        ([0, 1, 2], [0, 1, 3]),  # different value
        ([0, 1, 2], [0, 2, 1]),  # different order
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_equals_index_mismatch(lhs, rhs):
    s1 = pandas.Series([1, 2, 3], index=lhs)
    s2 = pandas.Series([1, 2, 3], index=rhs)
    assert s1.equals(s2) == pd.Series(s1).equals(pd.Series(s2))
