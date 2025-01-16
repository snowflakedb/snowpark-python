#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "keep, expected",
    [
        ("first", native_pd.Series([False, False, True, False, True], name="name")),
        ("last", native_pd.Series([True, True, False, False, False], name="name")),
        (False, native_pd.Series([True, True, True, False, True], name="name")),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_duplicated_keep(keep, expected):
    ser = pd.Series(["a", "b", "b", "c", "a"], name="name")
    result = ser.duplicated(keep=keep)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@pytest.mark.parametrize(
    "keep, expected",
    [
        ("first", native_pd.Series([False, False, True, True, True])),
        ("last", native_pd.Series([True, True, False, True, False])),
        (False, native_pd.Series([True, True, True, True, True])),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_duplicated_nan_none(keep, expected):
    # Note that Snowpark pandas treats np.nan and None the same
    ser = pd.Series([np.nan, 3, 3, None, np.nan], dtype=object)

    result = ser.duplicated(keep=keep)
    assert_snowpark_pandas_equal_to_pandas(result, expected)
