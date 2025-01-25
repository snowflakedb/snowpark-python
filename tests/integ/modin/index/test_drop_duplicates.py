#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_index_equal
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("keep", ["first", "last", False])
@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates(keep):
    pandas_idx = native_pd.Index(["a", "b", "b", "c", "a"], name="name")
    snow_idx = pd.Index(pandas_idx)

    assert_index_equal(
        snow_idx.drop_duplicates(keep=keep),
        pandas_idx.drop_duplicates(keep=keep),
    )


@pytest.mark.parametrize("keep", ["first", "last", False])
@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates_on_empty_index(keep):
    pandas_idx = native_pd.Index([], name="name")
    snow_idx = pd.Index(pandas_idx)

    assert_index_equal(
        snow_idx.drop_duplicates(keep=keep),
        pandas_idx.drop_duplicates(keep=keep),
    )


@pytest.mark.parametrize(
    "keep, expected",
    [
        ("first", native_pd.Index([np.nan, 3])),
        ("last", native_pd.Index([3, np.nan])),
        (False, native_pd.Index([], dtype="float64")),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates_nan_none(keep, expected):
    # Note that Snowpark pandas treats np.nan and None the same
    idx = pd.Index([np.nan, 3, 3, None, np.nan], dtype=object)

    result = idx.drop_duplicates(keep=keep)
    assert_index_equal(
        result,
        expected,
    )


@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates_default_keep():
    pandas_idx = native_pd.Index([], name="name")
    snow_idx = pd.Index(pandas_idx)

    assert_index_equal(
        snow_idx.drop_duplicates(),
        pandas_idx.drop_duplicates(),
    )


@sql_count_checker(query_count=0, join_count=0)
def test_drop_duplicates_invalid_keep():
    snow_idx = pd.Index(["a", "b", "b", "c", "a"], name="name")
    with pytest.raises(
        ValueError, match='keep must be either "first", "last" or False'
    ):
        snow_idx.drop_duplicates(keep="invalid")
