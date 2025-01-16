#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.index.conftest import NATIVE_INDEX_UNIQUE_TEST_DATA
from tests.integ.modin.utils import assert_index_equal
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize("native_index", NATIVE_INDEX_UNIQUE_TEST_DATA)
@sql_count_checker(query_count=1)
def test_index_unique(native_index):
    snow_index = pd.Index(native_index)
    assert_index_equal(snow_index.unique(), native_index.unique())


@sql_count_checker(query_count=2)
def test_index_unique_nan_vs_none_negative():
    native_index = native_pd.Index(["a", "b", 1, 2, np.nan, None, "a", 2])
    snow_index = pd.Index(native_index)
    # Snowflake treats np.nan and None as the same thing, but pandas does not.
    with pytest.raises(AssertionError):
        assert_index_equal(snow_index.unique(), native_index.unique())
    # Try again with np.nan changed to None.
    native_index = native_pd.Index(["a", "b", 1, 2, None, None, "a", 2])
    assert_index_equal(snow_index.unique(), native_index.unique())


@pytest.mark.parametrize("native_index", NATIVE_INDEX_UNIQUE_TEST_DATA)
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_index_nunique(native_index, dropna):
    snow_index = pd.Index(native_index)
    assert snow_index.nunique(dropna=dropna) == native_index.nunique(dropna=dropna)


@sql_count_checker(query_count=4)
def test_index_unique_data_columns_should_not_affect_index_column():
    native_df = native_pd.DataFrame(
        {
            "B": [10, 20, 30, 40, 50],
            "A": [11, 22, 33, 44, 55],
            "C": [60, 70, 80, 90, 100],
        },
        index=native_pd.Index([5, 4, 3, 2, 1], name="A"),
    )
    snow_df = pd.DataFrame(native_df)

    # The index column and data columns in a DataFrame object can have
    # the same column names. We need to ensure that the correct column is
    # picked during access to df.index and df.col_name, and the results
    # for df.col_name.unique() and df.index.unique() are different.
    # In this test, both the index and a data column have the name "A".
    # Check that they produce different results with unique.
    # 2 queries to perform snow_df.A.unique(), 1 query for snow_df.index.unique()
    with pytest.raises(AssertionError):
        assert_index_equal(snow_df.index.unique(), snow_df.A.unique())

    # Verify that Index.unique is working as expected. 1 query for snow_df.index.unique().
    assert_index_equal(snow_df.index.unique(), native_df.index.unique())


@sql_count_checker(query_count=0)
def test_index_unique_level_negative():
    snow_index = pd.Index(["a", "b", 1, 2, np.nan, None, "a", 2])
    with pytest.raises(
        IndexError,
        match="Too many levels: Index has only 1 level, 2 is not a valid level number.",
    ):
        snow_index.unique(level=2)
