#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.index.conftest import NATIVE_INDEX_UNIQUE_TEST_DATA
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import assert_index_equal


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

    with pytest.raises(AssertionError):
        # Assert that even though the index column and a data column have
        # the same name "A", the correct column is picked for both cases and
        # different results are produced.
        snow_index = snow_df.index
        snow_df_a = snow_df.A
        with SqlCounter(query_count=1):
            assert_index_equal(snow_index.unique(), snow_df_a.unique())

    with SqlCounter(query_count=1):
        assert_index_equal(snow_df.index.unique(), native_df.index.unique())
    # TODO: SNOW-1524901: snow_df.A.unique() does not produce the correct result here.
    #  Right now an empty result is returned: []. This is because the index name and
    #  the column name are the same here.
    # assert snow_df.A.unique() == native_df.A.unique()
