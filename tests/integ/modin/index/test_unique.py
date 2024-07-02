#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.index.conftest import NATIVE_INDEX_UNIQUE_TEST_DATA
from tests.integ.modin.sql_counter import sql_count_checker
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
@sql_count_checker(query_count=1)
def test_index_nunique(native_index):
    snow_index = pd.Index(native_index)
    assert snow_index.nunique() == native_index.nunique()
