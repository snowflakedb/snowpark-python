#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker

NATIVE_INDEX_BOOL_INT_TEST_DATA = [
    native_pd.Index([True, True, True, True, False, False], dtype=bool, name="name"),
    native_pd.Index([0, 1, 2, 3, 4], dtype=np.int64),
    native_pd.Index([True, True, True, None, False, False], dtype=bool),
]

NATIVE_INDEX_NON_BOOL_INT_TEST_DATA = [
    native_pd.Index([[1, 2], [2, 3], [3, 4]]),
    native_pd.Index([3, np.nan, 5], name="my_index"),
    native_pd.Index([1.1, np.nan]),
    native_pd.Index([5, None, 7]),
    native_pd.Index(["a", "b", 1, 2]),
    native_pd.Index(["a", "b", "c", "d"]),
    native_pd.Index([5, None, 7]),
    native_pd.Index([], dtype="object"),
    native_pd.Index([pd.Timedelta(0), None]),
    native_pd.Index([pd.Timedelta(0)]),
    native_pd.Index([pd.Timedelta(0), pd.Timedelta(1)]),
]

NATIVE_INDEX_EMPTY_DATA = [
    native_pd.Index([], dtype=bool),
    native_pd.Index([], dtype=int, name="NM"),
]


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("func", ["all", "any"])
@pytest.mark.parametrize("native_index", NATIVE_INDEX_BOOL_INT_TEST_DATA)
def test_index_all_any(func, native_index):
    snow_index = pd.Index(native_index)
    assert getattr(snow_index, func)() == getattr(native_index, func)()


@pytest.mark.parametrize("func", ["all", "any"])
@pytest.mark.parametrize("native_index", NATIVE_INDEX_NON_BOOL_INT_TEST_DATA)
@sql_count_checker(query_count=0)
def test_index_all_any_negative(func, native_index):
    snow_index = pd.Index(native_index)
    with pytest.raises(
        NotImplementedError,
        match=f"Snowpark pandas {func} API doesn't yet support non-integer/boolean columns",
    ):
        assert getattr(snow_index, func)() == getattr(native_index, func)()


@pytest.mark.parametrize("func", ["all", "any"])
@pytest.mark.parametrize("native_index", NATIVE_INDEX_EMPTY_DATA)
@sql_count_checker(query_count=1)
def test_index_all_any_empty(func, native_index):
    snow_index = pd.Index(native_index)
    assert getattr(snow_index, func)() == getattr(native_index, func)()
