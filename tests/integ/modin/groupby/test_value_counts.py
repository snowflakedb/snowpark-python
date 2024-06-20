#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result

TEST_DATA = [
    {
        "by": ["c", "b", "a", "a", "b", "b", "c", "a"],
        "value1": ["ee", "aa", "bb", "aa", "bb", "cc", "dd", "aa"],
        "value2": [1, 2, 3, 1, 1, 3, 2, 1],
    },
    {
        "by": ["key 1", None, None, "key 1", "key 2", "key 1"],
        "value1": [None, "value", None, None, None, "value"],
        "value2": ["value", None, None, None, "value", None],
    },
    # Copied from pandas docs
    {
        "by": ["male", "male", "female", "male", "female", "male"],
        "value1": ["low", "medium", "high", "low", "high", "low"],
        "value2": ["US", "FR", "US", "FR", "FR", "FR"],
    },
]


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("by", ["by", ["by", "value1"], ["by", "value2"]])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize(
    "subset",
    [None, ["value1"], ["value2"], ["value1", "value2"]],
)
@pytest.mark.parametrize("normalize", [False])
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_basic(test_data, by, as_index, subset, normalize, dropna):
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data),
        lambda df: df.groupby(by=["by"], as_index=as_index).value_counts(
            subset=subset,
            normalize=normalize,
            dropna=dropna,
        ),
    )


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("subset", [["by"], []])
def test_value_counts_subset_negative(test_data, subset):
    with SqlCounter(query_count=1 if len(subset) > 0 else 0):
        eval_snowpark_pandas_result(
            *create_test_dfs(test_data),
            lambda x: x.groupby(by=["by"]).value_counts(subset=subset),
            expect_exception=True,
        )


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_sort_ascending(test_data, as_index, sort, ascending):
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data),
        lambda x: x.groupby(by=["by"], as_index=as_index).value_counts(
            sort=sort, ascending=ascending
        ),
    )


@sql_count_checker(query_count=1)
def test_value_counts_series():
    by = ["a", "a", "b", "b", "a", "c"]
    native_ser = native_pd.Series(
        [0, 0, None, 1, None, 3],
    )
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: ser.groupby(by=by).value_counts()
    )
