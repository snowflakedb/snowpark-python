#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    eval_snowpark_pandas_result,
)

TEST_DATA = [
    [1, 2, 2, 3, 3, 3],
    ["1", "2", "2", "3", "3", "3"],
]


TEST_NULL_DATA = [
    [1, 2, 3, 3, 3, None, np.nan, 4],
    ["2", "3", "3", "1", "1", "1", None],
    [None, None, np.nan],
]


NATIVE_SERIES_TEST_DATA = [
    native_pd.Series(
        [1, 2, 3, 2, 3, 5, 6, 7, 8, 4, 4, 5, 6, 7, 1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 7]
    ),
    # native_pd.Series([1.1, 2.2, 1.0, 1, 1.1, 2.2, 1, 1, 1, 2, 2, 2, 2.2]),
    # native_pd.Series([1, 3, 1, 1, 1, 3, 1, 1, 1, 2, 2, 2, 3]),
    # native_pd.Series(
    #     [True, False, True, False, True, False, True, False, True, True], dtype=bool
    # ),
    native_pd.Series(
        [
            "a",
            "b",
            "c",
            "b",
            "c",
            "e",
            "f",
            "g",
            "h",
            "d",
            "d",
            "e",
            "f",
            "g",
            "a",
            "b",
            "a",
            "b",
            "c",
            "d",
            "c",
            "d",
            "e",
            "f",
            "g",
        ]
    ),
]


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("has_name", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_sort_ascending(test_data, sort, ascending, has_name):
    snow_series = pd.Series(test_data, name="name" if has_name else None)
    native_series = native_pd.Series(test_data, name="name" if has_name else None)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda x: x.value_counts(sort=sort, ascending=ascending),
    )


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("has_name", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_normalize(test_data, has_name):
    snow_series = pd.Series(test_data, name="name" if has_name else None).value_counts(
        normalize=True
    )
    native_series = native_pd.Series(
        test_data, name="name" if has_name else None
    ).value_counts(normalize=True)
    # snowpark pandas will return a series with decimal type
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64(
        snow_series,
        native_series,
    )


@pytest.mark.parametrize("test_data", TEST_NULL_DATA)
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_dropna(test_data, dropna):
    snow_series = pd.Series(test_data)
    native_series = native_pd.Series(test_data)
    # if NULL value is not dropped, the index will contain NULL
    # Snowpark pandas returns string type but pandas returns mixed type
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda x: x.value_counts(dropna=dropna),
        check_index_type=dropna,
    )


@sql_count_checker(query_count=0)
def test_value_counts_bins():
    with pytest.raises(NotImplementedError, match="bins argument is not yet supported"):
        pd.Series([1, 2, 3, 4]).value_counts(bins=3)


@pytest.mark.parametrize("native_series", NATIVE_SERIES_TEST_DATA)
@pytest.mark.parametrize("normalize", [True, False])
@pytest.mark.parametrize("sort", [True])
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=2)
def test_series_value_counts(native_series, normalize, sort, ascending, dropna):
    snow_series = pd.Series(native_series)
    print(f"\n___PANDAS VERSION___\n{native_pd.__version__}")
    print("\n___NATIVE PANDAS VALUE COUNTS RESULT___")
    print(
        native_series.value_counts(
            normalize=normalize, sort=sort, ascending=ascending, dropna=dropna
        )
    )
    print("\nSNOWPARK PANDAS VALUE COUNTS RESULT___")
    print(
        snow_series.value_counts(
            normalize=normalize, sort=sort, ascending=ascending, dropna=dropna
        )
    )
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda s: s.value_counts(
            normalize=normalize, sort=sort, ascending=ascending, dropna=dropna
        ),
    )
