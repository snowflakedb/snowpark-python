#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

NATIVE_NUMERIC_INDEX_TEST_DATA = [
    native_pd.Index([], dtype="int64"),
    native_pd.Index([1], dtype="float64"),
    native_pd.Index([1.1, 2.2, 1.0, 1, 1.1, 2.2, 1, 1, 1, 2, 2, 2, 2.2]),
    native_pd.Index([1, 3, 1, 1, 1, 3, 1, 1, 1, 2, 2, 2, 3], name="random name"),
    native_pd.Index(
        [True, False, True, False, True, False, True, False, True, True], dtype=bool
    ),
]

INDEX1 = native_pd.Series(
    [1, 2, 3, 2, 3, 5, 6, 7, 8, 4, 4, 5, 6, 7, 1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 7]
)
INDEX2 = native_pd.Series(
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
)


@pytest.mark.parametrize(
    "native_series, expected, normalize, sort, ascending, dropna",
    [
        (
            INDEX1,
            native_pd.Series(
                [0.04, 0.12, 0.12, 0.12, 0.12, 0.16, 0.16, 0.16],
                index=[8, 1, 5, 6, 7, 2, 3, 4],
                name="proportion",
            ),
            True,
            True,
            True,
            True,
        ),
        (
            INDEX1,
            native_pd.Series(
                [0.04, 0.12, 0.12, 0.12, 0.12, 0.16, 0.16, 0.16],
                index=[8, 1, 5, 6, 7, 2, 3, 4],
                name="proportion",
            ),
            True,
            True,
            True,
            False,
        ),
        (
            INDEX1,
            native_pd.Series(
                [1, 3, 3, 3, 3, 4, 4, 4], index=[8, 1, 5, 6, 7, 2, 3, 4], name="count"
            ),
            False,
            True,
            True,
            True,
        ),
        (
            INDEX1,
            native_pd.Series(
                [1, 3, 3, 3, 3, 4, 4, 4], index=[8, 1, 5, 6, 7, 2, 3, 4], name="count"
            ),
            False,
            True,
            True,
            False,
        ),
        (
            INDEX2,
            native_pd.Series(
                [0.04, 0.12, 0.12, 0.12, 0.12, 0.16, 0.16, 0.16],
                index=["h", "a", "e", "f", "g", "b", "c", "d"],
                name="proportion",
            ),
            True,
            True,
            True,
            True,
        ),
        (
            INDEX2,
            native_pd.Series(
                [0.04, 0.12, 0.12, 0.12, 0.12, 0.16, 0.16, 0.16],
                index=["h", "a", "e", "f", "g", "b", "c", "d"],
                name="proportion",
            ),
            True,
            True,
            True,
            False,
        ),
        (
            INDEX2,
            native_pd.Series(
                [1, 3, 3, 3, 3, 4, 4, 4],
                index=["h", "a", "e", "f", "g", "b", "c", "d"],
                name="count",
            ),
            False,
            True,
            True,
            True,
        ),
        (
            INDEX2,
            native_pd.Series(
                [1, 3, 3, 3, 3, 4, 4, 4],
                index=["h", "a", "e", "f", "g", "b", "c", "d"],
                name="count",
            ),
            False,
            True,
            True,
            False,
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_series_value_counts_non_deterministic_pandas_behavior(
    native_series, expected, normalize, sort, ascending, dropna
):
    # Native pandas produces different results locally and on Jenkins when the above data is used.
    # Therefore, explicitly compare the actual and expected results.

    # If `sort=True`, sort by the frequencies. If `sort=False`, maintain the original ordering.
    # `ascending` controls whether to sort the count in ascending or descending order.

    # When there is a tie between frequencies, the order is still deterministic, but
    # may be different from the result from native pandas. Snowpark pandas will
    # always respect the order of insertion during ties. Native pandas is not
    # deterministic since the original order/order of insertion is based on the
    # Python hashmap which may produce different results on different versions.
    # Refer to: https://github.com/pandas-dev/pandas/issues/15833
    snow_index = pd.Index(native_series)
    actual = snow_index.value_counts(
        normalize=normalize, sort=sort, ascending=ascending, dropna=dropna
    )
    assert_series_equal(actual, expected)


@pytest.mark.parametrize("native_index", NATIVE_NUMERIC_INDEX_TEST_DATA)
@pytest.mark.parametrize("normalize", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_index_value_counts(native_index, normalize, sort, ascending, dropna):
    snow_index = pd.Index(native_index)
    eval_snowpark_pandas_result(
        snow_index,
        native_index,
        lambda idx: idx.value_counts(
            normalize=normalize, sort=sort, ascending=ascending, dropna=dropna
        ),
    )


@sql_count_checker(query_count=0)
def test_index_value_counts_bins_negative():
    snow_index = pd.Index([1, 2, 3, 4])
    with pytest.raises(NotImplementedError, match="bins argument is not yet supported"):
        snow_index.value_counts(bins=2)
