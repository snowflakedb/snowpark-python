#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    create_test_dfs,
    eval_snowpark_pandas_result,
)

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
@pytest.mark.parametrize("by", ["by"])  # , ["by", "value1"], ["by", "value2"]])
@pytest.mark.parametrize(
    "subset",
    [None, ["value1"], ["value2"], ["value1", "value2"]],
)
@pytest.mark.parametrize("normalize", [True, False])
@pytest.mark.parametrize("dropna", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_basic(test_data, by, subset, normalize, dropna):
    # Tests here use check_like=True because Snowpark pandas will always preserve the original
    # order of rows within groups, while native pandas provides this guarantee only for the
    # grouping and count/proportion columns. We check this ordering behavior in a separate test.
    #
    # Note that when as_index=False, check_like is insufficient because columns that were originally
    # part of the MultiIndex are demoted to data columns before value_counts returns, so check_like
    # doesn't know to reorder by those columns.
    #
    # See these issues for details:
    # https://github.com/pandas-dev/pandas/issues/59307
    # https://github.com/snowflakedb/snowpark-python/pull/1909
    # https://github.com/pandas-dev/pandas/issues/15833
    by_list = by if isinstance(by, list) else [by]
    none_in_by_col = any(None in test_data[col] for col in by_list)
    if not dropna and none_in_by_col:
        # when dropna is False, pandas gives a different result because it drops all NaN
        # keys in the multiindex
        # https://github.com/pandas-dev/pandas/issues/56366
        # as a workaround, replace all Nones in the pandas frame with a sentinel value
        # since NaNs are sorted last, we want the sentinel to sort to the end as well
        VALUE_COUNTS_TEST_SENTINEL = "zzzzzz"
        snow_df, native_df = create_test_dfs(test_data)
        snow_result = snow_df.groupby(by=by).value_counts(
            subset=subset,
            normalize=normalize,
            dropna=dropna,
        )
        native_df = native_df.fillna(value=VALUE_COUNTS_TEST_SENTINEL)
        native_result = native_df.groupby(by=by).value_counts(
            subset=subset,
            normalize=normalize,
            dropna=dropna,
        )
        native_result.index = native_result.index.map(
            lambda x: tuple(None if i == VALUE_COUNTS_TEST_SENTINEL else i for i in x)
        )
        assert_snowpark_pandas_equal_to_pandas(
            snow_result, native_result, check_like=True
        )
    else:
        eval_snowpark_pandas_result(
            *create_test_dfs(test_data),
            lambda df: df.groupby(by=by).value_counts(
                subset=subset,
                normalize=normalize,
                dropna=dropna,
            ),
            check_like=True,
        )


@pytest.mark.parametrize(
    "test_data, groupby_kwargs, sort, expected_result",
    [
        (
            TEST_DATA[0],
            {
                "by": ["by"],
                "as_index": True,
                "sort": True,
            },
            True,
            # In pandas, the row [c, dd, 2] appears before [c, ee, 1] despite the latter
            # appearing earlier in the original frame
            #
            # by  value1  value2
            # a   aa      1         2
            #     bb      3         1
            # b   aa      2         1
            #     bb      1         1
            #     cc      3         1
            # c   ee      1         1
            #     dd      2         1
            # Name: count, dtype: int64
            native_pd.Series(
                [2, 1, 1, 1, 1, 1, 1],
                name="count",
                index=pd.MultiIndex.from_tuples(
                    [
                        ("a", "aa", 1),
                        ("a", "bb", 3),
                        ("b", "aa", 2),
                        ("b", "bb", 1),
                        ("b", "cc", 3),
                        ("c", "ee", 1),
                        ("c", "dd", 2),
                    ],
                    names=["by", "value1", "value2"],
                ),
            ),
        ),
        (
            TEST_DATA[0],
            {
                "by": ["by"],
                "as_index": True,
                "sort": True,
            },
            False,
            # by  value1  value2
            # a   bb      3         1
            #     aa      1         2
            # b   aa      2         1
            #     bb      1         1
            #     cc      3         1
            # c   ee      1         1
            #     dd      2         1
            # Name: count, dtype: int64
            native_pd.Series(
                [1, 2, 1, 1, 1, 1, 1],
                name="count",
                index=pd.MultiIndex.from_tuples(
                    [
                        ("a", "bb", 3),
                        ("a", "aa", 1),
                        ("b", "aa", 2),
                        ("b", "bb", 1),
                        ("b", "cc", 3),
                        ("c", "ee", 1),
                        ("c", "dd", 2),
                    ],
                    names=["by", "value1", "value2"],
                ),
            ),
        ),
        (
            TEST_DATA[0],
            {
                "by": ["by"],
                "as_index": False,
                "sort": True,
            },
            True,
            #   by value1  value2  count
            # 0  a     aa       1      2
            # 1  a     bb       3      1
            # 2  b     aa       2      1
            # 3  b     bb       1      1
            # 4  b     cc       3      1
            # 5  c     ee       1      1
            # 6  c     dd       2      1
            native_pd.DataFrame(
                {
                    "by": ["a", "a", "b", "b", "b", "c", "c"],
                    "value1": ["aa", "bb", "aa", "bb", "cc", "ee", "dd"],
                    "value2": [1, 3, 2, 1, 3, 1, 2],
                    "count": [2, 1, 1, 1, 1, 1, 1],
                }
            ),
        ),
        (
            TEST_DATA[0],
            {
                "by": ["by"],
                "as_index": False,
                "sort": True,
            },
            False,
            #   by value1  value2  count
            # 0  a     bb       3      1
            # 1  a     aa       1      2
            # 2  b     aa       2      1
            # 3  b     bb       1      1
            # 4  b     cc       3      1
            # 5  c     ee       1      1
            # 6  c     dd       2      1
            native_pd.DataFrame(
                {
                    "by": ["a", "a", "b", "b", "b", "c", "c"],
                    "value1": ["bb", "aa", "aa", "bb", "cc", "ee", "dd"],
                    "value2": [3, 1, 2, 1, 3, 1, 2],
                    "count": [1, 2, 1, 1, 1, 1, 1],
                }
            ),
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_value_counts_pandas_issue_59307(
    test_data,
    groupby_kwargs,
    sort,
    expected_result,
):
    # Tests that Snowpark pandas preserves the order of rows from the input frame within groups.
    # When groupby(sort=True), native pandas guarantees order is preserved only for the grouping columns.
    # https://github.com/pandas-dev/pandas/issues/59307
    assert_snowpark_pandas_equal_to_pandas(
        pd.DataFrame(test_data).groupby(**groupby_kwargs).value_counts(sort=sort),
        expected_result,
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
@pytest.mark.parametrize("groupby_sort", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_sort_ascending(
    test_data, as_index, groupby_sort, sort, ascending
):
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data),
        lambda x: x.groupby(
            by=["by"], as_index=as_index, sort=groupby_sort
        ).value_counts(sort=sort, ascending=ascending),
    )


# An additional query is needed to validate the length of the by list
# A JOIN is needed to set the index to the by list
@sql_count_checker(query_count=2, join_count=1)
def test_value_counts_series():
    by = ["a", "a", "b", "b", "a", "c"]
    native_ser = native_pd.Series(
        [0, 0, None, 1, None, 3],
    )
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: ser.groupby(by=by).value_counts()
    )


# 1 query always runs to validate the length of the by list
@sql_count_checker(query_count=1)
def test_value_counts_bins_unimplemented():
    by = ["a", "a", "b", "b", "a", "c"]
    native_ser = native_pd.Series(
        [0, 0, None, 1, None, 3],
    )
    snow_ser = pd.Series(native_ser)
    with pytest.raises(NotImplementedError):
        eval_snowpark_pandas_result(
            snow_ser, native_ser, lambda ser: ser.groupby(by=by).value_counts(bins=3)
        )
