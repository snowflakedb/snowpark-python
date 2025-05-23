#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

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
@pytest.mark.parametrize("by", ["by", ["value1", "by"], ["by", "value2"]])
@pytest.mark.parametrize("groupby_sort", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize(
    "subset",
    [None, ["value1"], ["value2"], ["value1", "value2"]],
)
@pytest.mark.parametrize("dropna", [True, False])
def test_value_counts_basic(
    test_data, by, groupby_sort, sort, ascending, subset, dropna
):
    by_list = by if isinstance(by, list) else [by]
    value_counts_kwargs = {
        "sort": sort,
        "ascending": ascending,
        "subset": subset,
        "dropna": dropna,
    }
    if len(set(by_list) & set(subset or [])):
        # If subset and by overlap, check for ValueError
        # Unlike pandas, we do not surface label names in the error message
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                *create_test_dfs(test_data),
                lambda df: df.groupby(by=by, sort=groupby_sort).value_counts(
                    **value_counts_kwargs
                ),
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match="in subset cannot be in the groupby column keys",
                assert_exception_equal=False,
            )
            return
    with SqlCounter(query_count=1):
        none_in_by_col = any(None in test_data[col] for col in by_list)
        if not dropna and none_in_by_col:
            # when dropna is False, pandas gives a different result because it drops all NaN
            # keys in the multiindex
            # https://github.com/pandas-dev/pandas/issues/56366
            # as a workaround, replace all Nones in the pandas frame with a sentinel value
            # since NaNs are sorted last, we want the sentinel to sort to the end as well
            VALUE_COUNTS_TEST_SENTINEL = "zzzzzz"
            snow_df, native_df = create_test_dfs(test_data)
            snow_result = snow_df.groupby(by=by, sort=groupby_sort).value_counts(
                **value_counts_kwargs
            )
            native_df = native_df.fillna(value=VALUE_COUNTS_TEST_SENTINEL)
            native_result = native_df.groupby(by=by, sort=groupby_sort).value_counts(
                **value_counts_kwargs
            )
            native_result.index = native_result.index.map(
                lambda x: tuple(
                    None if i == VALUE_COUNTS_TEST_SENTINEL else i for i in x
                )
            )
            assert_snowpark_pandas_equal_to_pandas(snow_result, native_result)
        else:
            eval_snowpark_pandas_result(
                *create_test_dfs(test_data),
                lambda df: df.groupby(by=by, sort=groupby_sort).value_counts(
                    **value_counts_kwargs
                ),
            )


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("by", ["by", ["value1", "by"], ["by", "value2"]])
@pytest.mark.parametrize("groupby_sort", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("ascending", [True, False])
@pytest.mark.parametrize("normalize", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_normalize(
    test_data, by, groupby_sort, sort, ascending, normalize
):
    value_counts_kwargs = {
        "sort": sort,
        "ascending": ascending,
        "normalize": normalize,
    }
    # When normalize is set, pandas will (counter-intuitively) sort by the pre-normalization
    # counts rather than the result proportions. This only matters if groupby_sort is False
    # and sort is True.
    # We work around this by using check_like=True
    # See https://github.com/pandas-dev/pandas/issues/59307#issuecomment-2313767856
    check_like = not groupby_sort and sort and normalize
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data),
        lambda df: df.groupby(by=by, sort=groupby_sort).value_counts(
            **value_counts_kwargs
        ),
        check_like=check_like,
    )


@pytest.mark.parametrize("test_data", TEST_DATA)
@pytest.mark.parametrize("by", ["by", ["value1", "by"], ["by", "value2"]])
@pytest.mark.parametrize("groupby_sort", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@sql_count_checker(query_count=1)
def test_value_counts_as_index(test_data, by, groupby_sort, sort, as_index):
    eval_snowpark_pandas_result(
        *create_test_dfs(test_data),
        lambda df: df.groupby(by=by, sort=groupby_sort, as_index=as_index).value_counts(
            sort=sort
        ),
    )


@pytest.mark.parametrize(
    "subset, exception_cls",
    [
        (["bad_key"], KeyError),  # key not in frame
        (["by"], ValueError),  # subset cannot overlap with grouping columns
        (["by", "bad_key"], ValueError),  # subset cannot overlap with grouping columns
    ],
)
@sql_count_checker(query_count=0)
def test_value_counts_bad_subset(subset, exception_cls):
    eval_snowpark_pandas_result(
        *create_test_dfs(TEST_DATA[0]),
        lambda x: x.groupby(by=["by"]).value_counts(subset=subset),
        expect_exception=True,
        expect_exception_type=exception_cls,
        assert_exception_equal=False,
    )


# A JOIN is needed to set the index to the by list
@sql_count_checker(query_count=1, join_count=1)
def test_value_counts_series():
    by = ["a", "a", "b", "b", "a", "c"]
    native_ser = native_pd.Series(
        [0, 0, None, 1, None, 3],
    )
    snow_ser = pd.Series(native_ser)
    eval_snowpark_pandas_result(
        snow_ser, native_ser, lambda ser: ser.groupby(by=by).value_counts()
    )


@sql_count_checker(query_count=0)
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
