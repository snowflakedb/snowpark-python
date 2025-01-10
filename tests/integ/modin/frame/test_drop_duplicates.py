#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize("subset", ["a", ["a"], ["a", "B"], []])
def test_drop_duplicates_with_misspelled_column_name_or_empty_subset(subset):
    df = native_pd.DataFrame({"A": [0, 0, 1], "B": [0, 0, 1], "C": [0, 0, 1]})
    # pandas explicitly check existence of the values from subset while Snowpark pandas does not since duplicated API is
    # using getitem to fetch the column(s)
    with pytest.raises((KeyError, ValueError)):
        df.drop_duplicates(subset)
    expected_res = (
        df.drop_duplicates(["B"])
        if "B" in subset
        else native_pd.DataFrame(columns=["A", "B", "C"])
    )
    query_count = 0
    join_count = 0
    if subset == []:
        join_count += 1
        query_count += 1
    with SqlCounter(query_count=query_count, join_count=join_count):
        if subset == []:
            assert_frame_equal(
                pd.DataFrame(df).drop_duplicates(subset),
                expected_res,
                check_dtype=False,
                check_index_type=False,
            )
        else:
            if isinstance(subset, list):
                if all(label not in df.columns for label in subset):
                    match_str = r"None of .* are in the \[columns\]"
                else:
                    match_str = r".* not in index"
            else:
                match_str = r"None of .* are in the \[columns\]"
            with pytest.raises(KeyError, match=match_str):
                assert_frame_equal(
                    pd.DataFrame(df).drop_duplicates(subset),
                    expected_res,
                    check_dtype=False,
                    check_index_type=False,
                )


@pytest.mark.parametrize("subset", ["A", ["A"], ["B"], ["A", "B"]])
@pytest.mark.parametrize("keep", ["first", "last", False])
@pytest.mark.parametrize("ignore_index", [True, False])
def test_drop_duplicates(subset, keep, ignore_index):
    pandas_df = native_pd.DataFrame(
        {"A": [0, 1, 1, 2, 0], "B": ["a", "b", "c", "b", "a"]}
    )
    snow_df = pd.DataFrame(pandas_df)
    query_count = 1
    join_count = 2
    if ignore_index is True:
        query_count += 1
        join_count += 3
    with SqlCounter(query_count=query_count, join_count=join_count):
        assert_frame_equal(
            snow_df.drop_duplicates(
                subset=subset, keep=keep, ignore_index=ignore_index
            ),
            pandas_df.drop_duplicates(
                subset=subset, keep=keep, ignore_index=ignore_index
            ),
            check_dtype=False,
            check_index_type=False,
        )


@pytest.mark.parametrize("subset", ["a", ["a"], ["b"], ["a", "b"]])
@pytest.mark.parametrize("keep", ["first", "last", False])
@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates_on_empty_frame(subset, keep):
    pandas_df = native_pd.DataFrame(columns=["a", "b"])
    snow_df = pd.DataFrame(pandas_df)

    assert_frame_equal(
        snow_df.drop_duplicates(subset=subset, keep=keep),
        pandas_df.drop_duplicates(subset=subset, keep=keep),
        check_dtype=False,
        check_index_type=False,
    )


@sql_count_checker(query_count=1, join_count=2)
def test_drop_duplicates_post_sort_values():
    pandas_df = native_pd.DataFrame(
        {"A": [0, 1, 1, 2, 0], "B": ["a", "b", "c", "b", "a"]}
    )
    snow_df = pd.DataFrame(pandas_df)

    assert_frame_equal(
        snow_df.sort_values("A", kind="stable").drop_duplicates(),
        pandas_df.sort_values("A", kind="stable").drop_duplicates(),
        check_dtype=False,
        check_index_type=False,
    )
