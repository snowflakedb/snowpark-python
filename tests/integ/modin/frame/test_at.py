#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest
from pandas.errors import IndexingError

from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=2, join_count=2)
def test_at_get_default_index_str_columns(
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    assert (
        default_index_snowpark_pandas_df.at[0, "A"]
        == default_index_native_df.at[0, "A"]
    )


@sql_count_checker(query_count=1, join_count=1)
def test_at_set_default_index_str_columns(
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    def at_set_helper(df):
        df.at[0, "A"] = 100

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        at_set_helper,
        inplace=True,
    )


@sql_count_checker(query_count=2, join_count=2)
def test_at_get_str_index_str_columns(
    str_index_snowpark_pandas_df,
    str_index_native_df,
):
    assert str_index_snowpark_pandas_df.at["b", "B"] == str_index_native_df.at["b", "B"]


@sql_count_checker(query_count=1, join_count=1)
def test_at_set_str_index_str_columns(
    str_index_snowpark_pandas_df,
    str_index_native_df,
):
    def at_set_helper(df):
        df.at["b", "B"] = 100

    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_df, str_index_native_df, at_set_helper, inplace=True
    )


@sql_count_checker(query_count=2)
def test_at_get_time_index_time_columns(
    time_index_snowpark_pandas_df,
    time_index_native_df,
):
    assert (
        time_index_snowpark_pandas_df.at["2023-01-01 00:00:00", "2023-01-01 00:00:00"]
        == time_index_native_df.at["2023-01-01 00:00:00", "2023-01-01 00:00:00"]
    )


@sql_count_checker(query_count=1, join_count=1)
def test_at_set_time_index_time_columns(
    time_index_snowpark_pandas_df,
    time_index_native_df,
):
    def at_set_helper(df):
        df.at["2023-01-01 00:00:00", "2023-01-01 00:00:00"] = 100

    eval_snowpark_pandas_result(
        time_index_snowpark_pandas_df, time_index_native_df, at_set_helper, inplace=True
    )


@sql_count_checker(query_count=2)
def test_at_get_multiindex_index_str_columns(
    default_index_native_df,
    multiindex_native,
):
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    assert snowpark_df.at[("bar", "one"), "A"] == native_df.at[("bar", "one"), "A"]


@sql_count_checker(query_count=0)
def test_at_set_multiindex_index_str_columns_neg(
    default_index_native_df,
    multiindex_native,
):
    def at_set_helper(df):
        df.at[("bar", "one"), "A"] = 100

    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with pytest.raises(NotImplementedError):
        at_set_helper(snowpark_df)


@sql_count_checker(query_count=2, join_count=2)
def test_at_get_default_index_multiindex_columns(
    native_df_with_multiindex_columns,
):
    native_df = native_df_with_multiindex_columns
    snowpark_df = pd.DataFrame(native_df)
    assert snowpark_df.at[0, ("bar", "one")] == native_df.at[0, ("bar", "one")]


@sql_count_checker(query_count=0)
def test_at_set_default_index_multiindex_columns_neg(
    native_df_with_multiindex_columns,
):
    def at_set_helper(df):
        df.at[0, ("bar", "one")] = 100

    native_df = native_df_with_multiindex_columns
    snowpark_df = pd.DataFrame(native_df)
    with pytest.raises(NotImplementedError):
        at_set_helper(snowpark_df)


@sql_count_checker(query_count=2)
def test_at_get_multiindex_index_multiindex_columns(
    native_df_with_multiindex_columns,
    multiindex_native,
):
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    assert (
        snowpark_df.at[("bar", "one"), ("bar", "one")]
        == native_df.at[("bar", "one"), ("bar", "one")]
    )


@sql_count_checker(query_count=0)
def test_at_set_multiindex_index_multiindex_columns_neg(
    native_df_with_multiindex_columns,
    multiindex_native,
):
    def at_set_helper(df):
        df.at[("bar", "one"), ("bar", "one")] = 100

    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with pytest.raises(NotImplementedError):
        at_set_helper(snowpark_df)


@pytest.mark.parametrize(
    "key, error",
    [
        (0, IndexingError),
        ([0, 0], IndexingError),
        ({0: 0}, IndexingError),
        ("a", IndexingError),
        ((0,), IndexingError),
        (([0, 0],), IndexingError),
        (({0: 0},), IndexingError),
        (([0], [0]), KeyError),
        (({0: 0}, {-1: -1}), KeyError),
        ((0, [0]), KeyError),
        (("a", "b"), KeyError),
        ((0, "A", 2), IndexingError),
    ],
)
@sql_count_checker(query_count=0)
def test_at_neg(
    key,
    error,
    default_index_snowpark_pandas_df,
):
    with pytest.raises(error):
        default_index_snowpark_pandas_df.at[key]


@pytest.mark.parametrize(
    "key, error",
    [
        ((("bar",), ("bar", "one")), IndexingError),
        ((("bar", "one"), ("bar",)), IndexingError),
        ((("bar",), ("bar",)), IndexingError),
        ((("bar", "one", "one"), ("bar", "one")), IndexingError),
    ],
)
@sql_count_checker(query_count=0)
def test_at_multiindex_neg(
    key,
    error,
    native_df_with_multiindex_columns,
    multiindex_native,
):
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with pytest.raises(error):
        snowpark_df.at[key]


@sql_count_checker(query_count=0)
def test_raise_set_cell_with_list_like_value_error():
    s = pd.Series([[1, 2], [3, 4]])
    with pytest.raises(NotImplementedError):
        s.at[0] = [0, 0]
    with pytest.raises(NotImplementedError):
        s.to_frame().at[0, 0] = [0, 0]
