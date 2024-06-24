#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
import pytest
from pandas.errors import IndexingError

from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result  # noqa: F401


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_get_default_index_str_columns(
    key,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    assert default_index_snowpark_pandas_df.iat[key] == default_index_native_df.iat[key]


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_default_index_str_columns(
    key,
    default_index_snowpark_pandas_df,
    default_index_native_df,
):
    def iat_set_helper(df):
        df.iat[key] = 100

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        iat_set_helper,
        inplace=True,
    )


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_get_str_index_str_columns(
    key,
    str_index_snowpark_pandas_df,
    str_index_native_df,
):
    assert str_index_snowpark_pandas_df.iat[key] == str_index_native_df.iat[key]


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_str_index_str_columns(
    key,
    str_index_snowpark_pandas_df,
    str_index_native_df,
):
    def iat_set_helper(df):
        df.iat[key] = 100

    eval_snowpark_pandas_result(
        str_index_snowpark_pandas_df, str_index_native_df, iat_set_helper, inplace=True
    )


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_get_time_index_time_columns(
    key,
    time_index_snowpark_pandas_df,
    time_index_native_df,
):
    assert time_index_snowpark_pandas_df.iat[key] == time_index_native_df.iat[key]


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_time_index_time_columns(
    key,
    time_index_snowpark_pandas_df,
    time_index_native_df,
):
    def iat_set_helper(df):
        df.iat[key] = 100

    eval_snowpark_pandas_result(
        time_index_snowpark_pandas_df,
        time_index_native_df,
        iat_set_helper,
        inplace=True,
    )


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_get_multiindex_index_str_columns(
    key,
    default_index_native_df,
    multiindex_native,
):
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    assert snowpark_df.iat[key] == native_df.iat[key]


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_multiindex_index_str_columns(
    key,
    default_index_native_df,
    multiindex_native,
):
    def at_set_helper(df):
        df.iat[key] = 100

    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snowpark_df, native_df, at_set_helper, inplace=True)


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_get_default_index_multiindex_columns(
    key,
    native_df_with_multiindex_columns,
):
    native_df = native_df_with_multiindex_columns
    snowpark_df = pd.DataFrame(native_df)
    assert snowpark_df.iat[key] == native_df.iat[key]


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_default_index_multiindex_columns(
    key,
    native_df_with_multiindex_columns,
):
    def at_set_helper(df):
        df.iat[key] = 100

    native_df = native_df_with_multiindex_columns
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snowpark_df, native_df, at_set_helper, inplace=True)


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_get_multiindex_index_multiindex_columns(
    key,
    native_df_with_multiindex_columns,
    multiindex_native,
):
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    assert snowpark_df.iat[key] == native_df.iat[key]


@pytest.mark.parametrize(
    "key",
    [
        (0, 0),
        (0, -7),
        (-7, 0),
        (-7, -7),
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_iat_set_multiindex_index_multiindex_columns(
    key,
    native_df_with_multiindex_columns,
    multiindex_native,
):
    def at_set_helper(df):
        df.iat[key] = 100

    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(snowpark_df, native_df, at_set_helper, inplace=True)


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
        (("a", "b"), IndexError),
        ((0, 0, 0), IndexingError),
    ],
)
@sql_count_checker(query_count=0)
def test_iat_neg(
    key,
    error,
    default_index_snowpark_pandas_df,
):
    with pytest.raises(error):
        default_index_snowpark_pandas_df.iat[key]
