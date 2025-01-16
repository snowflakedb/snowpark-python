#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
@pytest.mark.parametrize(
    "by",
    [
        "LEXLUTHOR",
        ["LOBO", "DARKSEID"],
        ["DARKSEID", "FRIENDS"],
        "FRIENDS",
        ["FRIENDS", "RATING"],
    ],
)
@pytest.mark.parametrize("as_index", [True, False])
def test_groupby_shift(periods, by, as_index):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, "Lois", 42.42],
            [1, 5, 6, "Lana", 55.55],
            [2, 5, 8, "Luma", 76.76],
            [2, 6, 9, "Lyla", 90099.95],
            [3, 7, 10, "Cat", 888.88],
        ],
        columns=["LEXLUTHOR", "LOBO", "DARKSEID", "FRIENDS", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(by=by, as_index=as_index).shift(periods=periods),
        )


@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
@pytest.mark.parametrize(
    "by",
    [
        1,
        [2, 3],
        [3, 4],
        4,
        [4, 5],
    ],
)
def test_groupby_shift_columns_with_numeric_names(periods, by):
    pandas_df = native_pd.DataFrame(
        data=[
            [1, 2, 3, "Lois", 42.42],
            [1, 5, 6, "Lana", 55.55],
            [2, 5, 8, "Luma", 76.76],
            [2, 6, 9, "Lyla", 90099.95],
            [3, 7, 10, "Cat", 888.88],
        ],
        columns=[1, 2, 3, 4, 5],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(by).shift(periods=periods),
        )


@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
def test_groupby_shift_with_fill_string(periods):
    pandas_df = native_pd.DataFrame(
        data=[
            ["Lois", 42.42],
            ["Lana", 55.55],
            ["Luma", 76.76],
            ["Lyla", 90099.95],
            ["Cat", 888.88],
        ],
        columns=["LEXLUTHOR", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(["RATING"]).shift(
                periods=periods, fill_value="mxyzptlk"
            ),
        )


@pytest.mark.parametrize("periods", [0, 1, 2, 3, -3, -2, -1])
def test_groupby_shift_with_fill_numeric(periods):
    pandas_df = native_pd.DataFrame(
        data=[
            ["Lois", 42],
            ["Lois", 42],
            ["Lana", 76],
            ["Lana", 76],
            ["Lima", 888],
        ],
        columns=["LEXLUTHOR", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            pandas_df,
            lambda df: df.groupby(["LEXLUTHOR"]).shift(
                periods=periods, fill_value=4242
            ),
        )


@sql_count_checker(query_count=1)
def test_groupby_shift_with_fill_string_for_numeric_column():
    pandas_df = native_pd.DataFrame(
        data=[
            ["Lois", 42],
            ["Lois", 42],
            ["Lana", 76],
            ["Lana", 76],
            ["Lima", 888],
        ],
        columns=["LEXLUTHOR", "RATING"],
        index=["tuna", "salmon", "catfish", "goldfish", "shark"],
    )
    snow_df = pd.DataFrame(pandas_df)
    eval_snowpark_pandas_result(
        snow_df,
        pandas_df,
        lambda df: df.groupby(["LEXLUTHOR"]).shift(periods=0, fill_value="mxyzptlk"),
    )
