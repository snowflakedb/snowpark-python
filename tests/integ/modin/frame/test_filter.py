#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import random

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def _generate_data(N=100):
    data = {
        "timedelta": native_pd.timedelta_range(start="1 day", periods=N),
        "X": np.random.randint(-100, 100, size=N),
        "Y": np.random.randint(-100, 100, size=N),
        "Z": np.random.randint(-100, 100, size=N),
    }

    data["A"] = [
        random.choice(["Elephant", "Zebra", "Giraffe", "Lion", "Penguin", "Mouse"])
        for _ in range(N)
    ]
    data["B"] = [random.choice(["small", "large", "medium", None]) for _ in range(N)]
    data["C"] = np.random.normal(size=N)

    data["W"] = np.random.random(size=N) > 0.5
    return data


@pytest.mark.parametrize("key", [-1, 10, 1200])
@sql_count_checker(query_count=1, join_count=0)
def test_basic_filter_single_column(key):
    data = {
        "X": np.random.randint(0, 100, size=100),
        "Y": np.random.randint(0, 100, size=100),
        "Z": np.random.randint(0, 100, size=100),
    }

    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df[df.X > key])


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df[df["W"]],
        lambda df: df[~df["W"]],
        lambda df: df[(df.X > 20) & (df.X < 40)],
        lambda df: df[(df["Y"] > 50) | df.X <= 20],
        lambda df: df[df["A"] == "Zebra"],
        lambda df: df[df["A"] != "Mouse"],
        lambda df: df[~(df["A"] == "Zebra")],
        param(
            lambda df: df[~(df["timedelta"] == pd.Timedelta("1 day"))],
            id="timedelta_not_equal",
        ),
        param(
            lambda df: df[
                ~(
                    (df["timedelta"] == pd.Timedelta("1 day"))
                    | (df["timedelta"] == pd.Timedelta("2 days"))
                )
            ],
            id="timedelta_exclude_two_values",
        ),
        param(
            lambda df: df[df["timedelta"] > pd.Timedelta("50 days")],
            id="timedelta_greater_than_scalar",
        ),
    ],
)
@sql_count_checker(query_count=4)
def test_filtering_with_self(func):
    data = _generate_data()
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    eval_snowpark_pandas_result(snow_df, native_df, func)


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "func",
    [
        # Note 1: can't use B here e.g., because column contains None - but Snowflake would allow this.
        # Note 2: Make sure that some unsupported operation is used below
        lambda df: df[df.A.str.casefold().str.startswith("P")],
        lambda df: df[df.A.str.casefold().str.lower() == "zebra"],
    ],
)
def test_filtering_with_self_not_implemented(
    func,
):
    data = _generate_data()
    snow_df = pd.DataFrame(data)
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method Series.str.casefold",
    ):
        func(snow_df)


@pytest.mark.parametrize(
    "df1,df2,func, expected_query_count, expected_join_count",
    [
        (
            native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 3, 5]}),
            native_pd.DataFrame({"X": ["a", "b", "c"], "Y": [0, 1, 0]}),
            lambda df1, df2: df1[df2.Y == 0],
            1,
            1,
        ),
        (
            native_pd.DataFrame({"A": [100, -20, 4.5, 7.8], "B": ["a", "b", "c", "d"]}),
            native_pd.DataFrame({"X": [0, 1, 0, None], "Y": [20, 7, 3.2, 5]}),
            lambda df1, df2: df1[(df2.Y >= 5) & (df2.Y < 10)],
            1,
            1,
        ),
        (
            native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 3, 5]}, index=[4, 3, 5]),
            native_pd.DataFrame(
                {"X": ["a", "b", "c"], "Y": [0, 1, 0]}, index=[5, 4, 3]
            ),
            lambda df1, df2: df1[df2.Y != 0],
            1,
            1,
        ),
        param(
            native_pd.DataFrame(
                {"A": native_pd.to_timedelta([0, "2 days"]), "B": [3, 4]}
            ),
            native_pd.DataFrame(
                {"A": native_pd.to_timedelta(["1 days", "2 days"]), "B": [3, 4]}
            ),
            lambda df1, df2: df2[df1.A != pd.Timedelta(1)],
            1,
            1,
            id="timedelta_and_int_column",
        ),
    ],
)
def test_filtering_df_with_other_df(
    df1, df2, func, expected_query_count, expected_join_count
):
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        snow_df1 = pd.DataFrame(df1)
        snow_df2 = pd.DataFrame(df2)

        ans = func(df1, df2)
        snow_ans = func(snow_df1, snow_df2)

        assert_snowpark_pandas_equal_to_pandas(snow_ans, ans, check_dtype=False)


@sql_count_checker(query_count=1, join_count=1)
def test_filtering_df_with_other_df_unaligned_index():
    native_df1 = native_pd.DataFrame({"A": [1, 2, 3], "B": [4, 3, 5]}, index=[4, 3, 5])
    native_df2 = native_pd.DataFrame(
        {"X": ["a", "b", "c"], "Y": [0, 1, 0]}, index=[1, 3, 10]
    )
    snow_df1 = pd.DataFrame(native_df1)
    snow_df2 = pd.DataFrame(native_df2)

    # This behavior is different compare with native pandas. Native pandas raises IndexError if the index value of
    # key is not in the index of the dataframe.
    # In Snowpark pandas, we do not trigger eager evaluation for validation, and it actually produces a result with
    # the filter value.
    snow_res = snow_df1[snow_df2.Y != 0]
    expected_res = native_pd.DataFrame({"A": [2], "B": [3]}, index=[3])
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_res, expected_res)
