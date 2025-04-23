#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result, create_test_series
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "by",
    [
        "col1_grp",
        "col2_int64",
        "col3_int_identical",
        "col4_int32",
        "col6_mixed",
        "col7_bool",
        "col8_bool_missing",
        "col9_int_missing",
        "col10_mixed_missing",
    ],
)
def test_groupby_get_group(by):
    pandas_df = native_pd.DataFrame(
        {
            "col1_grp": ["g1", "g2", "g0", "g0", "g2", "g3", "g0", "g2", "g3"],
            "col2_int64": np.arange(9, dtype="int64") // 3,
            "col3_int_identical": [2] * 9,
            "col4_int32": np.arange(9, dtype="int32") // 4,
            "col5_int16": np.arange(9, dtype="int16") // 3,
            "col6_mixed": np.concatenate(
                [
                    np.arange(3, dtype="int64") // 3,
                    np.arange(3, dtype="int32") // 3,
                    np.arange(3, dtype="int16") // 3,
                ]
            ),
            "col7_bool": [True] * 5 + [False] * 4,
            "col8_bool_missing": [
                True,
                None,
                False,
                False,
                None,
                None,
                True,
                False,
                None,
            ],
            "col9_int_missing": [5, 6, np.nan, 2, 1, np.nan, 5, np.nan, np.nan],
            "col10_mixed_missing": np.concatenate(
                [
                    np.arange(2, dtype="int64") // 3,
                    [np.nan],
                    np.arange(2, dtype="int32") // 3,
                    [np.nan],
                    np.arange(2, dtype="int16") // 3,
                    [np.nan],
                ]
            ),
        }
    )
    snowpark_pandas_df = pd.DataFrame(pandas_df)
    name = pandas_df[by if not isinstance(by, list) else by[0]].iloc[0]

    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: df.groupby(by).get_group(name),
        )
    # DataFrame with __getitem__
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas does not yet support the method SeriesGroupBy.get_group",
    ):
        snowpark_pandas_df.groupby(by)["col5_int16"].get_group(name)


@sql_count_checker(query_count=0)
def test_groupby_get_group_with_list():
    snowpark_pandas_df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3], "c": [1, 2, 3]})
    by = ["a", "b"]
    name = 1
    with pytest.raises(
        NotImplementedError,
        match="Snowpark pandas GroupBy.get_group does not yet support multiple by columns.",
    ):
        snowpark_pandas_df.groupby(by).get_group(name)


@sql_count_checker(query_count=0)
def test_error_message():
    eval_snowpark_pandas_result(
        *create_test_series(list("abc") * 4),
        lambda s: s.groupby(pd.Grouper("b")).get_group("b"),
        expect_exception=True,
        expect_exception_type=KeyError,
        expect_exception_match=False,
        assert_exception_equal=False,
    )
