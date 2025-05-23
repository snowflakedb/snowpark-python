#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
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
        ["col1_grp", "col2_int64"],
        ["col6_mixed", "col7_bool", "col3_int_identical"],
    ],
)
@pytest.mark.parametrize("as_index", [True, False])
def test_groupby_size(by, as_index):
    snowpark_pandas_df = pd.DataFrame(
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
    pandas_df = snowpark_pandas_df.to_pandas()
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: df.groupby(by, as_index=as_index).size(),
        )

    # DataFrame with __getitem__
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: df.groupby(by, as_index=as_index)["col5_int16"].size(),
        )


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
        ["col1_grp", "col2_int64"],
        ["col6_mixed", "col7_bool", "col3_int_identical"],
    ],
)
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("size_func", ["size", len])
def test_groupby_agg_size(by, as_index, size_func):
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
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: df.groupby(by, as_index=as_index).agg(
                new_col=pd.NamedAgg("col5_int16", size_func)
            ),
            # This is a bug in pandas - the attrs are not propagated for
            # size, but are propagated for other functions.
            test_attrs=False,
        )

    # DataFrame with __getitem__
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: df.groupby(by, as_index=as_index)["col5_int16"].agg(
                new_col=size_func
            ),
            # This is a bug in pandas - the attrs are not propagated for
            # size, but are propagated for other functions.
            test_attrs=False,
        )


@sql_count_checker(query_count=0)
def test_error_checking():
    s = pd.Series(list("abc") * 4)
    with pytest.raises(NotImplementedError):
        s.groupby(s).size()


@sql_count_checker(query_count=0)
def test_multiindex_negative():
    # Because of internal calls to reset_index, attempting to perform groupby_size with
    # a level parameter in a MultiIndex frame will fail.
    df = pd.DataFrame(
        {"a": list(range(10)), "b": list(range(10)), "c": list(range(10))}
    ).set_index(["a", "b"])
    with pytest.raises(KeyError, match=re.escape("['a'] not in index")):
        df.groupby(pd.Grouper(level=0)).size()


@pytest.mark.parametrize("by", ["A", "B"])
@sql_count_checker(query_count=1)
def test_timedelta(by):
    native_df = native_pd.DataFrame(
        {
            "A": native_pd.to_timedelta(
                ["1 days 06:05:01.00003", "16us", "nan", "16us"]
            ),
            "B": [8, 8, 12, 10],
            "C": ["the", "name", "is", "bond"],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by).size(),
    )


@pytest.mark.parametrize("by", ["A", "B"])
@pytest.mark.parametrize("size_func", ["size", len])
@sql_count_checker(query_count=1)
def test_timedelta_agg(by, size_func):
    native_df = native_pd.DataFrame(
        {
            "A": native_pd.to_timedelta(
                ["1 days 06:05:01.00003", "16us", "nan", "16us"]
            ),
            "B": [8, 8, 12, 10],
            "C": ["the", "name", "is", "bond"],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby(by).agg(
            d=pd.NamedAgg("A" if by != "A" else "C", size_func)
        ),
        # This is a bug in pandas - the attrs are not propagated for
        # size, but are propagated for other functions.
        test_attrs=False,
    )
