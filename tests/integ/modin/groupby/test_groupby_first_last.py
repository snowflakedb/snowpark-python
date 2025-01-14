#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

data_dictionary = {
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
    "col11_timedelta": [
        pd.Timedelta("1 days"),
        None,
        pd.Timedelta("2 days"),
        None,
        None,
        None,
        None,
        None,
        None,
    ],
}


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
@pytest.mark.parametrize("skipna", [True, False])
@pytest.mark.parametrize("method", ["first", "last"])
def test_groupby_first_last(by, as_index, skipna, method):
    # TODO: Add sort when SNOW-1481281 is resolved
    snowpark_pandas_df = pd.DataFrame(data_dictionary)
    pandas_df = snowpark_pandas_df.to_pandas()
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: getattr(df.groupby(by, as_index=as_index), method)(
                skipna=skipna
            ),
        )
    # DataFrame with __getitem__
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            pandas_df,
            lambda df: getattr(df.groupby(by, as_index=as_index)["col5_int16"], method)(
                skipna=skipna
            ),
        )

    # TODO: Support min_count in groupby first and last (SNOW-1482931)
    with pytest.raises(NotImplementedError):
        snowpark_pandas_df.groupby(by).first(min_count=2)


@sql_count_checker(query_count=0)
def test_error_checking():
    s = pd.Series(list("abc") * 4)
    with pytest.raises(NotImplementedError):
        s.groupby(s).first()

    with pytest.raises(NotImplementedError):
        s.groupby(s).last()


@pytest.mark.parametrize("agg_func", ["first", "last"])
@pytest.mark.parametrize("by", ["A", "B"])
@sql_count_checker(query_count=1)
def test_timedelta(agg_func, by):
    native_df = native_pd.DataFrame(
        {
            "A": native_pd.to_timedelta(
                ["1 days 06:05:01.00003", "16us", "nan", "16us"]
            ),
            "B": [8, 8, 12, 10],
        }
    )
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: getattr(df.groupby(by), agg_func)()
    )
