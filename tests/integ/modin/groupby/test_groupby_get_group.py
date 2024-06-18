#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import modin.pandas as pd
import numpy as np
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import eval_snowpark_pandas_result


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
def test_groupby_get_group(by):
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
    name = pandas_df[by].iloc[0]
    if isinstance(by, list):
        with pytest.raises(
            NotImplementedError,
            match="Snowpark pandas GroupBy.get_group does not yet support multiple by columns.",
        ):
            snowpark_pandas_df.groupby(by).get_group(name)
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snowpark_pandas_df,
                pandas_df,
                lambda df: df.groupby(by).get_group(name),
            )
        # DataFrame with __getitem__
        with pytest.raises(
            NotImplementedError,
            match="get_group is not yet implemented for SeriesGroupBy",
        ):
            snowpark_pandas_df.groupby(by)["col5_int16"].get_group(name)


@sql_count_checker(query_count=0)
def test_error_checking():
    s = pd.Series(list("abc") * 4)
    with pytest.raises(NotImplementedError):
        s.groupby(s).get_group("b")
