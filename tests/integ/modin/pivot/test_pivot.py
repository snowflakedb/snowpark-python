#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter
from tests.integ.modin.utils import eval_snowpark_pandas_result


def test_pivot():
    native_df = native_pd.DataFrame(
        {
            "foo": ["one", "one", "one", "two", "two", "two"],
            "bar": ["A", "B", "C", "A", "B", "C"],
            "baz": [1, 2, 3, 4, 5, 6],
            "zoo": ["x", "y", "z", "q", "w", "t"],
        }
    )
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.pivot(index="foo", columns="bar", values="baz"),
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.pivot(index="foo", columns="bar")["baz"]
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.pivot(index="foo", columns="bar", values=["baz", "zoo"]),
        )


def test_pivot_list_columns_names():
    native_df = native_pd.DataFrame(
        {
            "lev1": [1, 1, 1, 2, 2, 2],
            "lev2": [1, 1, 2, 1, 1, 2],
            "lev3": [1, 2, 1, 2, 1, 2],
            "lev4": [1, 2, 3, 4, 5, 6],
            "values": [0, 1, 2, 3, 4, 5],
        }
    )
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.pivot(
                index="lev1", columns=["lev2", "lev3"], values="values"
            ),
        )
