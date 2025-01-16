#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def test_pivot(df_pivot_data):
    native_df = native_pd.DataFrame(df_pivot_data)
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.pivot(index="foo", columns="bar", values="baz"),
            # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
            test_attrs=False,
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.pivot(index="foo", columns="bar")["baz"],
            test_attrs=False,
        )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.pivot(index="foo", columns="bar", values=["baz", "zoo"]),
            test_attrs=False,
        )


@sql_count_checker(query_count=1)
def test_pivot_pandas_general(df_pivot_data):
    native_df = native_pd.DataFrame(df_pivot_data)
    snow_df = pd.DataFrame(native_df)

    output_df_native = native_pd.pivot(
        data=native_df, index="foo", columns="bar", values="baz"
    )
    output_df_snow = pd.pivot(data=snow_df, index="foo", columns="bar", values="baz")
    eval_snowpark_pandas_result(output_df_snow, output_df_native, lambda df: df)


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
            # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
            test_attrs=False,
        )
