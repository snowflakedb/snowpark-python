#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.pivot.pivot_utils import pivot_table_test_helper
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("columns", ["C", ["B", "C"]])
def test_pivot_table_single_value_with_dropna(df_data_with_nulls, dropna, columns):
    with SqlCounter(query_count=1, join_count=0 if dropna else 1):
        pivot_table_test_helper(
            df_data_with_nulls,
            {
                "index": "A",
                "columns": columns,
                "values": "D",
                "dropna": dropna,
            },
        )


@pytest.mark.parametrize(
    "aggfunc, expected_join_count",
    [
        ("mean", 3),
        ({"D": "max", "E": "sum"}, 2),
        ({"D": ["count", "max"], "E": ["mean", "sum"]}, 4),
        ({"D": "min", "E": ["mean"]}, 2),
        (["min", "max"], 6),
    ],
)
def test_pivot_table_multiple_values_dropna_nonnull_data(
    df_data,
    aggfunc,
    expected_join_count,
):
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data,
            {
                "index": ["A"],
                "columns": ["B", "C"],
                "values": ["D", "E", "F"],
                "dropna": False,
                "aggfunc": aggfunc,
            },
        )


@pytest.mark.parametrize(
    "aggfunc, expected_join_count",
    [
        ({"E": "count", "F": ["mean", "sum"]}, 3),
        ({"E": ["min", "max"], "F": ["mean", "sum"]}, 4),
        (["min", "max"], 4),
        ({"E": "min", "F": "mean"}, 2),
        ({"E": "max", "F": "max"}, 2),
    ],
)
def test_pivot_table_multiple_pivot_values_dropna_null_data(
    df_data_with_nulls,
    aggfunc,
    expected_join_count,
):
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data_with_nulls,
            {
                "index": ["A"],
                "columns": ["B", "C"],
                "values": ["E", "F"],
                "dropna": False,
                "aggfunc": aggfunc,
            },
        )


@sql_count_checker(query_count=1, join_count=5)
def test_pivot_table_multiple_index_single_pivot_values_dropna_null_data(
    df_data_with_nulls_2,
):
    pivot_table_test_helper(
        df_data_with_nulls_2,
        {
            "index": ["A", "B"],
            "columns": ["C"],
            "values": ["E", "F"],
            "dropna": False,
            "aggfunc": {"E": ["min", "max"], "F": ["mean", "sum"]},
        },
    )


@pytest.mark.parametrize("values", [["D"], ["E"], ["F"], ["E", "F"]])
def test_pivot_table_single_all_aggfuncs_dropna_and_null_data(
    df_data_with_nulls_2,
    values,
):
    expected_join_count = 10 if len(values) > 1 else 5
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data_with_nulls_2,
            {
                "index": ["A"],
                "columns": ["C"],
                "values": values,
                "dropna": False,
                "aggfunc": ["count", "sum", "min", "max", "mean"],
            },
        )


@sql_count_checker(query_count=1, join_count=4)
def test_pivot_table_single_nuance_aggfuncs_dropna_and_null_data(
    df_data_with_nulls_2,
):
    # pandas drops the 'D' in the result, but we will not model this way for pandas.
    #
    # 839.py:23: FutureWarning: pivot_table dropped a column because it failed to aggregate. This behavior is
    # deprecated and will raise in a future version of pandas. Select only the columns that can be aggregated.
    #
    snow_df = pd.DataFrame(df_data_with_nulls_2)
    snow_df = snow_df.pivot_table(
        index=["A"],
        columns=["C"],
        values=["D", "E"],
        dropna=False,
        aggfunc=["sum", "mean"],
    )

    native_df = native_pd.DataFrame(
        {
            ("sum", "D", "down"): [0.0, None],
            ("sum", "D", "up"): [None, 0.0],
            ("sum", "E", "down"): [2.0, None],
            ("sum", "E", "up"): [None, 1.0],
            ("mean", "D", "down"): [None, None],
            ("mean", "D", "up"): [None, None],
            ("mean", "E", "down"): [2.0, None],
            ("mean", "E", "up"): [None, 1.0],
        }
    )
    native_df.index = native_pd.Index(["bar", "foo"], dtype="object", name="A")
    native_df.columns.names = [None, None, "C"]

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df,
        comparator=assert_snowpark_pandas_equals_to_pandas_with_coerce_to_float64,
    )


@pytest.mark.skip("SNOW-857485 Do not handle non-str values for index/values/columns")
def test_pivot_table_with_multi_index_values_columns_index():
    pivot_table_test_helper(
        {
            ("A", "D"): ["foo", "bar"],
            ("B", "E"): ["shiny", "dull"],
            ("A", "F"): [1, 2],
            ("G", "C"): [None, 2],
        },
        {
            "index": [("A", "D")],
            "columns": [("B", "E")],
            "values": [("A", "F"), ("G", "C")],
            "dropna": False,
            "aggfunc": "min",
        },
    )
