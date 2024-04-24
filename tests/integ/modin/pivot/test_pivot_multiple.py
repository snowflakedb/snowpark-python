#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import numpy as np
import pytest

from tests.integ.modin.pivot.pivot_utils import pivot_table_test_helper
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_single_index_single_column_multiple_values(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": "B",
            "values": ["D", "E"],
        },
    )


@pytest.mark.parametrize("aggfunc", ["count", "sum", "min", "max", "mean"])
@sql_count_checker(query_count=1)
def test_pivot_table_single_index_multiple_column_single_value(df_data, aggfunc):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
            "aggfunc": aggfunc,
        },
    )


@pytest.mark.skip(
    "SNOW-853416: Some lingering encoding issues and also unsorted order does not match"
)
def test_pivot_table_single_index_single_column_multiple_encoded_values(
    df_encoded_data,
):
    pivot_table_test_helper(
        df_encoded_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
        },
    )


@sql_count_checker(query_count=1)
def test_pivot_table_single_index_single_column_multiple_small_encoded_values(
    df_small_encoded_data,
):
    pivot_table_test_helper(
        df_small_encoded_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
        },
    )


@pytest.mark.skip(
    "SNOW-853416: Some lingering encoding issues and also unsorted order does not match"
)
def test_pivot_table_single_index_single_column_multiple_encoded_values_with_sort(
    df_encoded_data,
):
    pivot_table_test_helper(
        df_encoded_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
            "sort": True,
        },
    )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_single_index_multiple_columns_multiple_values(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": ["D", "E"],
        },
    )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_multiple_index_multiple_columns_multiple_values(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "F"],
            "columns": ["B", "C"],
            "values": ["D", "E"],
        },
    )


@sql_count_checker(query_count=1, join_count=9)
def test_pivot_table_single_index_no_column_single_value_multiple_aggr_funcs(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": None,
            "values": None,
            "aggfunc": ["min", "max"],
        },
    )


@pytest.mark.skip(
    "SNOW-854301: Multi-Index replaces None with Nan causing test to fail"
)
@pytest.mark.parametrize(
    "values",
    [
        [None],
        ["D", None],
    ],
)
def test_pivot_table_multiple_index_multiple_columns_null_values(df_data, values):
    def update_columns_inline(df):
        df.columns = ["A", "B", "C", "D", None, "F"]
        return df

    pivot_table_test_helper(
        df_data,
        {
            "index": ["A"],
            "columns": ["B", "C"],
            "values": values,
        },
        preprocess_df=lambda df: update_columns_inline(df),
    )


# TODO (SNOW-854301): Needs support for MultiIndex.levels, fails because result.columns.levels[N] don't equal
# We use xfail to run so we can help code coverage
@pytest.mark.xfail
@pytest.mark.parametrize("values", [None, []])
@sql_count_checker(query_count=0)
def test_pivot_table_no_values_by_default(df_data, values):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "B", "C"],
            "columns": ["D", "E", "F"],
            "values": values,
            "aggfunc": "min",
        },
    )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_single_index_single_column_single_value_multiple_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {"index": "A", "columns": "C", "values": "D", "aggfunc": ["min", "max"]},
    )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_single_index_single_column_single_value_multiple_duplicate_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {"index": "A", "columns": "C", "values": "D", "aggfunc": ["min", "min"]},
    )


@sql_count_checker(query_count=1, join_count=3)
def test_pivot_table_single_index_multiple_column_multiple_values_multiple_duplicate_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": ["D", "E"],
            "aggfunc": ["sum", "sum"],
        },
    )


@pytest.mark.parametrize(
    "aggfunc, expected_join_count",
    [
        ("mean", 0),
        (["min", "max"], 1),
        ({"D": "count"}, 0),
        ({"D": ["sum"]}, 0),
        ({"D": ["min", "max"]}, 1),
    ],
)
def test_pivot_table_multiple_index_single_column_single_value_multiple_aggr_funcs(
    df_data,
    expected_join_count,
    aggfunc,
):
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data,
            {
                "index": ["A", "B"],
                "columns": "C",
                "values": "D",
                "aggfunc": aggfunc,
            },
        )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_multiple_index_multiple_column_single_value_multiple_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "F"],
            "columns": ["B", "C"],
            "values": "D",
            "aggfunc": ["min", "max"],
        },
    )


@pytest.mark.parametrize(
    "aggfunc, expected_join_count",
    [
        ({"D": [np.min, "count"], "E": [np.mean, np.sum]}, 3),
        ([np.min, np.max], 3),
        # Aggregation column E should be dropped since it is not mentioned here.
        ({"D": ["min", "max"]}, 1),
    ],
)
def test_pivot_table_multiple_index_multiple_column_multiple_values_multiple_aggr_funcs(
    df_data,
    aggfunc,
    expected_join_count,
):
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data,
            {
                "index": ["A", "F"],
                "columns": ["B", "C"],
                "values": ["D", "E"],
                "aggfunc": aggfunc,
            },
        )


@pytest.mark.parametrize("index_keys", ["A", ["A", "B"], "B"])
@sql_count_checker(query_count=1)
def test_pivot_table_with_initial_index_and_multiple_columns(df_data, index_keys):
    pivot_table_test_helper(
        df_data,
        pivot_table_kwargs={
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
        },
        preprocess_df=lambda df: df.set_index(keys=index_keys),
    )
