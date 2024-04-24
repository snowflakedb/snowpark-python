#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.pivot.pivot_utils import pivot_table_test_helper
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("columns", ["C", ["B", "C"]])
@pytest.mark.parametrize("fill_value", [None, 99.99])
def test_pivot_table_single_with_dropna_options(
    df_data_with_nulls, dropna, columns, fill_value
):
    expected_join_count = 2 if not dropna else 1
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data_with_nulls,
            {
                "index": "A",
                "columns": columns,
                "values": "D",
                "dropna": dropna,
                "fill_value": fill_value,
                "margins": True,
            },
        )


@pytest.mark.parametrize(
    "aggfunc",
    [
        "count",
        "sum",
        ["min", "max"],
        {"E": "mean", "D": "count"},
        "mean",
    ],
)
@pytest.mark.parametrize("dropna", [True, False])
def test_pivot_table_multiple_columns_values_with_margins(
    df_data,
    aggfunc,
    dropna,
):
    expected_join_count = 2
    if isinstance(aggfunc, list):
        expected_join_count += 2
    if not dropna:
        expected_join_count += expected_join_count
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data,
            {
                "index": ["A"],
                "columns": ["B", "C"],
                "values": ["D", "E"],
                "aggfunc": aggfunc,
                "dropna": dropna,
                "margins": True,
            },
        )


@pytest.mark.parametrize(
    "fill_value",
    [
        None,
        # pandas 1.5 had incorrect behavior where the `mean` margin was being set to the fill_value.
        # Snowpark pandas likewise should not be applying the fill value, but does so anyway
        pytest.param(
            99.99,
            marks=pytest.mark.xfail(
                strict=True, reason="SNOW-1201908: fill_value should not affect margin"
            ),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=9, union_count=1)
def test_pivot_table_multiple_pivot_values_null_data_with_margins(
    df_data_with_nulls, fill_value
):
    pivot_table_test_helper(
        df_data_with_nulls,
        {
            "index": ["A", "B"],
            "columns": "C",
            "values": "F",
            "aggfunc": ["count", "sum", "mean"],
            "dropna": False,
            "fill_value": fill_value,
            "margins": True,
            "margins_name": "TOTAL",
        },
    )


@pytest.mark.parametrize(
    "fill_value",
    [
        None,
        # pandas 1.5 had incorrect behavior where the `mean` margin was being set to the fill_value.
        # Snowpark pandas likewise should not be applying the fill value, but does so anyway
        pytest.param(
            99.99,
            marks=pytest.mark.xfail(
                strict=True, reason="SNOW-1201908: fill_value should not affect margin"
            ),
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=6, union_count=1)
def test_pivot_table_multiple_pivot_values_null_data_with_margins_nan_blocked(
    df_data_with_nulls, fill_value
):
    pivot_table_test_helper(
        df_data_with_nulls,
        {
            "index": ["A", "B"],
            "columns": "C",
            "values": "F",
            "aggfunc": ["min", "max"],
            "dropna": False,
            "fill_value": fill_value,
            "margins": True,
            "margins_name": "TOTAL",
        },
    )


@sql_count_checker(query_count=1, join_count=12, union_count=1)
def test_pivot_table_mixed_index_types_with_margins(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "F"],
            "columns": ["B", "C"],
            "values": ["D", "E"],
            "aggfunc": ["min", "max"],
            "dropna": False,
            "margins": True,
        },
    )


@sql_count_checker(query_count=1, join_count=8, union_count=1)
def test_pivot_table_single_aggfuncs_dropna_and_null_data_pandas_drops_columns(
    df_data_with_nulls_2,
):
    # pandas 1.5 previously dropped columns with all NULL values, but no longer does this
    # in 2.x.
    pivot_table_test_helper(
        df_data_with_nulls_2,
        {
            "index": ["A"],
            "columns": ["B"],
            "values": ["D", "E"],
            "aggfunc": ["sum", "mean"],
            "dropna": False,
            "margins": True,
        },
    )


@sql_count_checker(query_count=0)
def test_pivot_table_unsupported_dropna_with_expanded_aggregation_margins_unsupported(
    df_data,
):
    snow_df = pd.DataFrame(df_data)

    with pytest.raises(
        ValueError, match="Margins not supported if list of aggregation functions"
    ):
        snow_df.pivot_table(
            index="A",
            columns="C",
            values=["E", "F"],
            aggfunc={"E": ["min"], "F": "max"},
            margins=True,
        )
