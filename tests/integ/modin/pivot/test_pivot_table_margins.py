#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.pivot.pivot_utils import pivot_table_test_helper
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize("index", [None, "A"], ids=["no_index", "single_index"])
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("columns", ["C", ["B", "C"]])
@pytest.mark.parametrize("fill_value", [None, 99.99])
def test_pivot_table_single_with_dropna_options(
    df_data_with_nulls, index, dropna, columns, fill_value
):
    expected_join_count = 2 if not dropna else 1
    if not dropna and index is None:
        expected_join_count += 1
    if len(columns) > 1 and index is None:
        pytest.xfail(
            reason="SNOW-1435365 - pandas computes values differently than us: https://github.com/pandas-dev/pandas/issues/58722."
        )
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data_with_nulls,
            {
                "index": index,
                "columns": columns,
                "values": "D",
                "dropna": dropna,
                "fill_value": fill_value,
                "margins": True,
            },
        )


# Not marking as strict since the following test cases pass:
# [None-C-True-no_index]
# [None-columns1-True-no_index]
# [None-C-False-no_index]
# [None-columns1-False-no_index]
@pytest.mark.xfail(
    reason="SNOW-1435365 - we do not support margins=True, with no index and aggfunc as a dictionary."
)
@pytest.mark.parametrize("index", [None, "A"], ids=["no_index", "single_index"])
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("columns", ["C", ["B", "C"]])
@pytest.mark.parametrize("fill_value", [None, 99.99])
def test_pivot_table_single_with_dropna_options_multiple_aggr_funcs(
    df_data_with_nulls, index, dropna, columns, fill_value
):
    expected_join_count = 2 if not dropna else 1
    if not dropna and index is None:
        expected_join_count += 1
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data_with_nulls,
            {
                "index": index,
                "columns": columns,
                "values": ["D", "E"],
                "dropna": dropna,
                "fill_value": fill_value,
                "margins": True,
                "aggfunc": {"D": "sum", "E": "max"},
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
        expected_join_count += 1
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
    "index",
    [
        pytest.param(
            None,
            marks=pytest.mark.xfail(
                strict=True,
                reason="SNOW-1435365 - pandas computes values differently than us: https://github.com/pandas-dev/pandas/issues/58722.",
            ),
        ),
        ["A", "B"],
    ],
    ids=["no_index", "multiple_index"],
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
@sql_count_checker(query_count=1, join_count=5, union_count=1)
def test_pivot_table_multiple_pivot_values_null_data_with_margins(
    df_data_with_nulls, index, fill_value
):
    pivot_table_test_helper(
        df_data_with_nulls,
        {
            "index": index,
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
    "index", [None, ["A", "B"]], ids=["no_index", "multiple_index"]
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
def test_pivot_table_multiple_pivot_values_null_data_with_margins_nan_blocked(
    df_data_with_nulls, index, fill_value
):
    join_count = 5 if index is None and fill_value is None else 4
    union_count = 0 if index is None and fill_value is None else 1
    with SqlCounter(query_count=1, join_count=join_count, union_count=union_count):
        pivot_table_test_helper(
            df_data_with_nulls,
            {
                "index": index,
                "columns": "C",
                "values": "F",
                "aggfunc": ["min", "max"],
                "dropna": False,
                "fill_value": fill_value,
                "margins": True,
                "margins_name": "TOTAL",
            },
        )


@sql_count_checker(query_count=1, join_count=6, union_count=1)
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


@sql_count_checker(query_count=1, join_count=5, union_count=1)
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


@pytest.mark.parametrize(
    "columns", [["B"], ["B", "C"]], ids=["single_column", "multiple_columns"]
)
class TestPivotTableMarginsNoIndexFewerPivotValues:
    @sql_count_checker(query_count=1, join_count=1)
    def test_single_value_single_aggfunc(self, columns, df_data):
        pivot_table_test_helper(
            df_data,
            {
                "columns": columns,
                "values": ["D"],
                "aggfunc": "sum",
                "dropna": True,
                "margins": True,
            },
        )

    def test_multiple_value_single_aggfunc(self, columns, df_data):
        with SqlCounter(
            query_count=1, join_count=1, union_count=2 if len(columns) > 1 else 1
        ):
            pivot_table_test_helper(
                df_data,
                {
                    "columns": columns,
                    "values": ["D", "E"],
                    "aggfunc": ["sum"],
                    "dropna": True,
                    "margins": True,
                },
            )

    @sql_count_checker(query_count=1, join_count=3)
    def test_single_value_multiple_aggfunc(self, columns, df_data):
        pivot_table_test_helper(
            df_data,
            {
                "columns": columns,
                "values": ["D"],
                "aggfunc": ["sum", "min"],
                "dropna": True,
                "margins": True,
            },
        )

    @sql_count_checker(query_count=1, join_count=5, union_count=2)
    def test_multiple_value_multiple_aggfunc(self, columns, df_data):
        pivot_table_test_helper(
            df_data,
            {
                "columns": columns,
                "values": ["D", "E"],
                "aggfunc": ["sum", "min"],
                "dropna": True,
                "margins": True,
            },
        )


@sql_count_checker(query_count=1)
def test_pivot_table_empty_table_with_index_margins():
    # Cannot use pivot_table_test_helper since that checks the inferred types
    # on the resulting DataFrames' columns (which are empty), and the inferred type
    # on our DataFrame's columns is empty, while pandas has type floating.
    import pandas as native_pd

    native_df = native_pd.DataFrame({"A": [], "B": [], "C": [], "D": []})
    snow_df = pd.DataFrame(native_df)
    pivot_kwargs = {
        "index": ["A", "B"],
        "columns": "C",
        "values": "D",
        "aggfunc": "count",
        "margins": True,
    }

    snow_result = snow_df.pivot_table(**pivot_kwargs).to_pandas()
    native_result = native_df.pivot_table(**pivot_kwargs)

    assert native_result.empty == snow_result.empty and (native_result.empty is True)
    assert list(native_result.columns) == list(snow_result.columns)
    assert list(native_result.index) == list(snow_result.index)


@pytest.mark.parametrize(
    "columns", [["B"], ["B", "C"]], ids=["single_column", "multiple_columns"]
)
@pytest.mark.parametrize(
    "named_columns", [pytest.param(True, marks=pytest.mark.xfail()), False]
)
class TestPivotTableMarginsNoIndexMorePivotValues:
    @sql_count_checker(query_count=1, join_count=1)
    def test_single_value_single_aggfunc(
        self, columns, named_columns, df_data_more_pivot_values
    ):
        pivot_table_test_helper(
            df_data_more_pivot_values,
            {
                "columns": columns,
                "values": ["D"],
                "aggfunc": ["sum"],
                "dropna": True,
                "margins": True,
            },
            named_columns=named_columns,
        )

    def test_multiple_value_single_aggfunc(
        self, columns, named_columns, df_data_more_pivot_values
    ):
        with SqlCounter(
            query_count=1, join_count=1, union_count=2 if len(columns) > 1 else 1
        ):
            pivot_table_test_helper(
                df_data_more_pivot_values,
                {
                    "columns": columns,
                    "values": ["D", "E"],
                    "aggfunc": "sum",
                    "dropna": True,
                    "margins": True,
                },
                named_columns=named_columns,
            )

    @sql_count_checker(query_count=1, join_count=3)
    def test_single_value_multiple_aggfunc(
        self, columns, named_columns, df_data_more_pivot_values
    ):
        pivot_table_test_helper(
            df_data_more_pivot_values,
            {
                "columns": columns,
                "values": ["D"],
                "aggfunc": ["sum", "min"],
                "dropna": True,
                "margins": True,
            },
            named_columns=named_columns,
        )

    @sql_count_checker(query_count=1, join_count=5, union_count=2)
    def test_multiple_value_multiple_aggfunc(
        self, columns, named_columns, df_data_more_pivot_values
    ):
        pivot_table_test_helper(
            df_data_more_pivot_values,
            {
                "columns": columns,
                "values": ["D", "E"],
                "aggfunc": ["sum", "min"],
                "dropna": True,
                "margins": True,
            },
            named_columns=named_columns,
        )
