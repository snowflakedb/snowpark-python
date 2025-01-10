#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.pivot.pivot_utils import pivot_table_test_helper
from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize("fill_value", [None, 999, 3.14519])
@pytest.mark.parametrize("columns", ["C", ["B", "C"]])
@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_single_with_fill_value(df_data_with_nulls, fill_value, columns):
    pivot_table_test_helper(
        df_data_with_nulls,
        {
            "index": "A",
            "columns": columns,
            "values": "D",
            "dropna": False,
            "fill_value": fill_value,
        },
    )


# TODO (SNOW-862347): Type incompatible fill_value supported by pandas but not snowflake
@sql_count_checker(query_count=0)
def test_pivot_table_single_with_dropna_type_incompatible_fill_value(
    df_data_with_nulls,
):
    snow_df = pd.DataFrame(df_data_with_nulls)
    with pytest.raises(
        SnowparkSQLException, match="Numeric value 'XYZ' is not recognized"
    ):
        snow_df.pivot_table(
            index="A",
            columns="C",
            values="D",
            dropna=False,
            fill_value="XYZ",
        ).to_pandas()


@sql_count_checker(query_count=1, join_count=5)
def test_pivot_table_multiple_values_fill_value_nonnull_data(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A"],
            "columns": ["B", "C"],
            "values": ["D", "E", "F"],
            "dropna": False,
            "fill_value": 999.99,
            "aggfunc": {"D": ["count", "max", "min"], "E": ["mean", "sum"]},
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
def test_pivot_table_multiple_pivot_values_fill_value_null_data(
    df_data_with_nulls, aggfunc, expected_join_count
):
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data_with_nulls,
            {
                "index": ["A"],
                "columns": ["B", "C"],
                "values": ["E", "F"],
                "dropna": False,
                "fill_value": 999.99,
                "aggfunc": aggfunc,
            },
        )


@sql_count_checker(query_count=1, join_count=5)
def test_pivot_table_multiple_index_single_pivot_values_dfill_value_null_data(
    df_data_with_nulls_2,
):
    pivot_table_test_helper(
        df_data_with_nulls_2,
        {
            "index": ["A", "B"],
            "columns": ["C"],
            "values": ["E", "F"],
            "dropna": False,
            "fill_value": 999.99,
            "aggfunc": {"E": ["min", "max"], "F": ["mean", "sum"]},
        },
    )


@pytest.mark.parametrize(
    "values, expected_join_count",
    [(["D"], 5), (["E"], 5), (["F"], 5), (["E", "F"], 10)],
)
def test_pivot_table_single_all_aggfuncs_fill_value_and_null_data(
    df_data_with_nulls_2,
    values,
    expected_join_count,
):
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data_with_nulls_2,
            {
                "index": ["A"],
                "columns": ["C"],
                "values": values,
                "dropna": False,
                "fill_value": 999.99,
                "aggfunc": ["count", "sum", "min", "max", "mean"],
            },
        )


@sql_count_checker(query_count=1, join_count=4)
def test_pivot_table_single_nuance_aggfuncs_fill_value_and_null_data(
    df_data_with_nulls_2,
):
    # pandas drops the 'D' in the result, but we will not model this way for pandas.
    #
    # 839.py:23: FutureWarning: pivot_table dropped a column because it failed to aggregate. This behavior is
    # deprecated and will raise in a future version of pandas. Select only the columns that can be aggregated.
    #
    snow_df = pd.DataFrame(df_data_with_nulls_2)

    FILL_VALUE = 999.99

    TEST_INDEX = "A"
    TEST_COLUMNS = ["C"]
    TEST_AGGFUNC = ["sum", "mean"]
    TEST_VALUES = ["D", "E"]
    TEST_COLUMN_VALUES = ["down", "up"]

    snow_result_df = snow_df.pivot_table(
        index=TEST_INDEX,
        columns=TEST_COLUMNS,
        values=TEST_VALUES,
        dropna=False,
        fill_value=FILL_VALUE,
        aggfunc=TEST_AGGFUNC,
    )

    expected_df = native_pd.DataFrame(
        [
            [0.0, FILL_VALUE, 2.0, FILL_VALUE, FILL_VALUE, FILL_VALUE, 2.0, FILL_VALUE],
            [FILL_VALUE, 0.0, FILL_VALUE, 1.0, FILL_VALUE, FILL_VALUE, FILL_VALUE, 1.0],
        ],
        columns=pd.MultiIndex.from_product(
            [TEST_AGGFUNC, TEST_VALUES, TEST_COLUMN_VALUES],
            names=[None, None] + TEST_COLUMNS,
        ),
        index=native_pd.Index(["bar", "foo"], name=TEST_INDEX),
    )

    assert_frame_equal(
        snow_result_df.astype("float64"),
        expected_df,
        rtol=1.0e-5,
    )
