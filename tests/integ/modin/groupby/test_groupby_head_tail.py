#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.groupby.conftest import multiindex_data
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker

# Seeded random number generator.
rng = np.random.default_rng(1234)


@pytest.mark.parametrize("op_type", ["head", "tail"])
@pytest.mark.parametrize("n", [0, 1, 5, 10, -3, -20])
@pytest.mark.parametrize("dropna", [True, False])
@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@pytest.mark.parametrize("group_keys", [True, False])
class TestDataFrameGroupByHeadTail:
    """
    This is a class to test DataFrameGroupBy.head and DataFrameGroupBy.tail.
    """

    small_df_data = [
        ["lion", 78, 50, 50, 50],
        ["tiger", -35, 12, -378, 1246],
        ["giraffe", 54, -9, 67, -256],
        ["hippopotamus", 378, -537, -47, -789],
        ["tiger", 89, 2, 256, 246],
        ["tiger", -325, 2, 2, 5],
        ["tiger", 367, -367, 3, -6],
        ["giraffe", 25, 6, 312, 6],
        ["lion", -5, -5, -3, -4],
        ["lion", 15, 77, 2, 12],
        ["giraffe", 100, 200, 300, 400],
        ["hippopotamus", -100, -300, -600, -200],
        ["rhino", 26, 2, -45, 14],
        ["rhino", -7, 63, 257, -257],
        ["lion", 1, 2, 3, 4],
        ["giraffe", -5, -6, -7, 8],
        ["lion", 1234, 456, 78, 9],
    ]

    @sql_count_checker(query_count=1)
    def test_df_groupby_head_tail(self, op_type, n, dropna, as_index, sort, group_keys):
        """
        Test DataFrameGroupBy.head and DataFrameGroupBy.tail with a small df with no NA values.
        """
        eval_snowpark_pandas_result(
            *create_test_dfs(
                data=self.small_df_data,
                columns=("species", "speed", "age", "weight", "height"),
                index=list("ijklmnopabhqcdefg"),
            ),
            lambda df: df.groupby(
                by="species",
                dropna=dropna,
                as_index=as_index,
                sort=sort,
                group_keys=group_keys,
            ).__getattribute__(op_type)(n),
            check_index_type=False,
        )

    @sql_count_checker(query_count=4)
    def test_df_groupby_head_tail_large_data(
        self, op_type, n, dropna, as_index, sort, group_keys, large_df_with_na_values
    ):
        """
        Test DataFrameGroupBy.head and DataFrameGroupBy.tail with a large df with NA values
        in both grouping columns: "color" and "random3".

        Testing with a large set of values (100 rows) to ensure that we have enough data in each
        group created when grouping by two columns: "color" and "random3".

        Note that only 1 out of the 6 queries run is used for the DataFrameGroupBy.head/tail logic;
        the 5 other queries are used to set up and tearing down the DataFrame this is being performed on:
        1. create or replace temporary table TABLE_A.
        2. select data from this table (TABLE_A).
        3. drop the temporary table TABLE_A.
        <enter DataFrameGroupBy.head/tail operation>
        4. create a new temporary table to work on, TABLE_B.
        5. select the required entries from the groups using a window function.
        6. drop the temporary table TABLE_B.
        """
        eval_snowpark_pandas_result(
            *large_df_with_na_values,
            lambda df: df.groupby(
                by=["color", "random3"],
                dropna=dropna,
                as_index=as_index,
                sort=sort,
                group_keys=group_keys,
            ).__getattribute__(op_type)(n),
            check_index_type=False,
        )

    @pytest.mark.parametrize("level", [0, 1])
    @sql_count_checker(query_count=1)
    def test_df_groupby_head_tail_with_multiindex_df(
        self, op_type, level, n, dropna, as_index, sort, group_keys
    ):
        """
        Test DataFrameGroupBy.head and DataFrameGroupBy.tail with a MultiIndex DataFrame.

        Here, the MultiIndex DataFrames are grouped by `level` and not `by`.
        """
        snowpark_pandas_df, native_pandas_df = create_test_dfs(multiindex_data)
        snowpark_pandas_df = snowpark_pandas_df.set_index(["A", "B"])
        native_pandas_df = native_pandas_df.set_index(["A", "B"])

        eval_snowpark_pandas_result(
            snowpark_pandas_df,
            native_pandas_df,
            lambda df: df.groupby(
                level=level,
                dropna=dropna,
                as_index=as_index,
                sort=sort,
                group_keys=group_keys,
            ).__getattribute__(op_type)(n),
            check_index_type=False,
        )


@pytest.mark.parametrize("op_type", ["head", "tail"])
@pytest.mark.parametrize("n", [2.5, "3"])
@sql_count_checker(query_count=0)
def test_df_groupby_head_tail_non_integer_n_negative(
    op_type, n, basic_snowpark_pandas_df_with_missing_values
):
    """
    Test that DataFrameGroupBy.head/tail only works with integer n values.
    """
    df = basic_snowpark_pandas_df_with_missing_values
    with pytest.raises(TypeError, match="n must be an integer value."):
        df.groupby("col1").__getattribute__(op_type)(n)


@pytest.mark.parametrize("n", [0, 1, -2])
@pytest.mark.parametrize("op_type", ["head", "tail"])
@sql_count_checker(query_count=1)
def test_df_groupby_head_tail_df_with_duplicate_columns(op_type, n):
    data = [
        [None, 1, 1, 0, None, 0],
        [4, 5, None, 7, 4, 5],
        [3.1, 8.0, 12, 10, 4, np.nan],
        [17, 3, 16, 15, None, None],
        [None, 3, -1, 3, -2, None],
    ]
    eval_snowpark_pandas_result(
        *create_test_dfs(
            data, columns=["col1", "col2", "col2", "col5", "col5", "col6"]
        ),
        lambda df: df.groupby(by="col1").__getattribute__(op_type)(n),
        check_index_type=False,
    )


@sql_count_checker(query_count=1)
def test_df_groupby_last_chained_pivot_table_SNOW_1628228():
    native_df = native_pd.DataFrame(
        data=native_pd.Series(
            native_pd.date_range(start="2024-01-01", freq="min", periods=10)
        ).values,
        columns=["A"],
    )
    native_df["B"] = 2
    native_df["C"] = 3
    snow_df = pd.DataFrame(native_df)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.pivot_table(index="A", values="C", aggfunc="mean")
        .groupby("A")
        .last(),
    )


@pytest.mark.parametrize("agg_func", ["head", "tail"])
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
