#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pandas as native_pd
import pytest

from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter


def assert_items_results_equal(snow_result, pandas_result) -> None:
    snow_list = list(snow_result)
    pandas_list = list(pandas_result)
    assert len(snow_list) == len(pandas_list), "lengths of items are not equal."
    if len(snow_list) == 0:
        # Expect no queries if there are no columns.
        with SqlCounter(query_count=0):
            return
    for ((snow_label, snow_column), (pandas_label, pandas_column)) in zip(
        snow_list, pandas_list
    ):
        assert snow_label == pandas_label
        # Execute one query to materialize each column.
        with SqlCounter(query_count=1):
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_column, pandas_column
            )


@pytest.mark.parametrize(
    "dataframe",
    [
        native_pd.DataFrame(
            {
                "species": ["bear", "bear", "marsupial"],
                "population": [1864, 22000, 80000],
            },
            index=["panda", "polar", "koala"],
        ),
        native_pd.DataFrame(
            {
                (0, "species"): ["bear", "bear", "marsupial"],
                (0, "population"): [1864, 22000, 80000],
            },
            index=["panda", "polar", "koala"],
        ),
        native_pd.DataFrame(index=["a"]),
        native_pd.DataFrame(columns=["a"]),
        native_pd.DataFrame({"ts": native_pd.timedelta_range(10, periods=10)}),
    ],
)
def test_items(dataframe):
    eval_snowpark_pandas_result(
        *create_test_dfs(dataframe),
        lambda df: df.items(),
        comparator=assert_items_results_equal,
    )
