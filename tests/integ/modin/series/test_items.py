#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.utils import create_test_series, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


def assert_items_results_equal(snow_result, pandas_result) -> None:
    snow_list = list(snow_result)
    pandas_list = list(pandas_result)
    assert len(snow_list) == len(pandas_list), "lengths of items are not equal."
    if len(snow_list) == 0:
        # Expect no queries if there are no columns.
        with SqlCounter(query_count=0):
            return
    for ((snow_index, snow_value), (pandas_index, pandas_value)) in zip(
        snow_list, pandas_list
    ):
        with SqlCounter(query_count=0):
            assert snow_index == pandas_index
            # Workaround using NumPy since nan != nan
            if type(snow_value).__module__ == np.__name__:
                assert np.isnan(snow_value) and np.isnan(pandas_value)
            else:
                assert snow_value == pandas_value


@pytest.mark.parametrize(
    "series",
    [
        native_pd.Series(
            ["bear", "bear", "marsupial"],
            index=["panda", "polar", "koala"],
        ),
        native_pd.Series(data=["a"]),
        native_pd.Series(index=["a"]),
        native_pd.Series(native_pd.timedelta_range(10, periods=10)),
    ],
)
@sql_count_checker(query_count=1)
def test_items(series):
    # 1 row count query is issued when building the SnowparkPandasRowPartitionIterator
    eval_snowpark_pandas_result(
        *create_test_series(series),
        lambda series: series.items(),
        comparator=assert_items_results_equal,
    )
