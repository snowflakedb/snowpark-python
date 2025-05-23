#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin.extensions.snow_partition_iterator import (
    PARTITION_SIZE,
)
from tests.integ.modin.utils import (
    assert_values_equal,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import RUNNING_ON_GH

# To generate seeded random data.
rng = np.random.default_rng(12345)


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
            assert_values_equal(snow_index, pandas_index)
            assert_values_equal(snow_value, pandas_value)


@pytest.mark.parametrize(
    "series",
    [
        native_pd.Series(
            ["bear", "bear", "marsupial"],
            index=["panda", "polar", "koala"],
        ),
        native_pd.Series(data=["a"]),
        native_pd.Series(),
        native_pd.Series(index=["a"]),
        native_pd.Series(native_pd.timedelta_range(10, periods=10)),
        native_pd.Series(index=["a", "a", "a"]),
        native_pd.Series(data={"a": 1, "b": 2, "c": 3}),
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


@pytest.mark.skipif(RUNNING_ON_GH, reason="Slow test")
def test_items_large_series():
    size = PARTITION_SIZE * 2 + 1
    data = rng.integers(low=-1500, high=1500, size=size)
    native_series = native_pd.Series(data)
    snow_series = pd.Series(native_series)
    query_count = (np.floor(size / PARTITION_SIZE) + 1) * 4
    with SqlCounter(
        query_count=query_count,
        join_count=0,
        high_count_expected=True,
        high_count_reason="Series spans multiple iteration partitions, each of which requires 4 queries",
    ):
        eval_snowpark_pandas_result(
            snow_series,
            native_series,
            lambda series: series.items(),
            comparator=assert_items_results_equal,
        )
