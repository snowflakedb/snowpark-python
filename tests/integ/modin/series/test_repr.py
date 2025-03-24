#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.conftest import IRIS_DF
from tests.integ.utils.sql_counter import SqlCounter

SERIES_TO_TEST = [(IRIS_DF.iloc[:, i], 1, 0) for i in range(IRIS_DF.shape[1])] + [
    (native_pd.Series(), 1, 0),
    (native_pd.Series(list(range(10000))), 4, 0),
    (
        native_pd.Series(list(range(10000)), index=np.random.randint(-100, 100, 10000)),
        4,
        0,
    ),
    (
        native_pd.Series(
            [1, 2, 3], index=pd.MultiIndex.from_tuples([(1, 2), (3, 2), (4, 4)])
        ),
        1,
        0,
    ),
    (native_pd.Series(index=list(range(10000))), 4, 0),
]


@pytest.mark.parametrize(
    "native_series, expected_query_count, expected_join_count", SERIES_TO_TEST
)
def test_repr(native_series, expected_query_count, expected_join_count):
    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        snow_df = pd.Series(native_series)
        native_str = repr(native_series)
        snow_str = repr(snow_df)

        # because of type mismatch, replace in last line dtype for int8, int16, int32 with int64
        def replace_dtype_info(s):
            return (
                s.replace("dtype: int16", "dtype: int64")
                .replace("dtype: int8", "dtype: int64")
                .replace("dtype: int32", "dtype: int64")
            )

        assert native_str == replace_dtype_info(snow_str)
