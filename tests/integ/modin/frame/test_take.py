#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter


@pytest.mark.parametrize("test_multiindex", [False, True])
@pytest.mark.parametrize("dtype", ["float", "timedelta64[ns]"])
def test_df_take(float_native_df, test_multiindex, dtype):
    float_native_df = float_native_df.astype(dtype)

    def _test_take(native_df):
        df = pd.DataFrame(native_df)

        # homogeneous
        order = [3, 1, 2, 0]
        with SqlCounter(query_count=1, join_count=2):
            eval_snowpark_pandas_result(
                df, native_df, lambda df: df.take(order, axis=0)
            )

        # axis = 1
        with SqlCounter(query_count=1, join_count=0):
            eval_snowpark_pandas_result(
                df, native_df, lambda df: df.take(order, axis=1)
            )

        # negative indices
        order = [2, 1, -1]
        with SqlCounter(query_count=1, join_count=2):
            eval_snowpark_pandas_result(df, native_df, lambda df: df.take(order))
        # slice
        order = slice(1, 3)
        with SqlCounter(query_count=1, join_count=0):
            # assert_frame_equal(result, expected)
            eval_snowpark_pandas_result(df, native_df, lambda df: df.take(order))

        # Out-of-bounds testing - valid because .iloc is used in backend.
        order = [3, 1, 2, 30]
        result = df.take(order, axis=0)
        expected = df.iloc[order]
        with SqlCounter(query_count=2, join_count=4):
            assert_frame_equal(result, expected)

    if test_multiindex:
        mi = pd.MultiIndex.from_arrays(
            [
                np.random.rand(len(float_native_df)),
                np.random.rand(len(float_native_df)),
            ],
            names=["mi1", "mi2"],
        )

        float_native_df = float_native_df.set_index(mi)

    _test_take(float_native_df)
