#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import sql_count_checker


@pytest.fixture(scope="function")
def snow_and_native_df():
    data = [[1, 2], [3, 4]]
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    return snow_df, native_df


@pytest.fixture(scope="function")
def snow_and_native_df_nan():
    data = [[1, 2], [3, np.nan]]
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    return snow_df, native_df


# TODO: SNOW-1056369 : Implement binary operation __xor__
@pytest.mark.parametrize("func", [lambda df: df.__xor__([-1, 0]), lambda df: df ^ df])
@sql_count_checker(query_count=0)
def test_binary_op_xor(snow_and_native_df, func):
    snow_df, _ = snow_and_native_df
    msg = "Snowpark pandas doesn't yet support '__xor__' binary operation"
    with pytest.raises(NotImplementedError, match=msg):
        func(snow_df)


@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.dot(df),
        lambda df: df[0].dot(df[1]),
        lambda df: df[0] @ df,
        lambda df: df @ df[1],
    ],
)
@sql_count_checker(query_count=0)
def test_binary_op_dot(snow_and_native_df, func):
    snow_df, _ = snow_and_native_df
    msg = "Snowpark pandas doesn't yet support 'dot' binary operation"
    with pytest.raises(NotImplementedError, match=msg):
        func(snow_df)
