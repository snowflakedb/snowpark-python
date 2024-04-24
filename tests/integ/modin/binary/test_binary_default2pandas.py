#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.conftest import running_on_public_ci
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    eval_snowpark_pandas_result,
)


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
@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.parametrize("func", [lambda df: df.__xor__([-1, 0])])
@sql_count_checker(query_count=8, fallback_count=1, sproc_count=1)
def test_binary_op_on_list_like_value(snow_and_native_df, func):
    eval_snowpark_pandas_result(*snow_and_native_df, func)


@pytest.mark.xfail(
    reason="SNOW-1336091: Snowpark pandas cannot run in sprocs until modin 0.28.1 is available in conda",
    strict=True,
    raises=RuntimeError,
)
@pytest.mark.skipif(running_on_public_ci(), reason="slow fallback test")
@sql_count_checker(query_count=10, fallback_count=1, sproc_count=1)
def test_binary_xor_on_df(snow_and_native_df):
    eval_snowpark_pandas_result(*snow_and_native_df, lambda df: df ^ df)


@pytest.mark.skip(
    reason="TODO: SNOW-896220 support dot. It raises NotImplementedError today"
)
@pytest.mark.parametrize(
    "func",
    [
        lambda df: df.dot(df),
        lambda df: df[0].dot(df[1]),
        lambda df: df[0] @ df,
        lambda df: df @ df[1],
    ],
)
def test_binary_op_dot(snow_and_native_df, func):
    def compare(snow_result, pd_result, **kwargs):
        if not isinstance(
            pd_result, (native_pd.DataFrame, native_pd.Series)
        ) and not isinstance(snow_result, (pd.DataFrame, pd.Series)):
            assert pd_result == snow_result
        else:
            assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
                snow_result, pd_result, **kwargs
            )

    eval_snowpark_pandas_result(*snow_and_native_df, func, comparator=compare)
