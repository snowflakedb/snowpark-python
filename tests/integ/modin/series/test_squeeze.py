#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.fixture(params=[0, "index", None])
def axis(request):
    """
    cache keyword to pass to to_datetime.
    """
    return request.param


@pytest.fixture(params=[1, "columns"])
def axis_negative(request):
    """
    cache keyword to pass to to_datetime.
    """
    return request.param


@sql_count_checker(query_count=2)
def test_noop(axis):
    s = pd.Series([1, 2, 3])
    assert_series_equal(s, s.squeeze(axis=axis))


@sql_count_checker(query_count=1)
def test_squeeze_to_scalar(axis):
    s = pd.Series([1])
    assert 1 == s.squeeze(axis=axis)


@sql_count_checker(query_count=0)
def test_axis_negative(axis_negative):
    eval_snowpark_pandas_result(
        pd.Series([1, 2, 3]),
        native_pd.Series([1, 2, 3]),
        lambda s: s.squeeze(axis=axis),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match=f"No axis named {axis} for object type Series",
    )
