#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter
from tests.integ.modin.utils import eval_snowpark_pandas_result


@pytest.fixture(params=[0, "index", 1, "columns", None])
def axis(request):
    """
    cache keyword to pass to to_datetime.
    """
    return request.param


def test_1d(axis):
    if axis == 1 or axis == "columns":
        expected_query_count = 1
    else:
        expected_query_count = 2

    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            pd.DataFrame([1, 2, 3]),
            native_pd.DataFrame([1, 2, 3]),
            lambda df: df.squeeze(axis=axis),
        )
    if axis is None:
        expected_query_count = 3
    elif axis in [0, "index"]:
        expected_query_count = 2
    else:
        expected_query_count = 1
    with SqlCounter(query_count=expected_query_count):
        eval_snowpark_pandas_result(
            pd.DataFrame({"a": [1], "b": [2], "c": [3]}),
            native_pd.DataFrame({"a": [1], "b": [2], "c": [3]}),
            lambda df: df.squeeze(axis=axis),
        )


def test_2d(axis):
    with SqlCounter(query_count=1 if axis in [1, "columns"] else 2):
        eval_snowpark_pandas_result(
            pd.DataFrame({"A": [1, 2, 3], "B": [2, 3, 4]}),
            native_pd.DataFrame({"A": [1, 2, 3], "B": [2, 3, 4]}),
            lambda df: df.squeeze(axis=axis),
        )


def test_scalar(axis):
    if axis == 1 or axis == "columns":
        expected_query_count = 1
    else:
        expected_query_count = 2
    with SqlCounter(query_count=expected_query_count):
        if axis is None:
            assert 1 == pd.DataFrame({"A": [1]}).squeeze()
        else:
            # still return a dataframe/series
            eval_snowpark_pandas_result(
                pd.DataFrame({"A": [1]}),
                native_pd.DataFrame({"A": [1]}),
                lambda df: df.squeeze(axis=axis),
            )
