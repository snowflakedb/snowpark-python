#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_series_equal,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
def test_skew_basic():
    native_df = native_pd.DataFrame(
        np.array([[1, 2, 3], [2, 3, 4], [1, 2, 1], [2, 3, 3]]), columns=["A", "B", "C"]
    )
    snow_df = pd.DataFrame(native_df)
    assert_series_equal(
        snow_df.skew(),
        native_df.skew(),
        rtol=1.0e-5,
    )


@pytest.mark.parametrize(
    "data",
    [
        {
            "frame": {"A": [1, 2, 3], "B": [2, 3, 4], "C": [1, 2, 1], "D": [2, 3, 3]},
            "kwargs": {},
        },
        {
            "frame": {"A": [1, 2, 3], "B": [2, 3, 4], "C": [1, 2, 1], "D": [2, 3, 3]},
            "kwargs": {"axis": 0},
        },
        {
            "frame": {
                "A": [1, 2, 3],
                "B": [2, np.nan, 4],
                "C": [1, 2, np.nan],
                "D": [np.nan, np.nan, 3],
            },
            "kwargs": {"skipna": True},
        },
        {
            "frame": {
                "A": [1, 2, 3],
                "B": ["a", "b", "c"],
                "C": [1, 2, np.nan],
                "D": ["x", "y", "z"],
            },
            "kwargs": {"numeric_only": True},
        },
        {
            "frame": {
                "A": [1, 2, 3],
                "B": ["a", "b", "c"],
                "C": [1, 2, np.nan],
                "D": ["x", "y", "z"],
            },
            "kwargs": {"numeric_only": True, "skipna": True},
        },
        {
            "frame": {
                "A": [pd.Timedelta(1)],
            },
            "kwargs": {
                "numeric_only": True,
            },
        },
    ],
)
@sql_count_checker(query_count=1)
def test_skew(data):
    eval_snowpark_pandas_result(
        *create_test_dfs(data["frame"]),
        lambda df: df.skew(**data["kwargs"]),
        rtol=1.0e-5
    )


@pytest.mark.parametrize(
    "unsupported",
    [
        {
            "frame": {"A": [1, 2, 3], "B": [2, 3, 4], "C": [1, 2, 1], "D": [2, 3, 3]},
            "kwargs": {"axis": 1},
        },
        {
            "frame": {
                "A": [1, 2, 3],
                "B": ["a", "b", "c"],
                "C": [1, 2, np.nan],
                "D": ["x", "y", "z"],
            },
            "kwargs": {"numeric_only": False},
        },
        {
            "frame": {
                "A": [1, 2, 3],
            },
            "kwargs": {"numeric_only": False},
        },
        {
            "frame": {
                "A": [1, 2, 3],
            },
            "kwargs": {"level": 2},
        },
        {
            "frame": {
                "A": [pd.Timedelta(1)],
            },
            "kwargs": {
                "numeric_only": False,
            },
        },
    ],
)
@sql_count_checker(query_count=0)
def test_skew_unsupported(unsupported):
    native_df = native_pd.DataFrame(unsupported["frame"])
    snow_df = pd.DataFrame(native_df)
    try:
        snow_df.skew(**unsupported["kwargs"])
        raise AssertionError()
    except NotImplementedError:
        pass
    except TypeError:
        # numeric_only = False on non-numeric data will produce a
        # type error which matches pandas
        pass
