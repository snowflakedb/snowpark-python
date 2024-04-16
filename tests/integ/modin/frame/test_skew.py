#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.pandas as pd
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import assert_series_equal


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
                "B": [2, np.NaN, 4],
                "C": [1, 2, np.NaN],
                "D": [np.NaN, np.NaN, 3],
            },
            "kwargs": {"skipna": True},
        },
        {
            "frame": {
                "A": [1, 2, 3],
                "B": ["a", "b", "c"],
                "C": [1, 2, np.NaN],
                "D": ["x", "y", "z"],
            },
            "kwargs": {"numeric_only": True},
        },
        {
            "frame": {
                "A": [1, 2, 3],
                "B": ["a", "b", "c"],
                "C": [1, 2, np.NaN],
                "D": ["x", "y", "z"],
            },
            "kwargs": {"numeric_only": True, "skipna": True},
        },
    ],
)
@sql_count_checker(query_count=1)
def test_skew(data):
    native_df = native_pd.DataFrame(data["frame"])
    snow_df = pd.DataFrame(native_df)
    assert_series_equal(
        snow_df.skew(**data["kwargs"]),
        native_df.skew(**data["kwargs"]),
        rtol=1.0e-5,
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
                "C": [1, 2, np.NaN],
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
