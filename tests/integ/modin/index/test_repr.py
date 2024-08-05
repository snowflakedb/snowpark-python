#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter


@pytest.mark.parametrize(
    "native_index, query_count",
    [
        (native_pd.Index([]), 1),
        (native_pd.Index(["lone element"], name="A B C D  E"), 2),
        (native_pd.Index(list(range(250)), name="lots of numbers"), 2),
        (
            native_pd.Index(["a", "b", "c", "d o j", "e", "f", "g", "hello", "i", "j"]),
            2,
        ),
        (
            native_pd.Index(
                ["this is a very long string"] * 200, name="unnecessary name"
            ),
            2,
        ),
        (native_pd.Index(["abc", 12, 0.03, None]), 2),
        (native_pd.Index([e / 100 for e in range(-100, 100)], name="numbers"), 2),
    ],
)
def test_repr(native_index, query_count):
    with SqlCounter(query_count=query_count):
        snow_index = pd.Index(native_index)
        native_str = repr(native_index)
        snow_str = repr(snow_index)
        assert snow_str == native_str


def test_repr_high_qc():
    with SqlCounter(
        query_count=12,
        high_count_expected=True,
        high_count_reason="Index is very large",
    ):
        native_index = native_pd.Index(native_pd.Index(list(range(300))))
        snow_index = pd.Index(native_index)
        native_str = repr(native_index)
        snow_str = repr(snow_index)
        assert snow_str == native_str
