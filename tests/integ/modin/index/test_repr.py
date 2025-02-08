#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import SqlCounter


@pytest.mark.parametrize(
    "native_index",
    [
        native_pd.Index([]),
        native_pd.Index(["lone element"], name="A B C D  E"),
        native_pd.Index(list(range(250)), name="lots of numbers"),
        native_pd.Index(
            [
                "a",
                "b",
                "c",
                "d o j",
                "e",
                "f",
                "g",
                "hello",
                "i",
                "j",
                "can't, shan't, won't",
                "N\nN\n\t\n",
            ]
        ),
        native_pd.Index(["this is a very long string"] * 200, name="unnecessary name"),
        native_pd.Index(["abc", 12, 0.03, None, "..."]),
        native_pd.Index([e / 100 for e in range(-100, 100)], name="numbers"),
        native_pd.Index(["A" * 80] * 20, name="B" * 100),
        native_pd.Index(["A" * 80] * 250, name="B" * 62),
    ],
)
def test_repr(native_index):
    with SqlCounter(query_count=1):
        snow_index = pd.Index(native_index)
        native_str = repr(native_index)
        snow_str = repr(snow_index)
        assert snow_str == native_str


def test_repr_high_qc():
    with SqlCounter(query_count=4):
        native_index = native_pd.Index(native_pd.Index(list(range(300))))
        snow_index = pd.Index(native_index)
        native_str = repr(native_index)
        snow_str = repr(snow_index)
        assert snow_str == native_str
