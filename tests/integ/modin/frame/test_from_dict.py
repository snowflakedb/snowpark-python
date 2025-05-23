#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd

from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
def test_from_dict_basic():
    data = {"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]}

    assert_frame_equal(
        pd.DataFrame.from_dict(data),
        native_pd.DataFrame.from_dict(data),
        check_dtype=False,
    )


@sql_count_checker(query_count=1)
def test_from_dict_nested_dict():
    data = {"col_1": {"a": 1, "b": 2}, "col_2": {"c": 3, "d": 4}}

    assert_frame_equal(
        pd.DataFrame.from_dict(data),
        native_pd.DataFrame.from_dict(data),
        check_dtype=False,
    )


@sql_count_checker(query_count=1)
def test_from_dict_orient_index():
    data = {"row_1": [3, 2, 1, 0], "row_2": ["a", "b", "c", "d"]}

    assert_frame_equal(
        pd.DataFrame.from_dict(data, orient="index"),
        native_pd.DataFrame.from_dict(data, orient="index"),
        check_dtype=False,
    )


@sql_count_checker(query_count=1)
def test_from_dict_orient_index_columns():
    data = {"row_1": [3, 2, 1, 0], "row_2": ["a", "b", "c", "d"]}

    assert_frame_equal(
        pd.DataFrame.from_dict(data, orient="index", columns=["A", "B", "C", "D"]),
        native_pd.DataFrame.from_dict(
            data, orient="index", columns=["A", "B", "C", "D"]
        ),
        check_dtype=False,
    )


@sql_count_checker(query_count=1)
def test_from_dict_orient_tight():
    data = {
        "index": [("a", "b"), ("a", "c")],
        "columns": [("x", 1), ("y", 2)],
        "data": [[1, 3], [2, 4]],
        "index_names": ["n1", "n2"],
        "column_names": ["z1", "z2"],
    }

    assert_frame_equal(
        pd.DataFrame.from_dict(data, orient="tight"),
        native_pd.DataFrame.from_dict(data, orient="tight"),
        check_dtype=False,
    )


@sql_count_checker(query_count=5)
def test_from_dict_series_values():
    # TODO(SNOW-1857349): Proved a lazy implementation for this case
    data = {i: pd.Series(range(1)) for i in range(2)}

    assert_frame_equal(
        pd.DataFrame.from_dict(data),
        native_pd.DataFrame.from_dict(data),
        check_dtype=False,
    )
