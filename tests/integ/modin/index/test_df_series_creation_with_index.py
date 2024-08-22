#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import re

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import assert_frame_equal, assert_series_equal


def obj_type_helper(obj_type: str) -> tuple:
    """
    Helper function to return the appropriate objects and kwargs based on the object type.

    Parameters
    ----------
    obj_type : str
        The type of object to be created. Can be either "df" or "series".

    Returns
    -------
    tuple
        A tuple containing the assert_equal_func, Snowpark pandas object dtype, native pandas object dtype, and kwargs.
    """
    if obj_type == "df":
        assert_equal_func = assert_frame_equal
        snow_obj, native_obj = pd.DataFrame, native_pd.DataFrame
        kwargs = {"check_column_type": False}
    else:
        assert_equal_func = assert_series_equal
        snow_obj, native_obj = pd.Series, native_pd.Series
        kwargs = {}
    return assert_equal_func, snow_obj, native_obj, kwargs


@pytest.mark.parametrize(
    "native_idx",
    [
        native_pd.Index([1, 2, 3, 4], name="some name"),
        native_pd.Index(list(range(250))),
        native_pd.Index(["A", None, 2.3, 1], name="AAAAA"),
        native_pd.Index([]),
    ],
)
@pytest.mark.parametrize("obj_type", ["series", "df"])
@sql_count_checker(query_count=1)
def test_create_with_index_as_data(native_idx, obj_type):
    """
    Creating a Series where the data is an Index.
    """
    snow_idx = pd.Index(native_idx)
    assert_equal_func, snow_obj, native_obj, _ = obj_type_helper(obj_type)
    assert_equal_func(snow_obj(snow_idx), native_obj(native_idx))


@pytest.mark.parametrize(
    "data, native_idx",
    [
        ([1, 2, 3, 4], native_pd.Index(["A", "B", "C", "D"], name="some name")),
        (list(range(100)), native_pd.Index(list(range(200, 300)))),
        (["A", None, 2.3, 1], native_pd.Index([None, "B", 0, 3.14])),
        ([], native_pd.Index([], name="empty index")),
    ],
)
@pytest.mark.parametrize("obj_type", ["series", "df"])
@sql_count_checker(query_count=1, join_count=1)
def test_create_with_index_as_index(data, native_idx, obj_type):
    """
    Creating a Series/DataFrame where the index is an Index.
    """
    # A join is performed to set the index columns of the generated Series/DataFrame.
    snow_idx = pd.Index(native_idx)
    assert_equal_func, snow_obj, native_obj, kwargs = obj_type_helper(obj_type)
    assert_equal_func(
        snow_obj(data, index=snow_idx),
        native_obj(data, index=native_idx),
        check_dtype=False,
        check_index_type=False,
        **kwargs,
    )


@pytest.mark.parametrize(
    "native_idx_data, native_idx_index",
    [
        (
            native_pd.Index([1, 2, 3, 4], name="data name"),
            native_pd.Index(["A", "B", "C", "D"]),
        ),
        (
            native_pd.Index(list(range(250))),
            native_pd.Index(list(range(250, 500)), name="index name"),
        ),
        (
            native_pd.Index(["A", None, 2.3, 1], name="data name"),
            native_pd.Index([None, "B", 0, 3.14], name="index name"),
        ),
        (native_pd.Index([]), native_pd.Index([])),
    ],
)
@pytest.mark.parametrize("obj_type", ["series", "df"])
@sql_count_checker(query_count=1, join_count=1)
def test_create_with_index_as_data_and_index(
    native_idx_data, native_idx_index, obj_type
):
    """
    Creating a Series/DataFrame where the data is an Index and the index is also an Index.
    """
    # A join is required to combine the query compilers of the data and index objects.
    snow_idx_data = pd.Index(native_idx_data)
    snow_idx_index = pd.Index(native_idx_index)
    assert_equal_func, snow_obj, native_obj, _ = obj_type_helper(obj_type)
    assert_equal_func(
        snow_obj(data=snow_idx_data, index=snow_idx_index),
        native_obj(data=native_idx_data, index=native_idx_index),
    )


@pytest.mark.parametrize(
    "native_index, native_series",
    [
        (
            native_pd.Index([1, 2, 3, 4], name="index name"),
            native_pd.Series(
                ["A", "B", "C", "D"],
                index=[1.1, 2.2, 3.3, 4.4],
                name="index series name",
            ),
        ),
        (
            native_pd.Index(list(range(100)), name="AAAAA"),
            native_pd.Series(list(range(100, 200))),
        ),
        (
            native_pd.Index(["A", None, 2.3, 1]),
            native_pd.Series([None, "B", 0, 3.14]),
        ),
        (native_pd.Index([]), native_pd.Series([], name="empty series")),
    ],
)
@pytest.mark.parametrize("obj_type", ["series", "df"])
@sql_count_checker(query_count=1, join_count=1)
def test_create_with_index_as_data_and_series_as_index(
    native_index, native_series, obj_type
):
    """
    Creating a Series/DataFrame where the data is an Index and the index is a Series.
    """
    # A join is required to combine the query compilers of the data and index objects.
    snow_index = pd.Index(native_index)
    snow_series = pd.Series(native_series)
    assert_equal_func, snow_obj, native_obj, _ = obj_type_helper(obj_type)
    assert_equal_func(
        snow_obj(data=snow_index, index=snow_series),
        native_obj(data=native_index, index=native_series),
    )


@pytest.mark.parametrize(
    "native_series, native_index",
    [
        (
            native_pd.Series(
                ["A", "B", "C", "D"], index=[1.1, 2.2, 3, 4], name="index series name"
            ),
            native_pd.Index([1, 2, 3, 4], name="some name"),
        ),  # some index values are missing
        (
            native_pd.Series(list(range(100))),
            native_pd.Index(list(range(-50, 100, 4)), name="skip numbers"),
        ),  # some index values are missing
        (
            native_pd.Series(
                [10, 20, 30, 40],
                index=native_pd.Index([None, "B", 0, 3.14], name="mixed"),
                name="mixed series as index",
            ),
            native_pd.Index(["B", 0, None, 3.14]),
        ),  # rearranged index values
        (
            native_pd.Series(["A", "B", "C", "D", "E"], name="series"),
            native_pd.Index([3, 4], name="index"),
        ),  # subset of index values
        (
            native_pd.Series(
                list(range(20)), index=native_pd.Index(list(range(20)), name=20)
            ),
            native_pd.Index(list(range(20))),
        ),  # all index values match
        (
            native_pd.Series(["A", "V", "D", "R"]),
            native_pd.Index([10, 20, 30, 40], name="none"),
        ),  # no index values match
        (
            native_pd.Series([], name="empty series", dtype="int64"),
            native_pd.Index([], name="empty index", dtype="int64"),
        ),  # empty series and index
    ],
)
@pytest.mark.parametrize("obj_type", ["series", "df"])
@sql_count_checker(query_count=1, join_count=2)
def test_create_with_series_as_data_and_index_as_index(
    native_series, native_index, obj_type
):
    """
    Creating a Series/DataFrame where the data is a Series and the index is an Index.
    """
    # Two joins are performed: one from joining the data and index parameters to have a query compiler whose
    # index columns match the provided index, and one from performing .loc[] to filter the generated qc.
    snow_series = pd.Series(native_series)
    snow_index = pd.Index(native_index)
    assert_equal_func, snow_obj, native_obj, _ = obj_type_helper(obj_type)
    assert_equal_func(
        snow_obj(data=snow_series, index=snow_index),
        native_obj(data=native_series, index=native_index),
    )


@pytest.mark.parametrize(
    "native_df, native_index",
    [
        # Single column DataFrames.
        (
            native_pd.DataFrame(
                ["A", "B", "C", "D"], index=[1.1, 2.2, 3, 4], columns=["df column!"]
            ),
            native_pd.Index([1, 2, 3, 4], name="some name"),
        ),  # some index values are missing
        (
            native_pd.DataFrame(list(range(100))),
            native_pd.Index(list(range(-50, 100, 4)), name="skip numbers"),
        ),  # some index values are missing
        (
            native_pd.DataFrame(
                [10, 20, 30, 40],
                index=native_pd.Index([None, "B", 0, 3.14], name="mixed"),
                columns=["C"],
            ),
            native_pd.Index(["B", 0, None, 3.14]),
        ),  # rearranged index values
        (
            native_pd.DataFrame(["A", "B", "C", "D", "E"], columns=["B"]),
            native_pd.Index([3, 4], name="index"),
        ),  # subset of index values
        (
            native_pd.DataFrame(list(range(20))),
            native_pd.Index(list(range(20))),
        ),  # all index values match
        (
            native_pd.DataFrame(["A", "V", "D", "R"]),
            native_pd.Index([10, 20, 30, 40], name="none"),
        ),  # no index values match
        # Multi-column DataFrames.
        (
            native_pd.DataFrame(
                {"col1": ["A", "B", "C", "D"], "col2": ["B", "H", "T", "W"]},
                index=[1.1, 2.2, 3, 4],
            ),
            native_pd.Index([1, 2, 3, 4], name="some name"),
        ),  # some index values are missing
        (
            native_pd.DataFrame(
                [[10, 20, 30, 40], [2, 4, 6, 7], [-1, -2, -3, -4], [90, 50, 30, 10]],
                index=native_pd.Index([None, "B", 0, 3.14], name="mixed"),
                columns=["C", "L", "M", "W"],
            ),
            native_pd.Index(["B", 0, None, 3.14]),
        ),  # rearranged index values
        (
            native_pd.DataFrame(
                [["A", "B", "C", "D", "E"], ["R", "S", "T", "U", "V"]],
                columns=[1, 2, 3, 4, 5],
            ),
            native_pd.Index([3, 4], name="index"),
        ),  # subset of index values
        (
            native_pd.DataFrame([list(range(20)), list(range(20))]),
            native_pd.Index(list(range(20))),
        ),  # all index values match
        (
            native_pd.DataFrame(
                {
                    "A": ["A", "V", "D", "R"],
                    "V": ["V", "D", "R", "A"],
                    "D": ["D", "R", "A", "V"],
                    "R": ["R", "A", "V", "D"],
                }
            ),
            native_pd.Index([10, 20, 30, 40], name="none"),
        ),  # no index values match
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_create_df_with_df_as_data_and_index_as_index(native_df, native_index):
    """
    Creating a DataFrame where the data is a DataFrame and the index is an Index.
    """
    # Two joins are performed: one from joining the data and index parameters to have a query compiler whose
    # index columns match the provided index, and one from performing .loc[] to filter the generated qc.
    snow_df = pd.DataFrame(native_df)
    snow_index = pd.Index(native_index)
    assert_frame_equal(
        pd.DataFrame(snow_df, index=snow_index),
        native_pd.DataFrame(native_df, index=native_index),
    )


@pytest.mark.parametrize(
    "native_df, native_index",
    [
        # Single column DataFrames.
        (
            native_pd.DataFrame([]),
            native_pd.Index([], name="empty index", dtype="int64"),
        ),  # empty series and index
        # Multi-column DataFrames.
        (
            native_pd.DataFrame([]),
            native_pd.Index(["A", "V"], name="non-empty index"),
        ),  # empty df and index
    ],
)
@sql_count_checker(query_count=1, join_count=2)
def test_create_df_with_empty_df_as_data_and_index_as_index(native_df, native_index):
    """
    Creating a DataFrame where the data is an empty DataFrame and the index is an Index.
    """
    # Two joins are performed: one from joining the data and index parameters to have a query compiler whose
    # index columns match the provided index, and one from performing .loc[] to filter the generated qc.
    snow_df = pd.DataFrame(native_df)
    snow_index = pd.Index(native_index)
    assert_frame_equal(
        pd.DataFrame(snow_df, index=snow_index),
        native_pd.DataFrame(native_df, index=native_index),
        check_column_type=False,
    )


@pytest.mark.parametrize(
    "native_df, native_index, columns",
    [
        # Single column DataFrames.
        (
            native_pd.DataFrame(list(range(20))),
            native_pd.Index(list(range(20))),
            [1],
        ),  # all index values match
        (
            native_pd.DataFrame(["A", "V", "D", "R"]),
            native_pd.Index([10, 20, 30, 40], name="none"),
            ["A"],
        ),  # no index values match, column missing
        # Multi-column DataFrames.
        (
            native_pd.DataFrame(
                {"col1": ["A", "B", "C", "D"], "col2": ["B", "H", "T", "W"]},
                index=[1.1, 2.2, 3, 4],
            ),
            native_pd.Index([1, 2, 3, 4], name="some name"),
            ["col1"],
        ),  # some index values are missing, subset of columns
        (
            native_pd.DataFrame(
                [[10, 20, 30, 40], [2, 4, 6, 7], [-1, -2, -3, -4], [90, 50, 30, 10]],
                index=native_pd.Index([None, "B", 0, 3.14], name="mixed"),
                columns=["C", "L", "M", "W"],
            ),
            native_pd.Index(["B", 0, None, 3.14]),
            [3, 1],
        ),  # rearranged index and column values
        (
            native_pd.DataFrame(
                [["A", "B", "C", "D", "E"], ["R", "S", "T", "U", "V"]],
                columns=[1, 2, 3, 4, 5],
            ),
            native_pd.Index([3, 4], name="index"),
            ["A", "V", "C"],
        ),  # subset of index values
        (
            native_pd.DataFrame([list(range(20)), list(range(20))]),
            native_pd.Index(list(range(20))),
            [1],
        ),  # all index values match
        (
            native_pd.DataFrame(
                {
                    "A": ["A", "V", "D", "R"],
                    "V": ["V", "D", "R", "A"],
                    "D": ["D", "R", "A", "V"],
                    "R": ["R", "A", "V", "D"],
                }
            ),
            native_pd.Index([10, 20, 30, 40], name="none"),
            ["A", "X", "D", "R"],
        ),  # no index values match
        (
            native_pd.DataFrame([]),
            native_pd.Index([], name="empty index", dtype="int64"),
            [],
        ),  # empty data, index, and columns
        (
            native_pd.DataFrame([]),
            native_pd.Index(["A", "V"], name="non-empty index"),
            ["A", "V"],
        ),  # empty data, non-empty index and columns
    ],
)
@pytest.mark.parametrize("column_type", ["list", "index"])
def test_create_df_with_df_as_data_and_index_as_index_and_different_columns(
    native_df, native_index, columns, column_type
):
    """
    Creating a DataFrame where the data is a DataFrame, the index is an Index, and non-existent columns.
    """
    # Two joins are performed: one from joining the data and index parameters to have a query compiler whose
    # index columns match the provided index, and one from performing .loc[] to filter the generated qc.
    # One extra query is required to create the columns if it is an Index (column_type is "index").
    native_columns = columns if column_type == "list" else native_pd.Index(columns)
    snow_columns = columns if column_type == "list" else pd.Index(columns)
    snow_df = pd.DataFrame(native_df)
    snow_index = pd.Index(native_index)
    with SqlCounter(query_count=1 if column_type == "list" else 2, join_count=2):
        assert_frame_equal(
            pd.DataFrame(snow_df, index=snow_index, columns=native_columns),
            native_pd.DataFrame(native_df, index=native_index, columns=snow_columns),
            check_dtype=False,
        )


@sql_count_checker(query_count=1)
def test_create_df_with_new_columns():
    """
    Creating a DataFrame with columns that don't exist in `data`.
    """
    native_df = native_pd.DataFrame(list(range(100)))
    snow_df = pd.DataFrame(native_df)
    assert_frame_equal(
        pd.DataFrame(snow_df, columns=["new column"]),
        native_pd.DataFrame(native_df, columns=["new column"]),
        check_dtype=False,
    )


@sql_count_checker(query_count=0)
def test_create_df_with_df_index_negative():
    with pytest.raises(ValueError, match="Index data must be 1-dimensional"):
        pd.DataFrame([1, 2, 3], index=pd.DataFrame([[1, 2], [3, 4], [5, 6]]))
    with pytest.raises(
        ValueError,
        match=re.escape("Shape of passed values is (3, 1), indices imply (2, 1)"),
    ):
        pd.DataFrame([1, 2, 3], index=[[1, 2], [3, 4], [5, 6]])
