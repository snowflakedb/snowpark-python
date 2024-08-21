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


@pytest.mark.parametrize(
    "data", [[1, 2, 3, 4], list(range(250)), ["A", None, 2.3, 1], []]
)
@sql_count_checker(query_count=1)
def test_create_df_with_index_as_data(data):
    """
    Creating a DataFrame where the data is an Index.
    """
    # Create Snowpark pandas DataFrame and native pandas DataFrame from an Index object.
    native_idx = native_pd.Index(data, name="some name")
    snow_idx = pd.Index(native_idx)
    assert_frame_equal(pd.DataFrame(snow_idx), native_pd.DataFrame(native_idx))


@pytest.mark.parametrize(
    "data", [[1, 2, 3, 4], list(range(250)), ["A", None, 2.3, 1], []]
)
@sql_count_checker(query_count=1)
def test_create_series_with_index_as_data(data):
    """
    Creating a Series where the data is an Index.
    """
    # Create Snowpark pandas Series and native pandas Series from an Index object.
    native_idx = native_pd.Index(data, name="some name")
    snow_idx = pd.Index(native_idx)
    assert_series_equal(pd.Series(snow_idx), native_pd.Series(native_idx))


@pytest.mark.parametrize(
    "data, index",
    [
        ([1, 2, 3, 4], ["A", "B", "C", "D"]),
        (list(range(100)), list(range(200, 300))),
        (["A", None, 2.3, 1], [None, "B", 0, 3.14]),
        ([], []),
    ],
)
@sql_count_checker(query_count=2)
def test_create_df_with_index_as_index(data, index):
    """
    Creating a DataFrame where the index is an Index.
    """
    # Two queries are issued: one when creating the DataFrame (the index is converted
    # to a native pandas object), one when materializing the DataFrame for comparison.
    # Create Snowpark pandas DataFrame and native pandas DataFrame with an Index object as the index.
    native_idx = native_pd.Index(index, name="some name")
    snow_idx = pd.Index(native_idx)
    assert_frame_equal(
        pd.DataFrame(data, index=snow_idx),
        native_pd.DataFrame(data, index=native_idx),
        check_dtype=False,
        check_index_type=False,
        check_column_type=False,
    )


@pytest.mark.parametrize(
    "data, index",
    [
        ([1, 2, 3, 4], ["A", "B", "C", "D"]),
        (list(range(100)), list(range(100, 200))),
        (["A", None, 2.3, 1], [None, "B", 0, 3.14]),
        ([], []),
    ],
)
@sql_count_checker(query_count=2)
def test_create_series_with_index_as_index(data, index):
    """
    Creating a Series where the index is an Index.
    """
    # Two queries are issued: one when creating the Series (the index is converted
    # to a native pandas object), one when materializing the Series for comparison.
    # Create Snowpark pandas Series and native pandas Series with an Index object as the index.
    native_idx = native_pd.Index(index, name="some name")
    snow_idx = pd.Index(native_idx)
    assert_series_equal(
        pd.Series(data, index=snow_idx),
        native_pd.Series(data, index=native_idx),
        check_dtype=False,
        check_index_type=False,
    )


@pytest.mark.parametrize(
    "data, index",
    [
        ([1, 2, 3, 4], ["A", "B", "C", "D"]),
        (list(range(250)), list(range(250, 500))),
        (["A", None, 2.3, 1], [None, "B", 0, 3.14]),
        ([], []),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_create_df_with_index_as_data_and_index(data, index):
    """
    Creating a DataFrame where the data is an Index and the index is also an Index.
    """
    # Create Snowpark pandas DataFrame and native pandas DataFrame from Index objects.
    native_idx_data = native_pd.Index(data, name="data name")
    snow_idx_data = pd.Index(native_idx_data)
    native_idx_index = native_pd.Index(index, name="index name")
    snow_idx_index = pd.Index(native_idx_index)
    assert_frame_equal(
        pd.DataFrame(snow_idx_data, index=snow_idx_index),
        native_pd.DataFrame(native_idx_data, index=native_idx_index),
    )


@pytest.mark.parametrize(
    "data, index",
    [
        ([1, 2, 3, 4], ["A", "B", "C", "D"]),
        (list(range(100)), list(range(100, 200))),
        (["A", None, 2.3, 1], [None, "B", 0, 3.14]),
        ([], []),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_create_series_with_index_as_data_and_index(data, index):
    """
    Creating a Series where the data is an Index and the index is also an Index.
    """
    # Create Snowpark pandas Series and native pandas Series from Index objects.
    # TODO: Index is not being set at all.
    native_idx_data = native_pd.Index(data, name="data name")
    snow_idx_data = pd.Index(native_idx_data)
    native_idx_index = native_pd.Index(index, name="index name")
    snow_idx_index = pd.Index(native_idx_index)
    assert_series_equal(
        pd.Series(snow_idx_data, index=snow_idx_index),
        native_pd.Series(native_idx_data, index=native_idx_index),
    )


@pytest.mark.parametrize(
    "data, native_series",
    [
        (
            [1, 2, 3, 4],
            native_pd.Series(
                ["A", "B", "C", "D"],
                index=[1.1, 2.2, 3.3, 4.4],
                name="index series name",
            ),
        ),
        (list(range(100)), native_pd.Series(list(range(100, 200)))),
        (
            ["A", None, 2.3, 1],
            native_pd.Series([None, "B", 0, 3.14], name="mixed series as index"),
        ),
        ([], native_pd.Series([], name="empty series")),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_create_df_with_index_as_data_and_series_as_index(data, native_series):
    """
    Creating a DataFrame where the data is an Index and the index is a Series.
    """
    snow_series = pd.Series(native_series)
    native_index = native_pd.Index(data, name="index data name")
    snow_index = pd.Index(native_index)
    assert_frame_equal(
        pd.DataFrame(snow_index, index=snow_series),
        native_pd.DataFrame(native_index, index=native_series),
    )


@pytest.mark.parametrize(
    "data, native_series",
    [
        (
            [1, 2, 3, 4],
            native_pd.Series(
                ["A", "B", "C", "D"],
                index=[1.1, 2.2, 3.3, 4.4],
                name="index series name",
            ),
        ),
        (list(range(100)), native_pd.Series(list(range(100, 200)))),
        (
            ["A", None, 2.3, 1],
            native_pd.Series([None, "B", 0, 3.14], name="mixed series as index"),
        ),
        ([], native_pd.Series([], name="empty series")),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_create_series_with_index_as_data_and_series_as_index(data, native_series):
    """
    Creating a Series where the data is an Index and the index is a Series.
    """
    snow_series = pd.Series(native_series)
    native_index = native_pd.Index(data, name="index data name")
    snow_index = pd.Index(native_index)
    assert_series_equal(
        pd.Series(snow_index, index=snow_series),
        native_pd.Series(native_index, index=native_series),
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
@sql_count_checker(query_count=1, join_count=2)
def test_create_df_with_series_as_data_and_index_as_index(native_series, native_index):
    """
    Creating a DataFrame where the data is a Series and the index is an Index.
    """
    # Two joins are performed: one from joining the data and index parameters to have a query compiler whose
    # index columns match the provided index, and one from performing .loc[] to filter the generated qc.
    snow_series = pd.Series(native_series)
    snow_index = pd.Index(native_index)
    assert_frame_equal(
        pd.DataFrame(snow_series, index=snow_index),
        native_pd.DataFrame(native_series, index=native_index),
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
@sql_count_checker(query_count=1, join_count=2)
def test_create_series_with_series_as_data_and_index_as_index(
    native_series, native_index
):
    """
    Creating a Series where the data is a Series and the index is an Index.
    """
    # Two joins are performed: one from joining the data and index parameters to have a query compiler whose
    # index columns match the provided index, and one from performing .loc[] to filter the generated qc.
    snow_series = pd.Series(native_series)
    snow_index = pd.Index(native_index)
    assert_series_equal(
        pd.Series(snow_series, index=snow_index),
        native_pd.Series(native_series, index=native_index),
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


@sql_count_checker(query_count=0)
def test_create_df_with_df_index_negative():
    with pytest.raises(ValueError, match="Index data must be 1-dimensional"):
        pd.DataFrame([1, 2, 3], index=pd.DataFrame([[1, 2], [3, 4], [5, 6]]))
    with pytest.raises(
        ValueError,
        match=re.escape("Shape of passed values is (3, 1), indices imply (2, 1)"),
    ):
        pd.DataFrame([1, 2, 3], index=[[1, 2], [3, 4], [5, 6]])
