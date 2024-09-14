#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import re

import modin.pandas as pd
import numpy as np
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
@sql_count_checker(query_count=1, join_count=0)
def test_create_with_index_as_data(native_idx, obj_type):
    """
    Creating a Series where the data is an Index.
    """
    snow_idx = pd.Index(native_idx)
    assert_equal_func, snow_obj, native_obj, kwargs = obj_type_helper(obj_type)
    assert_equal_func(
        snow_obj(snow_idx), native_obj(native_idx), check_dtype=False, **kwargs
    )


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
@sql_count_checker(query_count=1, join_count=1)
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
        check_dtype=False,
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
        ({}, native_pd.Index([10, 0, 1], name="non-empty index")),
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
        (
            {
                "A": [1, 2, 3],
                "B": [4, 5, 6],
            },  # dict data should behave similar to DataFrame data
            native_pd.Index([10, 0, 1], name="non-empty index"),
            ["A", "C"],
        ),
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
    snow_df = (
        pd.DataFrame(native_df)
        if isinstance(native_df, native_pd.DataFrame)
        else native_df
    )
    snow_index = pd.Index(native_index)
    qc = 1 if column_type == "list" else 2
    qc += 1 if (isinstance(native_df, dict)) else 0
    qc += 1 if (isinstance(native_df, dict) and column_type == "index") else 0
    jc = 2 if isinstance(native_df, native_pd.DataFrame) else 0
    with SqlCounter(query_count=qc, join_count=jc):
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


@sql_count_checker(query_count=2)
def test_create_df_with_dict_as_data_and_index_as_index():
    """
    Special case when creating:
    DataFrame({"A": [1], "V": [2]}, native_pd.Index(["A", "B", "C"]), name="none")
          A  V
    none
    A     1  2
    B     1  2  <--- the first row is copied into the rest of the rows.
    C     1  2
    """
    data = {"A": [1], "V": [2]}
    native_index = native_pd.Index(["A", "B", "C"])
    snow_index = pd.Index(native_index)
    native_df = native_pd.DataFrame(data, index=native_index)
    snow_df = pd.DataFrame(data, index=snow_index)
    assert_frame_equal(snow_df, native_df)


@sql_count_checker(query_count=1)
def test_create_series_with_list_of_lists_index():
    # When given a list of lists as the index, this index needs to be converted to a MultiIndex before processing.
    arrays = [
        np.array(["qux", "qux", "foo", "foo", "baz", "baz", "bar", "bar"]),
        np.array(["two", "one", "two", "one", "two", "one", "two", "one"]),
    ]
    data = [1, 2, 3, 4, 5, 6, 7, 8]
    native_series = native_pd.Series(data, index=arrays)
    snow_series = pd.Series(data, index=arrays)
    assert_series_equal(snow_series, native_series)


@sql_count_checker(query_count=1, join_count=2)
def test_create_series_with_index_data_and_list_of_lists_index():
    # When given a list of lists as the index, this index needs to be converted to a MultiIndex before processing.
    arrays = [
        ["qux", "qux", "foo", "foo", "baz", "baz", "bar", "bar"],
        ["two", "one", "two", "one", "two", "one", "two", "one"],
    ]
    data = native_pd.Index([1, 2, 3, 4, 5, 6, 7, 8])
    native_series = native_pd.Series(data, index=arrays)
    snow_series = pd.Series(pd.Index(data), index=arrays)
    assert_series_equal(snow_series, native_series)


@sql_count_checker(query_count=1, join_count=2)
def test_create_df_with_index_data_and_list_of_lists_index():
    # When given a list of lists as the index, this index needs to be converted to a MultiIndex before processing.
    arrays = [
        ["qux", "qux", "foo", "foo", "baz", "baz", "bar", "bar"],
        ["two", "one", "two", "one", "two", "one", "two", "one"],
    ]
    data = native_pd.Index([1, 2, 3, 4, 5, 6, 7, 8])
    native_df = native_pd.DataFrame(data, index=arrays)
    snow_df = pd.DataFrame(pd.Index(data), index=arrays)
    assert_frame_equal(snow_df, native_df)


@sql_count_checker(query_count=1)
def test_create_series_with_none_data_and_non_empty_index():
    # When creating an empty Series with a non-empty index, the index should be used as the index of the Series.
    index = ["A", "B", "C", "D"]
    native_series = native_pd.Series(None, index=index, dtype=object)
    snow_series = pd.Series(None, index=index, dtype=object)
    assert_series_equal(snow_series, native_series)


@pytest.mark.parametrize(
    "data1, data2", [("series", "series"), ("series", "index"), ("index", "index")]
)
def test_create_df_with_series_index_dict_data(data1, data2):
    # Create the dict data.
    native_data1 = (
        native_pd.Series([1, 2, 3]) if data1 == "series" else native_pd.Index([1, 2, 3])
    )
    native_data2 = (
        native_pd.Series([4, 5, 6]) if data2 == "series" else native_pd.Index([4, 5, 6])
    )
    snow_data1 = pd.Series([1, 2, 3]) if data1 == "series" else pd.Index([1, 2, 3])
    snow_data2 = pd.Series([4, 5, 6]) if data2 == "series" else pd.Index([4, 5, 6])
    native_data = {"A": native_data1, "B": native_data2}
    snow_data = {"A": snow_data1, "B": snow_data2}

    # Create DataFrame only with dict data.
    native_df = native_pd.DataFrame(native_data)
    snow_df = pd.DataFrame(snow_data)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with dict data and Series index.
    native_ser_index = native_pd.Series([9, 2, 999])
    snow_ser_index = pd.Series([9, 2, 999])
    native_df = native_pd.DataFrame(native_data, index=native_ser_index)
    snow_df = pd.DataFrame(snow_data, index=snow_ser_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with dict data and Index index.
    native_index = native_pd.Index([9, 2, 999])
    snow_index = pd.Index([9, 2, 999])
    native_df = native_pd.DataFrame(native_data, index=native_index)
    snow_df = pd.DataFrame(snow_data, index=snow_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with dict data, Series index, and columns.
    columns = ["A", "B", "C"]
    native_df = native_pd.DataFrame(
        native_data, index=native_ser_index, columns=columns
    )
    snow_df = pd.DataFrame(snow_data, index=snow_ser_index, columns=columns)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with dict data, Index index, and Index columns.
    native_columns = native_pd.Index(columns)
    snow_columns = pd.Index(columns)
    native_df = native_pd.DataFrame(
        native_data, index=native_index, columns=native_columns
    )
    snow_df = pd.DataFrame(snow_data, index=snow_index, columns=snow_columns)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)


@pytest.mark.parametrize(
    "data1, data2", [("series", "series"), ("series", "index"), ("index", "index")]
)
def test_create_df_with_series_index_list_data(data1, data2):
    # Create the list data.
    native_data1 = (
        native_pd.Series([11, 22, 33])
        if data1 == "series"
        else native_pd.Index([11, 22, 33])
    )
    native_data2 = (
        native_pd.Series([44, 55, 66])
        if data2 == "series"
        else native_pd.Index([44, 55, 66])
    )
    snow_data1 = (
        pd.Series([11, 22, 33]) if data1 == "series" else pd.Index([11, 22, 33])
    )
    snow_data2 = (
        pd.Series([44, 55, 66]) if data2 == "series" else pd.Index([44, 55, 66])
    )
    native_data = [native_data1, native_data2]
    snow_data = [snow_data1, snow_data2]

    # Create DataFrame only with list data.
    native_df = native_pd.DataFrame(native_data)
    snow_df = pd.DataFrame(snow_data)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with list data and Series index.
    native_ser_index = native_pd.Series([2, 11])
    snow_ser_index = pd.Series([2, 11])
    native_df = native_pd.DataFrame(native_data, index=native_ser_index)
    snow_df = pd.DataFrame(snow_data, index=snow_ser_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df, check_dtype=False)

    # Create DataFrame with list data and Index index.
    native_index = native_pd.Index([22, 11])
    snow_index = pd.Index([22, 11])
    native_df = native_pd.DataFrame(native_data, index=native_index)
    snow_df = pd.DataFrame(snow_data, index=snow_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df, check_dtype=False)

    # Create DataFrame with list data, Series index, and columns.
    columns = ["A", "B", "C"]
    native_df = native_pd.DataFrame(
        native_data, index=native_ser_index, columns=columns
    )
    snow_df = pd.DataFrame(snow_data, index=snow_ser_index, columns=columns)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df, check_dtype=False)

    # Create DataFrame with list data, Index index, and Index columns.
    native_columns = native_pd.Index(columns)
    snow_columns = pd.Index(columns)
    native_df = native_pd.DataFrame(
        native_data, index=native_index, columns=native_columns
    )
    snow_df = pd.DataFrame(snow_data, index=snow_index, columns=snow_columns)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df, check_dtype=False)


@pytest.mark.parametrize(
    "data1, data2", [("series", "series"), ("series", "index"), ("index", "index")]
)
def test_create_series_with_series_index_list_data(data1, data2):
    # Create the list data.
    native_data1 = (
        native_pd.Series([11, 22, 33])
        if data1 == "series"
        else native_pd.Index([11, 22, 33])
    )
    native_data2 = (
        native_pd.Series([44, 55, 66])
        if data2 == "series"
        else native_pd.Index([44, 55, 66])
    )
    snow_data1 = (
        pd.Series([11, 22, 33]) if data1 == "series" else pd.Index([11, 22, 33])
    )
    snow_data2 = (
        pd.Series([44, 55, 66]) if data2 == "series" else pd.Index([44, 55, 66])
    )
    native_data = [native_data1, native_data2]
    snow_data = [snow_data1, snow_data2]

    # Create Series only with list data.
    native_df = native_pd.Series(native_data)
    snow_df = pd.Series(snow_data)
    with SqlCounter(query_count=1):
        assert_series_equal(snow_df, native_df)

    # Create Series with list data and Series index.
    native_ser_index = native_pd.Series([2, 11])
    snow_ser_index = pd.Series([2, 11])
    native_df = native_pd.Series(native_data, index=native_ser_index)
    snow_df = pd.Series(snow_data, index=snow_ser_index)
    with SqlCounter(query_count=1):
        assert_series_equal(snow_df, native_df, check_dtype=False)

    # Create Series with list data and Index index.
    native_index = native_pd.Index([22, 11])
    snow_index = pd.Index([22, 11])
    native_df = native_pd.Series(native_data, index=native_index)
    snow_df = pd.Series(snow_data, index=snow_index)
    with SqlCounter(query_count=1):
        assert_series_equal(snow_df, native_df, check_dtype=False)


@pytest.mark.parametrize(
    "data1, data2", [("series", "series"), ("series", "index"), ("index", "index")]
)
def test_create_series_with_series_index_dict_data(data1, data2):
    # Create the dict data.
    native_data1 = (
        native_pd.Series([1, 2, 3]) if data1 == "series" else native_pd.Index([1, 2, 3])
    )
    native_data2 = (
        native_pd.Series([4, 5, 6]) if data2 == "series" else native_pd.Index([4, 5, 6])
    )
    snow_data1 = pd.Series([1, 2, 3]) if data1 == "series" else pd.Index([1, 2, 3])
    snow_data2 = pd.Series([4, 5, 6]) if data2 == "series" else pd.Index([4, 5, 6])
    native_data = {11: native_data1, 22: native_data2}
    snow_data = {11: snow_data1, 22: snow_data2}

    # Create DataFrame only with dict data.
    native_df = native_pd.Series(native_data)
    snow_df = pd.Series(snow_data)
    with SqlCounter(query_count=1):
        assert_series_equal(snow_df, native_df)

    # Create DataFrame with dict data and Series index.
    native_ser_index = native_pd.Series([9, 2, 999])
    snow_ser_index = pd.Series([9, 2, 999])
    native_df = native_pd.Series(native_data, index=native_ser_index)
    snow_df = pd.Series(snow_data, index=snow_ser_index)
    with SqlCounter(query_count=1):
        assert_series_equal(snow_df, native_df)

    # Create DataFrame with dict data and Index index.
    native_index = native_pd.Index([9, 2, 999])
    snow_index = pd.Index([9, 2, 999])
    native_df = native_pd.Series(native_data, index=native_index)
    snow_df = pd.Series(snow_data, index=snow_index)
    with SqlCounter(query_count=1):
        assert_series_equal(snow_df, native_df)


def test_create_df_with_mixed_series_index_dict_data():
    # Create the dict data.
    native_data1 = native_pd.Series([1, 2, 3])
    native_data2 = native_pd.Index([4, 5, 6])
    data3 = [7, 8, 9]
    snow_data1 = pd.Series([1, 2, 3])
    snow_data2 = pd.Index([4, 5, 6])
    native_data = {"A": native_data1, "B": native_data2, "C": data3}
    snow_data = {"A": snow_data1, "B": snow_data2, "C": data3}

    # Create DataFrame only with dict data.
    native_df = native_pd.DataFrame(native_data)
    snow_df = pd.DataFrame(snow_data)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with dict data and Series index.
    native_ser_index = native_pd.Series([9, 2, 999])
    snow_ser_index = pd.Series([9, 2, 999])
    native_df = native_pd.DataFrame(native_data, index=native_ser_index)
    snow_df = pd.DataFrame(snow_data, index=snow_ser_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with dict data and Index index.
    native_index = native_pd.Index([9, 2, 999])
    snow_index = pd.Index([9, 2, 999])
    native_df = native_pd.DataFrame(native_data, index=native_index)
    snow_df = pd.DataFrame(snow_data, index=snow_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with dict data, Series index, and columns.
    columns = ["A", "B", "C"]
    native_df = native_pd.DataFrame(
        native_data, index=native_ser_index, columns=columns
    )
    snow_df = pd.DataFrame(snow_data, index=snow_ser_index, columns=columns)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with dict data, Index index, and Index columns.
    native_columns = native_pd.Index(columns)
    snow_columns = pd.Index(columns)
    native_df = native_pd.DataFrame(
        native_data, index=native_index, columns=native_columns
    )
    snow_df = pd.DataFrame(snow_data, index=snow_index, columns=snow_columns)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)


def test_create_df_with_mixed_series_index_list_data():
    # Create the list data.
    native_data1 = native_pd.Series([1, 2, 3])
    native_data2 = native_pd.Index([4, 5, 6])
    data3 = [7, 8, 9]
    snow_data1 = pd.Series([1, 2, 3])
    snow_data2 = pd.Index([4, 5, 6])
    # Need to convert data3 to an Index since native pandas tries to perform `get_indexer` on it.
    native_data = [native_data1, native_data2, native_pd.Index(data3)]
    snow_data = [snow_data1, snow_data2, data3]

    # Create DataFrame only with list data.
    native_df = native_pd.DataFrame(native_data)
    snow_df = pd.DataFrame(snow_data)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)

    # Create DataFrame with list data and Series index.
    native_ser_index = native_pd.Series([2, 11, 0])
    snow_ser_index = pd.Series([2, 11, 0])
    native_df = native_pd.DataFrame(native_data, index=native_ser_index)
    snow_df = pd.DataFrame(snow_data, index=snow_ser_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df, check_dtype=False)

    # Create DataFrame with list data and Index index.
    native_index = native_pd.Index([22, 11, 0])
    snow_index = pd.Index([22, 11, 0])
    native_df = native_pd.DataFrame(native_data, index=native_index)
    snow_df = pd.DataFrame(snow_data, index=snow_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df, check_dtype=False)

    # Create DataFrame with list data, Series index, and columns.
    columns = ["A", "B", "C"]
    native_df = native_pd.DataFrame(
        native_data, index=native_ser_index, columns=columns
    )
    snow_df = pd.DataFrame(snow_data, index=snow_ser_index, columns=columns)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df, check_dtype=False)

    # Create DataFrame with list data, Index index, and Index columns.
    native_columns = native_pd.Index(columns)
    snow_columns = pd.Index(columns)
    native_df = native_pd.DataFrame(
        native_data, index=native_index, columns=native_columns
    )
    snow_df = pd.DataFrame(snow_data, index=snow_index, columns=snow_columns)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df, check_dtype=False)


@pytest.mark.xfail(
    reason="SNOW-1638397 DataFrane creation fails: reindex does not work with string index"
)
def test_create_df_with_series_data_and_series_index():
    # Create the data and index.
    native_data = native_pd.Series([1, 2, 3])
    native_index = native_pd.Series(["A", 0, "C"])
    snow_data = pd.Series(native_data)
    snow_index = pd.Series(native_index)

    # Create DataFrame with Series data and Series index.
    native_df = native_pd.DataFrame(native_data, index=native_index)
    snow_df = pd.DataFrame(snow_data, index=snow_index)
    with SqlCounter(query_count=1):
        assert_frame_equal(snow_df, native_df)


@sql_count_checker(query_count=0)
def test_create_df_with_df_index_negative():
    with pytest.raises(ValueError, match="Index data must be 1-dimensional"):
        native_pd.DataFrame(
            [1, 2, 3], index=native_pd.DataFrame([[1, 2], [3, 4], [5, 6]])
        )
    with pytest.raises(ValueError, match="Index data must be 1-dimensional"):
        pd.DataFrame([1, 2, 3], index=pd.DataFrame([[1, 2], [3, 4], [5, 6]]))


@sql_count_checker(query_count=0)
def test_create_series_with_df_index_negative():
    with pytest.raises(ValueError, match="Index data must be 1-dimensional"):
        native_pd.Series([1, 2, 3], index=native_pd.DataFrame([[1, 2], [3, 4], [5, 6]]))
    with pytest.raises(ValueError, match="Index data must be 1-dimensional"):
        pd.Series([1, 2, 3], index=pd.DataFrame([[1, 2], [3, 4], [5, 6]]))


@sql_count_checker(query_count=0)
def test_create_series_with_df_data_negative():
    with pytest.raises(
        ValueError,
        match=re.escape(
            "The truth value of a DataFrame is ambiguous. Use a.empty, a.bool()"
            ", a.item(), a.any() or a.all()."
        ),
    ):
        native_pd.Series(native_pd.DataFrame([[1, 2], [3, 4], [5, 6]]))
    with pytest.raises(ValueError, match="Index data must be 1-dimensional"):
        pd.Series(pd.DataFrame([[1, 2], [3, 4], [5, 6]]))
