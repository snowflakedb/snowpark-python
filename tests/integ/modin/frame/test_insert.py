#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.utils import (
    BASIC_NUMPY_PANDAS_SCALAR_DATA,
    BASIC_TYPE_DATA1,
    assert_frame_equal,
    assert_snowpark_pandas_equal_to_pandas,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture
def snow_df():
    return pd.DataFrame(
        {"col1": ["one", "two", "three"], "col2": ["abc", "pqr", "xyz"]},
        index=pd.Index([5, 1, 0]),
    )


@pytest.fixture(scope="function")
def native_df():
    return native_pd.DataFrame(
        {"col1": ["one", "two", "three"], "col2": ["abc", "pqr", "xyz"]},
        index=native_pd.Index([5, 1, 0]),
    )


@pytest.mark.parametrize(
    "native_value",
    [
        # DataFrame
        native_pd.DataFrame({"col1": ["a", "b", "c"]}),
        # DataFrame with index
        native_pd.DataFrame(
            {"col1": ["a", "b", "c"]}, index=native_pd.Index([0, 99, 100])
        ),
        # DataFrame with less rows
        native_pd.DataFrame({"col1": ["a"]}),
        # DataFrame with more rows
        native_pd.DataFrame(
            {"col1": ["a", "b", "c", "d", "e"]}, index=native_pd.Index([4, 3, 2, 1, 0])
        ),
    ],
)
@sql_count_checker(query_count=4, join_count=3)
def test_insert_snowpark_pandas_objects(native_df, native_value):
    snow_df = pd.DataFrame(native_df)
    value = pd.DataFrame(native_value)

    # Verify insert with DataFrame
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.insert(
            0, "col3", value if isinstance(df, pd.DataFrame) else value.to_pandas()
        ),
        inplace=True,
    )

    # Verify insert with Series
    value = pd.Series(native_df["col1"])
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.insert(
            0, "col4", value if isinstance(df, pd.DataFrame) else value.to_pandas()
        ),
        inplace=True,
    )


@pytest.mark.parametrize(
    "native_value",
    [
        # Note: Target dataFrame index values are [5, 1, 0]
        # DataFrame with duplicate index value 1
        native_pd.DataFrame(
            {"col1": ["a", "b", "c"]}, index=native_pd.Index([0, 1, 1])
        ),
        # Series with duplicate index value 1
        native_pd.Series(["a", "b", "c"], index=native_pd.Index([0, 1, 1])),
        # DataFrame with duplicate index value 2. 2 is not present in target dataframe
        # so technically this will not lead to one-to-many join but this is still
        # disallowed in native pandas.
        native_pd.DataFrame(
            {"col1": ["a", "b", "c"]}, index=native_pd.Index([0, 2, 2])
        ),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_insert_one_to_many(native_df, native_value):
    snow_df = pd.DataFrame(native_df)
    value = pd.DataFrame(native_value)

    # the Snowpark pandas behavior for insert with non-unique index values is different
    # compare with native pandas. Native pandas raise an ValueError,
    # "cannot reindex on an axis with duplicate labels". In Snowpark pandas, we are not
    # able to perform such check and raises an error without trigger an eager evaluation,
    # and it exposes an left join behavior.
    snow_df.insert(2, "col3", value)
    native_snow_res = snow_df.to_pandas().sort_index()

    if isinstance(native_value, native_pd.Series):
        native_value.name = "series"
    # We sort the index here, because pandas 2.2.x retains the sort order
    # of the original dict. https://github.com/pandas-dev/pandas/pull/55696
    # As we update the API from 2.1.4 to 2.2.x proper we may want to address
    # this behavior.
    expected_res = native_df.join(
        native_value, rsuffix="_x", how="left", sort=False
    ).sort_index()
    expected_res.columns = ["col1", "col2", "col3"]
    assert_frame_equal(native_snow_res, expected_res, check_index_type=False)


@pytest.mark.parametrize(
    "value",
    [
        np.array(["a", "b", "c"]),  # numpy array of shape (N,)
        np.array([["a"], ["b"], ["c"]]),  # numpy array of shape (N, 1)
        ["a", "b", "c"],  # python list
        {0: 1, 1: 2, 4: 3},  # python dict
        ("a", "b", "c"),  # python tuple
    ],
)
def test_insert_array_like(native_df, value):
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1, join_count=1):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.insert(0, "col3", value),
            inplace=True,  # insert operation is always inplace
        )


@pytest.mark.parametrize(
    "value",
    BASIC_TYPE_DATA1 + BASIC_NUMPY_PANDAS_SCALAR_DATA,
)
def test_insert_scalar(native_df, value):
    if isinstance(value, bytearray):
        # Byte array is treated as array-like it's not a scalar.
        with SqlCounter(query_count=0):
            pass
    else:
        snow_df = pd.DataFrame(native_df)
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: df.insert(0, "col3", value),
                inplace=True,  # insert operation is always inplace
            )


@sql_count_checker(query_count=0)
def test_insert_pandas_types_negative(snow_df):
    value = native_pd.DataFrame({"col1": ["a", "b", "c"]})
    # Verify pandas DataFrame is not allowed.
    msg = (
        f"{type(value)} is not supported as 'value' argument. Please convert this to Snowpark pandas"
        r" objects by calling modin.pandas.Series\(\)/DataFrame\(\)"
    )
    with pytest.raises(TypeError, match=msg):
        snow_df.insert(0, "col3", value)

    # Verify pandas Series is not allowed.
    value = value["col1"]
    msg = (
        f"{type(value)} is not supported as 'value' argument. Please convert this to Snowpark pandas"
        r" objects by calling modin.pandas.Series\(\)/DataFrame\(\)"
    )
    with pytest.raises(TypeError, match=msg):
        snow_df.insert(0, "col3", value)


@sql_count_checker(query_count=1)
def test_insert_dataframe_shape_negative(native_df):
    # DataFrame with more than one column
    snow_df = pd.DataFrame(native_df)
    value = pd.DataFrame({"A": [1, 2, 3], "B": [9, 8, 7]})
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.insert(
            0, "col3", value if isinstance(df, pd.DataFrame) else value.to_pandas()
        ),
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Expected a one-dimensional object, got a DataFrame with 2 columns instead",
    )


@pytest.mark.parametrize(
    "value",
    [
        # NOTE: Accepted numpy array shapes are (N,) or (N, 1) where N = number of rows = 3
        np.ones((3, 2)),
        np.ones((6, 1)),
        np.ones((1, 1)),
        [1, 2],  # len < number of rows
        (6, 7, 8, 9),  # len > number of rows
        {"a", "b", "c"},  # python set
    ],
)
def test_insert_value_negative(native_df, value):
    snow_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=0):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.insert(0, "col3", value),
            expect_exception=True,
        )


@sql_count_checker(query_count=1, join_count=1)
def test_insert_duplicate_label(native_df):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.insert(0, "col1", ["a", "b", "c"], allow_duplicates=True),
        inplace=True,
    )


@sql_count_checker(query_count=0)
def test_insert_duplicate_label_negative(native_df):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.insert(0, "col1", ["a", "b", "c"]),
        expect_exception=True,
    )


@pytest.mark.parametrize("loc", [0, 1, 2])
@sql_count_checker(query_count=1, join_count=1)
def test_insert_loc(native_df, loc):
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.insert(loc, "col3", ["a", "b", "c"]),
        inplace=True,
    )


@pytest.mark.parametrize("loc", [-99, -1, 99, "1"])
def test_insert_loc_negative(native_df, loc):
    with SqlCounter(query_count=0):
        snow_df = pd.DataFrame(native_df)
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.insert(loc, "col3", ["a", "b", "c"]),
            expect_exception=True,
        )


@pytest.mark.parametrize(
    "value, expected_join_count",
    [
        (np.array(["a", "b", "c", "d"]), 1),  # numpy array of shape (N,)
        (np.array([["a"], ["b"], ["c"], ["d"]]), 1),  # numpy array of shape (N, 1)
        (["a", "b", "c", "d"], 1),  # python list
        (("a", "b", "c", "d"), 1),  # python tuple
        ({(3, 1): 1}, 1),  # python dict
        ("abc", 0),  # sting scalar
        (1, 0),  # int scalar
    ],
)
def test_insert_multiindex_array_like_and_scalar(value, expected_join_count):
    arrays = [[3, 4, 5, 6], [1, 2, 1, 2]]
    index = pd.MultiIndex.from_arrays(arrays, names=["first", "second"])
    snow_df = pd.DataFrame({"col1": ["p", "q", "r", "s"]}, index=index)
    native_df = snow_df.to_pandas()
    with SqlCounter(query_count=1, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df.insert(0, "col3", value),
            inplace=True,  # insert operation is always inplace
        )


@pytest.mark.parametrize(
    "value",
    [
        np.array(["a", "b", "c", "d"]),  # numpy array of shape (N,)
        ["a", "b", "c", "d"],  # python list
        ("a", "b", "c", "d"),  # python tuple
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_insert_empty_multiindex_frame(value):
    mi = pd.MultiIndex.from_arrays([np.array([], dtype=int), np.array([], dtype=int)])
    snow_df = pd.DataFrame([], index=mi)
    native_df = native_pd.DataFrame([], index=mi)
    # This behaviour is different from native pandas. Native pandas fails with error
    # ValueError("Buffer dtype mismatch, expected 'Python object' but got 'long'")
    with pytest.raises(ValueError):
        native_df.insert(0, "col3", value)
    snow_df.insert(0, "col3", value)
    expected_df = native_pd.DataFrame(
        value,
        columns=["col3"],
        index=native_pd.Index([(None, None)] * 4),
    )
    assert_snowpark_pandas_equal_to_pandas(snow_df, expected_df)


@sql_count_checker(query_count=0)
def test_insert_multiindex_dict_negative():
    value = {"a": 1, "b": 2, "c": 3}
    arrays = [["apple", "apple", "banana", "banana"], [1, 2, 1, 2]]
    index = pd.MultiIndex.from_arrays(arrays, names=["first", "second"])
    snow_df = pd.DataFrame({"col1": ["p", "q", "r", "s"]}, index=index)
    # This is different behavior from native pandas. In native pandas new column
    # is inserted with null values but in Snowpark pandas we raise error.
    with pytest.raises(
        ValueError,
        match="Number of index levels of inserted column are different from frame index",
    ):
        snow_df.insert(0, "cole3", value)


@pytest.mark.parametrize(
    "df_index, value_index",
    [
        ([3, 0, 4], [1, 2, 3]),
        ([(1, 0), (1, 2), (2, 2)], [(1, 1), (1, 2), (2, 2)]),
        ([1.0, 2.5, 3.0], [1, 2, 3]),  # Long and Double can be joined
    ],
)
@sql_count_checker(query_count=3, join_count=1)
def test_insert_compatible_index(df_index, value_index):
    snow_df = pd.DataFrame({"col1": ["p", "q", "r"]}, index=native_pd.Index(df_index))
    value = pd.DataFrame({"col2": ["x", "y", "z"]}, index=native_pd.Index(value_index))
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.insert(
            0, "col3", value if isinstance(df, pd.DataFrame) else value.to_pandas()
        ),
        inplace=True,  # insert operation is always inplace
    )


@pytest.mark.parametrize(
    "df_index, value_index",
    [
        ([3, 2, 1], [(1, 0, 1), (1, 2, 3), (2, 1, 0)]),  # length mismatch 1 != 3
        (
            [(3, 1), (2, 1), (1, 2)],
            [(1, 0, 1), (1, 2, 3), (2, 1, 0)],
        ),  # length mismatch 2 != 3
        ([1, 2, 3], [(1, 0), (1, 2), (2, 2)]),  # 1 != 2
        ([(1, 0), (1, 2), (2, 2)], [(1, 2, 3), (3, 4, 5), (6, 5, 4)]),  # 2 != 3
        ([(1, 2, 3), (3, 4, 5), (6, 5, 4)], [3, 1, 2]),  # length mismatch 3 != 1
        (
            [(1, 1), (1, 2), (2, 2)],
            ["(1, 0)", "(1, 2)", "(2, 2)"],
        ),  # length and type mismatch
    ],
)
@sql_count_checker(query_count=0)
def test_insert_index_num_levels_mismatch_negative(df_index, value_index):
    snow_df = pd.DataFrame({"col1": ["p", "q", "r"]}, index=native_pd.Index(df_index))
    value = pd.DataFrame({"col2": ["w", "x", "y"]}, index=native_pd.Index(value_index))
    # This is different behavior from native pandas. Native pandas in some cases
    # insert new column with null values but in Snowpark pandas we always raise error.
    with pytest.raises(
        ValueError,
        match="Number of index levels of inserted column are different from frame index",
    ):
        snow_df.insert(0, "col3", value)


@pytest.mark.parametrize(
    "df_index, value_index, expected_index",
    [
        (["1", "2", "3"], [1, 2, 3], [1.0, 2.0, 3.0]),  # type mismatch str != long
        ([1, 2, 3], ["1", "2", "3"], [1.0, 2.0, 3.0]),  # type mismatch long != str
        (
            [True, False, True],
            [1, 0, 3],
            [True, False, True],
        ),  # type mismatch boolean != long
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_insert_index_type_mismatch(df_index, value_index, expected_index):
    # Note: This is different behavior than native pandas. In native pandas when
    # index datatype mismatch new columns in inserted will all NULL values.
    # But in Snowpark pandas we don't perform any client checks on type of index columns
    # and simply call join on them. snowflake backend will cast them whenever possible
    # otherwise, raise a SQLException.
    snow_df = pd.DataFrame({"col1": ["p", "q", "r"]}, index=native_pd.Index(df_index))
    value = pd.DataFrame({"col2": ["x", "y", "z"]}, index=native_pd.Index(value_index))
    expected_df = native_pd.DataFrame(
        {"col1": ["p", "q", "r"], "col2": ["x", "y", "z"]},
        index=native_pd.Index(expected_index),
    )
    snow_df.insert(1, "col2", value)
    assert_snowpark_pandas_equal_to_pandas(snow_df, expected_df)


@sql_count_checker(query_count=3, join_count=1)
def test_insert_with_null_index_values():
    snow_df = pd.DataFrame(
        {"A": ["p", "q", "r", "s"]}, native_pd.Index(["a", None, "b", None])
    )
    value = pd.Series([8, 4], native_pd.Index(["a", None]))
    eval_snowpark_pandas_result(
        snow_df,
        snow_df.to_pandas(),
        lambda df: df.insert(
            0, "col3", value if isinstance(df, pd.DataFrame) else value.to_pandas()
        ),
        inplace=True,  # insert operation is always inplace
    )


@sql_count_checker(query_count=3, join_count=1)
def test_insert_multiple_null():
    snow_df = pd.DataFrame(
        {"A": ["p", "q", "r", "s"]}, native_pd.Index(["a", "b", "c", "d"])
    )
    native_df = snow_df.to_pandas()
    value = pd.Series([8, None, None, 1], native_pd.Index(["a", None, None, "d"]))
    native_value = value.to_pandas()

    # the Snowpark pandas behavior for insert with non-unique index values is different
    # compare with native pandas. Native pandas raise an ValueError,
    # "cannot reindex on an axis with duplicate labels". In Snowpark pandas, we are not
    # able to perform such check and raises an error without trigger an eager evaluation,
    # and it exposes an left join behavior.
    snow_df.insert(1, "col3", value)

    native_value.name = "series"
    expected_res = native_df.join(native_value, rsuffix="_x", how="left", sort=False)
    expected_res.columns = ["A", "col3"]
    assert_frame_equal(snow_df, expected_res)


@pytest.mark.parametrize(
    "index, value",
    [
        ([1, 2], native_pd.Series([1, 2], index=[2, 3])),
        ([1, 2], [3, 4]),
    ],
)
def test_insert_into_empty_dataframe_with_index(index, value):
    with SqlCounter(query_count=1, join_count=1):
        snow_df = pd.DataFrame(index=index)
        native_df = native_pd.DataFrame(index=index)

        def helper(df, value):

            if isinstance(df, pd.DataFrame):
                # convert to snow series if given as native series
                if isinstance(value, native_pd.Series):
                    value = pd.Series(data=value.values, index=value.index)

                df.insert(0, "X", value)

            if isinstance(df, native_pd.DataFrame):
                df.insert(0, "X", value)

        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: helper(df, value),
            inplace=True,
        )


@pytest.mark.parametrize(
    "value",
    [
        10,
        [1, 2, 3],
        np.array([1, 2]),
        native_pd.Series(["abc", 4, 9.0]),
        native_pd.Series([7, 4, 1, 2, 3]).sort_values(),
        native_pd.Series(
            [8, None, None, 1], native_pd.Index([5, 10, 15, 20], dtype=int)
        ),
    ],
)
@pytest.mark.parametrize(
    "data,columns,expected_query_count,expected_join_count",
    [
        # Use [] and None to initialize dataframes with no columns. Test for both cases here
        # because None triggers default behavior (no columns) and [] explicitly defines no columns.
        ([], [], 1, 1),
        ([], None, 1, 1),
        (None, [], 1, 1),
        ([], ["A", "B", "C"], 1, 1),
    ],
)
def test_insert_into_empty_dataframe(
    value, data, columns, expected_query_count, expected_join_count
):
    if isinstance(value, int):
        expected_join_count = 0
    if isinstance(value, list) or isinstance(value, np.ndarray):
        expected_query_count = 1
    snow_df = pd.DataFrame(data=data, columns=columns)
    native_df = native_pd.DataFrame(data=data, columns=columns)

    def helper(df):
        if isinstance(value, native_pd.Series):
            snow_value = pd.Series(value)
            index_mismatch = False
        else:
            snow_value = value
            # Inserting a like-like column into empty dataframe behaves differently in
            # Snowpark pandas. Output frame's index will have all null values. To match
            # with native pandas we call reset_index.
            index_mismatch = True

        if isinstance(df, pd.DataFrame):
            df.insert(0, "X", snow_value)
            if index_mismatch:
                df.reset_index(drop=True, inplace=True)
        if isinstance(df, native_pd.DataFrame):
            df.insert(0, "X", value)

    with SqlCounter(query_count=expected_query_count, join_count=expected_join_count):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            helper,
            inplace=True,
            check_index_type=False,
        )


@sql_count_checker(query_count=0)
def test_insert_into_empty_dataframe_index_dtype_mismatch():
    native_ser = native_pd.Series(
        [8, None, None, 1], native_pd.Index(["a", None, None, "d"])
    )
    snow_ser = pd.Series(native_ser)
    native_df = native_pd.DataFrame()
    snow_df = pd.DataFrame()
    # Snowpark pandas cannot insert value w/ object index into empty frame, which by default has an int index
    native_df.insert(0, "X", native_ser)
    snow_df.insert(0, "X", snow_ser)
    with pytest.raises(SnowparkSQLException):
        snow_df.to_pandas()


@sql_count_checker(query_count=1, join_count=1)
def test_insert_empty_list_into_empty_dataframe():
    snow_df = pd.DataFrame()
    native_df = native_pd.DataFrame()
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.insert(0, "X", []),
        inplace=True,  # insert operation is always inplace
    )


@pytest.mark.parametrize("loc", [-30, -1, 4])
@pytest.mark.parametrize(
    "data,columns",
    [
        ([], []),
        ([], None),
        (None, []),
        ([], ["A", "B", "C"]),
    ],
)
@sql_count_checker(query_count=0)
def test_insert_into_empty_dataframe_negative(loc, data, columns):
    snow_df = pd.DataFrame(data=data, columns=columns)
    native_df = native_pd.DataFrame(data=data, columns=columns)

    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.insert(loc, "X", [1, 2, 3]),
        expect_exception=True,
        assert_exception_equal=True,
        inplace=True,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_insert_into_empty_df_with_single_column():

    series = native_pd.Series([1, 2], index=[3, 2])
    df = native_pd.DataFrame({"col1": []}, index=native_pd.Index([], dtype="int64"))

    def helper(df):
        temp_series = series
        if isinstance(df, pd.DataFrame):
            temp_series = pd.Series(temp_series)

        df.insert(0, "X", temp_series)

    eval_snowpark_pandas_result(pd.DataFrame(df), df, helper, inplace=True)


@pytest.mark.parametrize("insert_label", ["x", ("x",), ("x", "y"), ("x", "y", "z")])
@pytest.mark.parametrize(
    "columns",
    [
        ["a", "b"],  # single index
        [("a",), ("b",)],  # single index of tuples len=1
        [("a", 1), ("b", 2)],  # single index of tuples len=2
        [("a", 1, 1), ("b", 2, 2)],  # single index of tuples len=3
        native_pd.MultiIndex.from_tuples([("a",), ("b",)]),  # multi-index len=1
        native_pd.MultiIndex.from_tuples([("a", 1), ("b", 2)]),  # multi-index len=2
        native_pd.MultiIndex.from_tuples(
            [("a", 1, 1), ("b", 2, 2)]
        ),  # multi-index len=3
    ],
)
def test_insert_multiindex_column(snow_df, columns, insert_label):
    snow_df.columns = columns
    native_df = snow_df.to_pandas()
    if (
        snow_df.columns.nlevels == 1
        or not isinstance(insert_label, tuple)
        or len(insert_label) == snow_df.columns.nlevels
    ):
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: df.insert(0, insert_label, "abc"),
                inplace=True,  # insert operation is always inplace
            )
    else:
        # Raise error when, column index has multiple levels and insert_labels is a tuple
        # of different length.
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: df.insert(0, insert_label, "abc"),
                inplace=True,  # insert operation is always inplace
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match="Item must have length equal to number of levels",
            )


@pytest.mark.parametrize("insert_label", [("x",), ("x", "y"), ("x", "y", "z")])
@pytest.mark.parametrize(
    "columns",
    [
        native_pd.MultiIndex.from_tuples([("a", 1), ("b", 2)]),  # multi-index len=2
        native_pd.MultiIndex.from_tuples(
            [("a", 1, 1), ("b", 2, 2)]
        ),  # multi-index len=3
    ],
)
def test_insert_multiindex_column_negative(snow_df, columns, insert_label):
    # Raise error when, column index has multiple levels and insert_labels is a tuple
    # of different length.
    snow_df.columns = columns
    native_df = snow_df.to_pandas()
    if len(insert_label) != snow_df.columns.nlevels:
        with SqlCounter(query_count=0):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: df.insert(0, insert_label, "abc"),
                inplace=True,  # insert operation is always inplace
                expect_exception=True,
                expect_exception_type=ValueError,
                expect_exception_match="Item must have length equal to number of levels",
            )
    else:
        with SqlCounter(query_count=1):
            eval_snowpark_pandas_result(
                snow_df,
                native_df,
                lambda df: df.insert(0, insert_label, "abc"),
                inplace=True,  # insert operation is always inplace
            )


@pytest.mark.parametrize(
    "index_values, other_index_values, expect_mismatch",
    [
        [["a", "b", "c", "d", "e"], ["a", "b", "c", "d", "e"], False],
        [["a", "b", "c", "d", "e"], ["x", "y", "z", "w", "u"], False],
        [["a", "b", "b", "d", "e"], ["a", "b", "c", "d", "e"], False],
        [["a", "b", "b", "d", "e"], ["a", "b", "b", "d", "e"], False],
        [["a", "b", "b", "d", "e"], ["b", "b", "a", "d", "e"], True],
        [["a", "b", "b", "d", "d"], ["a", "b", "c", "d", "e"], False],
        [["a", "b", "b", "d", "d"], ["a", "b", "c", "d", "e"], False],
        [["a", "b", "b", "d", "d"], ["a", "b", "b", "d", "d"], False],
        [["a", "b", "b", "d", "d"], ["b", "a", "d", "b", "d"], True],
        [["a", "b", "c", "d", "e"], ["a", "b", "b", "d", "e"], True],
        [["a", "b", "b", "d", "d"], ["a", "b", "b", "d", "e"], True],
        [["a", "b", "b", "d", "e"], ["x", "y", "z", "u", "u"], True],
    ],
)
@sql_count_checker(query_count=3, join_count=1)
def test_insert_with_unique_and_duplicate_index_values(
    index_values, other_index_values, expect_mismatch
):
    data = list(range(5))
    data1 = {"foo": data}
    data2 = {"bar": [val * 10 for val in data]}
    native_index = native_pd.Index(index_values, name="INDEX")
    other_native_index = native_pd.Index(other_index_values, name="INDEX")
    index = pd.Index(native_index)
    other_index = pd.Index(other_native_index)

    snow_df1 = pd.DataFrame(data1, index=index)
    snow_df2 = pd.DataFrame(data2, index=other_index)

    native_df1 = native_pd.DataFrame(data1, index=native_index)
    native_df2 = native_pd.DataFrame(data2, index=other_native_index)

    def insert_op(df):
        df.insert(
            0,
            column="bar",
            value=native_df2["bar"]
            if isinstance(df, native_pd.DataFrame)
            else snow_df2["bar"],
        )
        return df

    if not expect_mismatch:
        eval_snowpark_pandas_result(
            snow_df1,
            native_df1,
            lambda df: insert_op(df),
        )
    else:
        # the Snowpark pandas behavior for insert with non-unique index values is different
        # compare with native pandas. Native pandas raise an ValueError,
        # "cannot reindex on an axis with duplicate labels". In Snowpark pandas, we are not
        # able to perform such check and raises an error without trigger an eager evaluation,
        # and it exposes an left join behavior.
        snow_res = insert_op(snow_df1)
        expected_res = native_df1.join(native_df2["bar"], how="left", sort=False)
        expected_res = expected_res[["bar", "foo"]]
        assert_frame_equal(snow_res, expected_res, check_dtype=False)


@sql_count_checker(query_count=3, join_count=6)
def test_insert_timedelta():
    native_df = native_pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    snow_df = pd.DataFrame(native_df)

    def insert(column, vals, df):
        if isinstance(df, pd.DataFrame) and isinstance(vals, native_pd.Series):
            values = pd.Series(vals)
        else:
            values = vals
        df.insert(1, column, values)
        return df

    vals = native_pd.timedelta_range(1, periods=2)
    eval_snowpark_pandas_result(
        snow_df, native_df, functools.partial(insert, "td", vals)
    )

    vals = native_pd.Series(native_pd.timedelta_range(1, periods=2))
    eval_snowpark_pandas_result(
        snow_df, native_df, functools.partial(insert, "td2", vals)
    )

    vals = native_pd.Series(native_pd.timedelta_range(1, periods=2), index=[0, 2])
    eval_snowpark_pandas_result(
        snow_df, native_df, functools.partial(insert, "td3", vals)
    )
